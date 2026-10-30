/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.dsl.snapshot.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A snapshot decider over a real MongoDB event store. The stores differ in how an append is held between reserving its
 * position and committing, and in how a caller owns a transaction, which the subclasses supply.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
abstract class MongoBackedSnapshotDcbDeciderTest {

    private static final String ACCOUNT = "acct";
    private static final long AWAIT_SECONDS = 30;

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    MongoClient mongoClient;
    String databaseName;
    private CloudEventConverter<DomainEvent> converter;
    private DcbEventStore eventStore;
    private SnapshotStore<List<String>> snapshots;
    private SnapshotDcbDeciderApplicationService<DomainEvent> service;
    private SnapshotDcbDecider<Add, List<String>, DomainEvent> decider;
    private String key;

    @BeforeEach
    void connect_and_wire_a_snapshot_decider() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".snapshotdecider");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = eventStore();
        snapshots = SnapshotStore.inMemory();
        service = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetryStrategy()));
        decider = SnapshotDcbDecider.from(DcbDecider.from(names(), command -> criteria(), event -> Set.of(tag())), snapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
        key = DcbSnapshotKeys.canonicalKey(criteria());
    }

    @AfterEach
    void closeClient() {
        disconnect();
        mongoClient.close();
    }

    /**
     * The event store the decider runs on.
     */
    abstract DcbEventStore eventStore();

    /**
     * A store over the same collection whose first append waits in {@code hold}, after it reserved its position and before it begins its transaction.
     */
    abstract DcbEventStore holdingEventStore(Runnable hold);

    /**
     * Runs {@code body} in a transaction the caller owns and rolls it back.
     */
    abstract void inCallerTransactionRolledBack(Runnable body);

    void disconnect() {
    }

    @Test
    void a_decision_after_a_snapshot_was_saved_while_another_append_was_held_sees_the_held_event() {
        append(eventStore, "seed");
        CountDownLatch held = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        DcbEventStore holding = holdingEventStore(() -> {
            if (first.compareAndSet(true, false)) {
                held.countDown();
                release.join();
            }
        });
        CompletableFuture<Void> heldAppend = CompletableFuture.runAsync(() -> append(holding, "held"));
        await(held);

        service.execute(new Add("second"), decider);
        Snapshot<List<String>> saved = snapshots.findLatest(key).orElseThrow();
        release.complete(null);
        heldAppend.orTimeout(AWAIT_SECONDS, TimeUnit.SECONDS).join();
        List<String> names = service.executeAndReturnState(new Add("third"), decider);

        assertThat(saved.state()).as("the snapshot saved while the append was held").containsExactlyInAnyOrder("seed", "second");
        assertThat(names).as("what a later decision sees").containsExactlyInAnyOrder("seed", "held", "second", "third");
    }

    @Test
    void a_decision_after_a_snapshot_was_saved_while_another_append_was_held_folds_in_position_order_when_the_fold_keeps_the_last_value() {
        append(eventStore, "seed");
        Decider<Add, String, DomainEvent> lastName = lastName();
        SnapshotStore<String> lastNameSnapshots = SnapshotStore.inMemory();
        SnapshotDcbDecider<Add, String, DomainEvent> snapshotDecider = SnapshotDcbDecider.from(DcbDecider.from(lastName, command -> criteria(), event -> Set.of(tag())), lastNameSnapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
        CountDownLatch held = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        DcbEventStore holding = holdingEventStore(() -> {
            if (first.compareAndSet(true, false)) {
                held.countDown();
                release.join();
            }
        });
        CompletableFuture<Void> heldAppend = CompletableFuture.runAsync(() -> append(holding, "five"));
        await(held);
        DcbAppendResult overtaking = service.execute(new Add("ten"), snapshotDecider).orElseThrow();
        Snapshot<String> saved = lastNameSnapshots.findLatest(key).orElseThrow();
        release.complete(null);
        heldAppend.orTimeout(AWAIT_SECONDS, TimeUnit.SECONDS).join();

        String resumed = service.executeAndReturnState(new Add(""), snapshotDecider);

        String inPositionOrder = lastName.evolve("", eventStore.read(criteria()).events().stream().map(converter::toDomainEvent).toList());
        assertThat(saved.highestFoldedPosition()).as("the highest position folded into the snapshot saved while the append was held").isEqualTo(overtaking.lastSequencePosition());
        assertThat(inPositionOrder).as("the state of a read in position order").isEqualTo("ten");
        assertThat(resumed).as("the state a later decision resumes to").isEqualTo(inPositionOrder);
    }

    @Test
    void saves_a_snapshot_with_the_marker_versions_of_the_boundary_when_the_store_commits_the_append() {
        append(eventStore, "seed");

        service.execute(new Add("second"), decider);

        Snapshot<List<String>> snapshot = snapshots.findLatest(key).orElseThrow();
        assertThat(snapshot.state()).containsExactlyInAnyOrder("seed", "second");
        assertThat(snapshot.markerVersions()).isEqualTo(eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).markerVersions());
        assertThat(snapshot.version()).isZero();
    }

    @Test
    void saves_a_snapshot_with_the_highest_position_the_append_reported() {
        append(eventStore, "seed");

        DcbAppendResult appended = service.execute(new Add("second"), decider).orElseThrow();

        assertThat(snapshots.findLatest(key).orElseThrow().highestFoldedPosition()).isEqualTo(appended.lastSequencePosition());
    }

    @Test
    void saves_no_snapshot_when_the_append_is_made_in_a_transaction_the_caller_rolls_back() {
        append(eventStore, "seed");

        inCallerTransactionRolledBack(() -> service.execute(new Add("second"), decider));

        assertThat(snapshots.findLatest(key)).as("a snapshot at versions of an append that aborted").isEmpty();
        assertThat(eventStore.read(criteria()).events()).as("the append aborted with the transaction").hasSize(1);
    }

    final void append(DcbEventStore store, String name) {
        DomainEvent event = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
        store.append(new ArrayList<>(converter.toCloudEvents(List.of(event)).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList()));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS)).as("the append reached the hold").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private record Add(String name) {
    }

    private static Decider<Add, String, DomainEvent> lastName() {
        return new Decider<>() {
            @Override
            public String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Add command, String state) {
                return command.name().isEmpty() ? List.of() : List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", command.name()));
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                return event.name();
            }
        };
    }

    private static Decider<Add, List<String>, DomainEvent> names() {
        return new Decider<>() {
            @Override
            public List<String> initialState() {
                return List.of();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Add command, List<String> state) {
                return List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", command.name()));
            }

            @Override
            public List<String> evolve(List<String> state, @NonNull DomainEvent event) {
                List<String> next = new ArrayList<>(state);
                next.add(event.name());
                return next;
            }
        };
    }
}
