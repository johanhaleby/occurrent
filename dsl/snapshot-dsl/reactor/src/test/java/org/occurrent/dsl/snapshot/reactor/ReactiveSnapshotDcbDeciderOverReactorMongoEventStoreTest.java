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

package org.occurrent.dsl.snapshot.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
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
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.HoldingTransactionalOperator;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A reactive snapshot decider over a real {@link ReactorMongoEventStore}, with an append held between reserving its
 * position and committing, and with a transaction the caller owns.
 */
@Testcontainers
@DisplayName("ReactiveSnapshotDcbDeciderApplicationService over ReactorMongoEventStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveSnapshotDcbDeciderOverReactorMongoEventStoreTest {

    private static final String ACCOUNT = "acct";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate template;
    private ReactiveMongoTransactionManager transactionManager;
    private CloudEventConverter<DomainEvent> converter;
    private ReactorMongoEventStore eventStore;
    private ReactiveSnapshotStore<List<String>> snapshots;
    private ReactiveSnapshotDcbDeciderApplicationService<DomainEvent> service;
    private ReactiveSnapshotDcbDecider<Add, List<String>, DomainEvent> decider;
    private String key;

    @BeforeEach
    void connect_and_wire_a_snapshot_decider() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdecider");
        String database = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        template = new ReactiveMongoTemplate(mongoClient, database);
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = storeOver(TransactionalOperator.create(transactionManager));
        snapshots = ReactiveSnapshotStore.inMemory();
        service = new ReactiveSnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(eventStore, converter, (DomainEvent event) -> Set.of(tag()), GenericDcbApplicationService.defaultRetry()));
        decider = ReactiveSnapshotDcbDecider.from(DcbDecider.from(names(), command -> criteria(), event -> Set.of(tag())), snapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
        key = DcbSnapshotKeys.canonicalKey(criteria());
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    @Test
    void a_decision_after_a_snapshot_was_saved_while_another_append_was_held_sees_the_held_event() {
        append(eventStore, "seed").block(TIMEOUT);
        CompletableFuture<Void> held = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        ReactorMongoEventStore holding = storeOver(new HoldingTransactionalOperator(TransactionalOperator.create(transactionManager), () -> {
            if (!first.compareAndSet(true, false)) {
                return Mono.empty();
            }
            held.complete(null);
            return Mono.fromFuture(release).timeout(TIMEOUT);
        }));
        CompletableFuture<Void> heldAppend = append(holding, "held").toFuture();
        Mono.fromFuture(held).timeout(TIMEOUT).block();

        service.execute(new Add("second"), decider).block(TIMEOUT);
        Snapshot<List<String>> saved = snapshots.findLatest(key).block(TIMEOUT);
        release.complete(null);
        Mono.fromFuture(heldAppend).block(TIMEOUT);
        List<String> names = service.executeAndReturnState(new Add("third"), decider).block(TIMEOUT);

        assertThat(saved).as("the snapshot saved while the append was held").isNotNull();
        assertThat(saved.state()).containsExactlyInAnyOrder("seed", "second");
        assertThat(names).as("what a later decision sees").containsExactlyInAnyOrder("seed", "held", "second", "third");
    }

    @Test
    void a_decision_after_a_snapshot_was_saved_while_another_append_was_held_folds_in_position_order_when_the_fold_keeps_the_last_value() {
        append(eventStore, "seed").block(TIMEOUT);
        Decider<Add, String, DomainEvent> lastName = lastName();
        ReactiveSnapshotStore<String> lastNameSnapshots = ReactiveSnapshotStore.inMemory();
        ReactiveSnapshotDcbDecider<Add, String, DomainEvent> snapshotDecider = ReactiveSnapshotDcbDecider.from(DcbDecider.from(lastName, command -> criteria(), event -> Set.of(tag())), lastNameSnapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
        CompletableFuture<Void> held = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        ReactorMongoEventStore holding = storeOver(new HoldingTransactionalOperator(TransactionalOperator.create(transactionManager), () -> {
            if (!first.compareAndSet(true, false)) {
                return Mono.empty();
            }
            held.complete(null);
            return Mono.fromFuture(release).timeout(TIMEOUT);
        }));
        CompletableFuture<Void> heldAppend = append(holding, "five").toFuture();
        Mono.fromFuture(held).timeout(TIMEOUT).block();
        DcbAppendResult overtaking = service.execute(new Add("ten"), snapshotDecider).block(TIMEOUT);
        Snapshot<String> saved = lastNameSnapshots.findLatest(key).block(TIMEOUT);
        release.complete(null);
        Mono.fromFuture(heldAppend).block(TIMEOUT);

        String resumed = service.executeAndReturnState(new Add(""), snapshotDecider).block(TIMEOUT);

        String inPositionOrder = lastName.evolve("", eventStore.read(criteria()).block(TIMEOUT).events().stream().map(converter::toDomainEvent).toList());
        assertThat(saved).as("the snapshot saved while the append was held").isNotNull();
        assertThat(saved.highestFoldedPosition()).as("the highest position folded into the snapshot saved while the append was held").isEqualTo(overtaking.lastSequencePosition());
        assertThat(inPositionOrder).as("the state of a read in position order").isEqualTo("ten");
        assertThat(resumed).as("the state a later decision resumes to").isEqualTo(inPositionOrder);
    }

    @Test
    void saves_a_snapshot_with_the_marker_versions_of_the_boundary_when_the_store_commits_the_append() {
        append(eventStore, "seed").block(TIMEOUT);

        service.execute(new Add("second"), decider).block(TIMEOUT);

        Snapshot<List<String>> snapshot = snapshots.findLatest(key).block(TIMEOUT);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.state()).containsExactlyInAnyOrder("seed", "second");
        assertThat(snapshot.markerVersions()).isEqualTo(eventStore.read(criteria(), DcbReadOptions.fromBeginning().withVersions()).block(TIMEOUT).markerVersions());
        assertThat(snapshot.version()).isZero();
    }

    @Test
    void saves_a_snapshot_with_the_highest_position_the_append_reported() {
        append(eventStore, "seed").block(TIMEOUT);

        DcbAppendResult appended = service.execute(new Add("second"), decider).block(TIMEOUT);

        assertThat(snapshots.findLatest(key).block(TIMEOUT).highestFoldedPosition()).isEqualTo(appended.lastSequencePosition());
    }

    @Test
    void saves_no_snapshot_when_the_append_is_made_in_a_transaction_the_caller_rolls_back() {
        append(eventStore, "seed").block(TIMEOUT);

        TransactionalOperator.create(transactionManager).execute(status -> {
            status.setRollbackOnly();
            return service.execute(new Add("second"), decider);
        }).single().block(TIMEOUT);

        assertThat(snapshots.findLatest(key).blockOptional(TIMEOUT)).as("a snapshot at versions of an append that aborted").isEmpty();
        assertThat(eventStore.read(criteria()).block(TIMEOUT).events()).as("the append aborted with the transaction").hasSize(1);
    }

    private ReactorMongoEventStore storeOver(TransactionalOperator transactions) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactions)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new ReactorMongoEventStore(template, config);
    }

    private Mono<Void> append(DcbEventStore store, String name) {
        DomainEvent event = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
        return store.append(new ArrayList<>(converter.toCloudEvents(List.of(event)).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList())).then();
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
