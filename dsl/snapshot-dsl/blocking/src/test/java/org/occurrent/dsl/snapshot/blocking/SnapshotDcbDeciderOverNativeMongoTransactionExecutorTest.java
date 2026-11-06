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
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.mongodb.nativedriver.NativeMongoTransactionExecutor;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.nativedriver.ClientSessionHolder;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
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
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A snapshot decider with a synchronous subscription over {@link NativeMongoTransactionExecutor}. The service reads and
 * decides before the executor only when no {@link ClientSession} is bound on the thread.
 */
@Testcontainers
@DisplayName("SnapshotDcbDeciderApplicationService over NativeMongoTransactionExecutor")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotDcbDeciderOverNativeMongoTransactionExecutorTest {

    private static final String ACCOUNT = "acct";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final List<Integer> executorDepthsAtDecision = new ArrayList<>();
    private MongoClient mongoClient;
    private CloudEventConverter<DomainEvent> converter;
    private MongoEventStore eventStore;
    private SnapshotStore<List<String>> snapshots;
    private DepthRecordingExecutor transactionExecutor;
    private SnapshotDcbDeciderApplicationService<DomainEvent> service;
    private SnapshotDcbDecider<Add, List<String>, DomainEvent> decider;

    @BeforeEach
    void wire_a_snapshot_decider_with_a_synchronous_subscription() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".snapshotdecidernative");
        mongoClient = MongoClients.create(connectionString);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), "events", config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("names", __ -> {
        });
        snapshots = SnapshotStore.inMemory();
        transactionExecutor = new DepthRecordingExecutor(mongoClient);
        service = new SnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(tag()))
                .synchronousSubscriptions(subscriptions)
                .transactionExecutor(transactionExecutor)
                .build());
        decider = SnapshotDcbDecider.from(DcbDecider.from(names(), command -> criteria(), event -> Set.of(tag())), snapshots, SnapshotOptions.of(1, SnapshotPolicy.always()));
    }

    @AfterEach
    void closeClient() {
        ClientSessionHolder.remove();
        mongoClient.close();
    }

    @Test
    void saves_a_snapshot_when_no_session_is_bound_on_the_thread() {
        appendSeed();

        service.execute(new Add("second"), decider);

        assertAll(
                () -> assertThat(executorDepthsAtDecision).as("executor calls the decision ran in").containsExactly(0),
                () -> assertThat(snapshots.findLatest(DcbSnapshotKeys.canonicalKey(criteria()))).as("the snapshot").hasValueSatisfying(snapshot ->
                        assertThat(snapshot.state()).containsExactlyInAnyOrder("seed", "second"))
        );
    }

    @Test
    void reads_and_decides_inside_the_executor_when_called_inside_it() {
        appendSeed();

        transactionExecutor.inTransaction(() -> service.execute(new Add("second"), decider));

        assertAll(
                () -> assertThat(executorDepthsAtDecision).as("executor calls the decision ran in").containsExactly(2),
                () -> assertThat(eventStore.read(criteria()).events()).as("events stored").hasSize(2)
        );
    }

    @Test
    void reads_and_decides_inside_the_executor_in_a_session_the_caller_bound() {
        appendSeed();

        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                service.execute(new Add("second"), decider);
            } finally {
                ClientSessionHolder.remove();
            }
        }

        assertAll(
                () -> assertThat(executorDepthsAtDecision).as("executor calls the decision ran in").containsExactly(1),
                () -> assertThat(eventStore.read(criteria()).events()).as("events stored").hasSize(2)
        );
    }

    private void appendSeed() {
        eventStore.append(new ArrayList<>(converter.toCloudEvents(List.of(nameWasChanged("seed"))).stream().map(ce -> DcbCloudEvents.withTags(ce, List.of(tag()))).toList()));
    }

    private static NameWasChanged nameWasChanged(String name) {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private static DcbCriteria criteria() {
        return DcbCriteria.tags(tag());
    }

    private static Tag tag() {
        return Tag.of("name", ACCOUNT);
    }

    private record Add(String name) {
    }

    private Decider<Add, List<String>, DomainEvent> names() {
        return new Decider<>() {
            @Override
            public List<String> initialState() {
                return List.of();
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Add command, List<String> state) {
                executorDepthsAtDecision.add(transactionExecutor.depth);
                return List.of(nameWasChanged(command.name()));
            }

            @Override
            public List<String> evolve(List<String> state, @NonNull DomainEvent event) {
                List<String> next = new ArrayList<>(state);
                next.add(event.name());
                return next;
            }
        };
    }

    /**
     * Counts how many of its {@code inTransaction} calls the current call runs inside, so the decision can record where it ran.
     */
    private static final class DepthRecordingExecutor extends NativeMongoTransactionExecutor {
        private int depth;

        DepthRecordingExecutor(MongoClient mongoClient) {
            super(mongoClient);
        }

        @Override
        public <T> T inTransaction(Supplier<T> action) {
            depth++;
            try {
                return super.inTransaction(action);
            } finally {
                depth--;
            }
        }
    }
}
