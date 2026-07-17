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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicies;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotDeciderApplicationServiceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdecider"));

    private ApplicationService<DomainEvent> applicationService;
    private ReactiveSnapshotDeciderApplicationService<DomainEvent> snapshotService;
    private ReactiveSnapshotStore<String> store;
    private AtomicInteger evolveCount;
    private Decider<Cmd, String, DomainEvent> decider;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivesnapshotdecider");
        MongoClient mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        ReactorMongoEventStore eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        snapshotService = new ReactiveSnapshotDeciderApplicationService<>(applicationService);
        store = ReactiveSnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        decider = countingDecider(evolveCount, time = LocalDateTime.now());
    }

    @Test
    void first_execute_folds_from_initial_and_saves_a_snapshot_when_the_policy_fires() {
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(snapshotService.execute(streamId, new Define("Jane"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.always())))
                .assertNext(writeResult -> assertThat(writeResult.newStreamVersion()).isEqualTo(1L))
                .verifyComplete();

        Optional<Snapshot<String>> snapshot = store.findLatest(streamId).blockOptional();
        assertAll(
                () -> assertThat(snapshot).isPresent(),
                () -> assertThat(snapshot.orElseThrow().state()).isEqualTo("Jane"),
                () -> assertThat(snapshot.orElseThrow().version()).isEqualTo(1L),
                () -> assertThat(snapshot.orElseThrow().schemaVersion()).isEqualTo(1)
        );
    }

    @Test
    void a_failing_snapshot_save_does_not_error_the_mono_and_the_write_is_committed() {
        String streamId = UUID.randomUUID().toString();
        ReactiveSnapshotStore<String> failingStore = new ReactiveSnapshotStore<>() {
            @Override
            public Mono<Snapshot<String>> findLatest(String key) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> save(String key, Snapshot<String> snapshot) {
                return Mono.error(new RuntimeException("snapshot store is down"));
            }
        };

        StepVerifier.create(snapshotService.execute(streamId, new Define("Jane"), decider, failingStore, SnapshotOptions.of(1, SnapshotPolicy.always())))
                .assertNext(writeResult -> assertThat(writeResult.newStreamVersion()).isEqualTo(1L))
                .verifyComplete();
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        snapshotService.execute(streamId, new Define("A"), decider, store, options).block();
        snapshotService.execute(streamId, new Change("B"), decider, store, options).block();

        evolveCount.set(0);
        String state = snapshotService.executeAndReturnState(streamId, new Change("C"), decider, store, options).block();

        assertAll(
                // Only the produced event is folded (1). A full replay of the two history events plus the produced one would be 3.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(state).isEqualTo("C"),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).isEqualTo(3L),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().state()).isEqualTo("C")
        );
    }

    @Test
    void everyNEvents_saves_only_when_the_version_delta_crosses_the_threshold() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.everyNEvents(1, 2);

        snapshotService.execute(streamId, new Define("A"), decider, store, options).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("delta 1 < 2, no snapshot").isEmpty();

        snapshotService.execute(streamId, new Change("B"), decider, store, options).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("delta 2 >= 2, snapshot").hasValueSatisfying(s -> {
            assertThat(s.version()).isEqualTo(2L);
            assertThat(s.state()).isEqualTo("B");
        });
    }

    @Test
    void never_policy_never_saves() {
        String streamId = UUID.randomUUID().toString();
        snapshotService.execute(streamId, new Define("A"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.never())).block();
        assertThat(store.findLatest(streamId).blockOptional()).isEmpty();
    }

    @Test
    void when_terminal_saves_at_the_closing_state() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider));

        snapshotService.execute(streamId, new Define("A"), decider, store, options).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("not terminal, no snapshot").isEmpty();

        snapshotService.execute(streamId, new Close(), decider, store, options).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("terminal, snapshot").hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("CLOSED"));
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot_and_replays_the_whole_stream() {
        String streamId = UUID.randomUUID().toString();
        snapshotService.execute(streamId, new Define("A"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.always())).block();

        evolveCount.set(0);
        // Schema 2 does not match the stored schema 1, so the snapshot is ignored and the state is rebuilt from scratch.
        String state = snapshotService.executeAndReturnState(streamId, new Change("B"), decider, store, SnapshotOptions.of(2, SnapshotPolicy.always())).block();

        assertAll(
                () -> assertThat(state).isEqualTo("B"),
                // Full replay: the history event A (1) plus the produced event B (1) = 2. A resume would have been 1.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().schemaVersion()).isEqualTo(2)
        );
    }

    private sealed interface Cmd {
    }

    private record Define(String name) implements Cmd {
    }

    private record Change(String name) implements Cmd {
    }

    private record Close() implements Cmd {
    }

    private static Decider<Cmd, String, DomainEvent> countingDecider(AtomicInteger evolveCount, LocalDateTime time) {
        return new Decider<>() {
            @Override
            public String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Cmd command, String state) {
                return switch (command) {
                    case Define d -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", d.name()));
                    case Change c -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", c.name()));
                    case Close ignored -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "CLOSED"));
                };
            }

            @Override
            public String evolve(String state, @NonNull DomainEvent event) {
                evolveCount.incrementAndGet();
                return event.name();
            }

            @Override
            public boolean isTerminal(String state) {
                return "CLOSED".equals(state);
            }
        };
    }
}
