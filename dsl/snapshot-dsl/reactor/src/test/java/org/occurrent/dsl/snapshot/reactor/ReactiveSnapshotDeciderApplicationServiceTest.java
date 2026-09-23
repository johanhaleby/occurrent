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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
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
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayName("ReactiveSnapshotDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactiveSnapshotDeciderApplicationServiceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private ApplicationService<DomainEvent> applicationService;
    private ReactiveSnapshotDeciderApplicationService<DomainEvent> service;
    private ReactorMongoEventStore eventStore;
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
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        service = new ReactiveSnapshotDeciderApplicationService<>(applicationService);
        store = ReactiveSnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        decider = countingDecider(evolveCount, time = LocalDateTime.now());
    }

    @Test
    void first_execute_folds_from_initial_and_saves_a_snapshot_when_the_policy_fires() {
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(service.execute(streamId, new Define("Jane"), ReactiveSnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))))
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
        ReactiveSnapshotStore<String> failingStore = new ThrowingReactiveSnapshotStore<>();

        StepVerifier.create(service.execute(streamId, new Define("Jane"), ReactiveSnapshotDecider.from(decider, failingStore, SnapshotOptions.of(1, SnapshotPolicy.always()))))
                .assertNext(writeResult -> assertThat(writeResult.newStreamVersion()).isEqualTo(1L))
                .verifyComplete();
    }

    @Test
    void a_snapshot_save_that_throws_a_checked_exception_before_returning_a_mono_does_not_error_and_the_write_is_committed() {
        assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(new IOException("snapshot store unreachable (test double)"));
    }

    @Test
    void a_snapshot_save_that_throws_a_StackOverflowError_before_returning_a_mono_does_not_error_and_the_write_is_committed() {
        assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(new StackOverflowError("snapshot store save overflowed (test double)"));
    }

    private void assertSnapshotSaveThrowingBeforeReturningAMonoIsSwallowed(Throwable saveFailure) {
        String streamId = UUID.randomUUID().toString();
        ReactiveSnapshotStore<String> failingStore = new ReactiveSnapshotStore<>() {
            @Override
            public Mono<Snapshot<String>> findLatest(String key) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> save(String key, Snapshot<String> snapshot) {
                throw ReactiveSnapshotDeciderApplicationServiceTest.<RuntimeException>sneakyThrow(saveFailure);
            }
        };
        AtomicReference<WriteResult> result = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> result.set(service.execute(streamId, new Define("Jane"), ReactiveSnapshotDecider.from(decider, failingStore, SnapshotOptions.of(1, SnapshotPolicy.always()))).block(TIMEOUT)));

        assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("the committed write").isEqualTo(1L);
        assertThat(thrown).as("what escaped execute after the write committed").isNull();
        assertThat(result.get().newStreamVersion()).isEqualTo(1L);
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = ReactiveSnapshotDecider.from(decider, store, options);
        service.execute(streamId, new Define("A"), account).block();
        service.execute(streamId, new Change("B"), account).block();

        evolveCount.set(0);
        String state = service.executeAndReturnState(streamId, new Change("C"), account).block();

        assertAll(
                // Only the produced event is folded (1). A full replay of the two history events plus the produced one would be 3.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(state).isEqualTo("C"),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).isEqualTo(3L),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().state()).isEqualTo("C")
        );
    }

    @Test
    void a_reset_stream_with_a_surviving_snapshot_does_not_error_and_folds_from_initial() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = ReactiveSnapshotDecider.from(decider, store, options);
        service.execute(streamId, new Define("A"), account).block();
        service.execute(streamId, new Change("B"), account).block();
        assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).as("snapshot ahead of the reset stream").isEqualTo(2L);

        // Reset the stream below the surviving snapshot without deleting the snapshot: the misuse the head guard covers.
        eventStore.deleteEventStream(streamId).block();

        // The first post-reset command must not error even though the snapshot's version (2) is ahead of the empty stream.
        StepVerifier.create(service.execute(streamId, new Define("C"), account))
                .assertNext(reset -> assertAll(
                        () -> assertThat(reset.oldStreamVersion()).as("wrote against the reset (empty) head").isEqualTo(0L),
                        () -> assertThat(reset.newStreamVersion()).isEqualTo(1L)
                ))
                .verifyComplete();
        // Self-heal dropped the stale snapshot so the next command folds fresh.
        assertThat(store.findLatest(streamId).blockOptional()).as("stale snapshot deleted").isEmpty();

        // The next command reads the reset stream fresh, folds from initial, and stays consistent.
        String state = service.executeAndReturnState(streamId, new Change("D"), account).block();
        assertAll(
                () -> assertThat(state).isEqualTo("D"),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().version()).isEqualTo(2L)
        );
    }

    @Test
    void a_stale_snapshot_delete_that_errors_with_a_checked_exception_does_not_error_and_the_write_is_committed() {
        assertStaleSnapshotDeleteFailureIsSwallowed(() -> Mono.error(new IOException("snapshot store unreachable (test double)")));
    }

    @Test
    void a_stale_snapshot_delete_that_throws_a_runtime_exception_before_returning_a_mono_does_not_error_and_the_write_is_committed() {
        assertStaleSnapshotDeleteFailureIsSwallowed(() -> {
            throw new IllegalStateException("snapshot store delete failed (test double)");
        });
    }

    @Test
    void a_stale_snapshot_delete_that_throws_a_checked_exception_before_returning_a_mono_does_not_error_and_the_write_is_committed() {
        assertStaleSnapshotDeleteFailureIsSwallowed(() -> {
            throw ReactiveSnapshotDeciderApplicationServiceTest.<RuntimeException>sneakyThrow(new IOException("snapshot store unreachable (test double)"));
        });
    }

    @Test
    void a_stale_snapshot_delete_that_throws_a_StackOverflowError_before_returning_a_mono_does_not_error_and_the_write_is_committed() {
        assertStaleSnapshotDeleteFailureIsSwallowed(() -> {
            throw new StackOverflowError("snapshot store delete overflowed (test double)");
        });
    }

    private void assertStaleSnapshotDeleteFailureIsSwallowed(Supplier<Mono<Void>> delete) {
        String streamId = UUID.randomUUID().toString();
        ReactiveSnapshotStore<String> deleteFailingStore = new ReactiveSnapshotStore<>() {
            @Override
            public Mono<Snapshot<String>> findLatest(String key) {
                return store.findLatest(key);
            }

            @Override
            public Mono<Void> save(String key, Snapshot<String> snapshot) {
                return store.save(key, snapshot);
            }

            @Override
            public Mono<Void> delete(String key) {
                return delete.get();
            }
        };
        var account = ReactiveSnapshotDecider.from(decider, deleteFailingStore, SnapshotOptions.of(1, SnapshotPolicy.always()));
        service.execute(streamId, new Define("A"), account).block(TIMEOUT);
        service.execute(streamId, new Change("B"), account).block(TIMEOUT);
        eventStore.deleteEventStream(streamId).block(TIMEOUT);
        AtomicReference<WriteResult> result = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> result.set(service.execute(streamId, new Define("C"), account).block(TIMEOUT)));

        assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("the committed write against the reset stream").isEqualTo(1L);
        assertThat(thrown).as("what escaped execute after the write committed").isNull();
        assertThat(result.get().oldStreamVersion()).isEqualTo(0L);
    }

    @Test
    void everyNEvents_saves_only_when_the_version_delta_crosses_the_threshold() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.everyNEvents(1, 2);
        var account = ReactiveSnapshotDecider.from(decider, store, options);

        service.execute(streamId, new Define("A"), account).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("delta 1 < 2, no snapshot").isEmpty();

        service.execute(streamId, new Change("B"), account).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("delta 2 >= 2, snapshot").hasValueSatisfying(s -> {
            assertThat(s.version()).isEqualTo(2L);
            assertThat(s.state()).isEqualTo("B");
        });
    }

    @Test
    void never_policy_never_saves() {
        String streamId = UUID.randomUUID().toString();
        service.execute(streamId, new Define("A"), ReactiveSnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.never()))).block();
        assertThat(store.findLatest(streamId).blockOptional()).isEmpty();
    }

    @Test
    void when_terminal_saves_at_the_closing_state() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider));
        var account = ReactiveSnapshotDecider.from(decider, store, options);

        service.execute(streamId, new Define("A"), account).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("not terminal, no snapshot").isEmpty();

        service.execute(streamId, new Close(), account).block();
        assertThat(store.findLatest(streamId).blockOptional()).as("terminal, snapshot").hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("CLOSED"));
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot_and_replays_the_whole_stream() {
        String streamId = UUID.randomUUID().toString();
        service.execute(streamId, new Define("A"), ReactiveSnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.always()))).block();

        evolveCount.set(0);
        // Schema 2 does not match the stored schema 1, so the snapshot is ignored and the state is rebuilt from scratch.
        String state = service.executeAndReturnState(streamId, new Change("B"), ReactiveSnapshotDecider.from(decider, store, SnapshotOptions.of(2, SnapshotPolicy.always()))).block();

        assertAll(
                () -> assertThat(state).isEqualTo("B"),
                // Full replay: the history event A (1) plus the produced event B (1) = 2. A resume would have been 1.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(streamId).blockOptional().orElseThrow().schemaVersion()).isEqualTo(2)
        );
    }

    @Test
    void executeAndReturnState_with_a_UUID_stream_id_and_a_command_list_folds_all_commands_and_returns_the_final_state() {
        UUID streamId = UUID.randomUUID();
        var account = ReactiveSnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        String state = service.executeAndReturnState(streamId, List.of(new Define("A"), new Change("B")), account).block(TIMEOUT);

        assertAll(
                () -> assertThat(state).isEqualTo("B"),
                () -> assertThat(requireNonNull(eventStore.read(streamId.toString()).block(TIMEOUT)).version()).isEqualTo(2L)
        );
    }

    @Test
    void from_throws_NullPointerException_when_the_decider_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> ReactiveSnapshotDecider.from(null, store, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("decider");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> ReactiveSnapshotDecider.from(decider, null, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Test
    void from_throws_NullPointerException_when_the_options_are_null() {
        assertThatThrownBy(() -> ReactiveSnapshotDecider.from(decider, store, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("options");
    }

    @Test
    void executeAndReturnState_with_a_single_command_refuses_a_null_state_before_anything_is_written() {
        String streamId = UUID.randomUUID().toString();
        var account = ReactiveSnapshotDecider.from(foldsToNullDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(streamId, new Delete(), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("nothing written for a refused null state").isZero();
    }

    @Test
    void executeAndReturnState_with_a_UUID_stream_id_also_refuses_a_null_state_before_anything_is_written() {
        UUID streamId = UUID.randomUUID();
        var account = ReactiveSnapshotDecider.from(foldsToNullDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(streamId, new Delete(), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertThat(requireNonNull(eventStore.read(streamId.toString()).block(TIMEOUT)).version()).as("nothing written for a refused null state").isZero();
    }

    @Test
    void executeAndReturnState_with_a_command_list_also_refuses_a_null_state_before_anything_is_written() {
        String streamId = UUID.randomUUID().toString();
        var account = ReactiveSnapshotDecider.from(foldsToNullDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(streamId, List.of(new Delete()), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("nothing written for a refused null state").isZero();
    }

    @Test
    void executeAndReturnState_with_several_commands_refuses_a_null_state_even_though_an_earlier_command_in_the_list_folded_non_null() {
        String streamId = UUID.randomUUID().toString();
        var account = ReactiveSnapshotDecider.from(foldsToNullOnTheLastOfSeveralCommandsDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        assertThatThrownBy(() -> service.executeAndReturnState(streamId, List.of(new Keep("A"), new FoldToNull()), account).block(TIMEOUT))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Mono cannot carry null");

        assertAll(
                () -> assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("nothing written, including the events the first command decided").isZero(),
                () -> assertThat(store.findLatest(streamId).blockOptional()).as("no snapshot for a refused write").isEmpty()
        );
    }

    @Test
    void executeAndReturnDecision_still_allows_a_null_state_and_the_write_is_committed() {
        String streamId = UUID.randomUUID().toString();
        var account = ReactiveSnapshotDecider.from(foldsToNullDecider(time), store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        Decider.Decision<@Nullable String, DomainEvent> decision = service.executeAndReturnDecision(streamId, new Delete(), account).block(TIMEOUT);

        assertAll(
                () -> assertThat(decision).isNotNull(),
                () -> assertThat(decision.state()).isNull(),
                () -> assertThat(requireNonNull(eventStore.read(streamId).block(TIMEOUT)).version()).as("the committed write").isEqualTo(1L)
        );
    }

    private static Decider<Delete, @Nullable String, DomainEvent> foldsToNullDecider(LocalDateTime time) {
        return new Decider<>() {
            @Override
            public @Nullable String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull Delete command, @Nullable String state) {
                return List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "DELETED"));
            }

            @Override
            public @Nullable String evolve(@Nullable String state, @NonNull DomainEvent event) {
                return null;
            }
        };
    }

    private record Delete() {
    }

    private static Decider<MultiFoldCmd, @Nullable String, DomainEvent> foldsToNullOnTheLastOfSeveralCommandsDecider(LocalDateTime time) {
        return new Decider<>() {
            @Override
            public @Nullable String initialState() {
                return "";
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull MultiFoldCmd command, @Nullable String state) {
                return switch (command) {
                    case Keep k -> List.of(new NameDefined(UUID.randomUUID().toString(), time, "name", k.name()));
                    case FoldToNull ignored -> List.of(new NameWasChanged(UUID.randomUUID().toString(), time, "name", "DELETED"));
                };
            }

            @Override
            public @Nullable String evolve(@Nullable String state, @NonNull DomainEvent event) {
                return event instanceof NameWasChanged ? null : event.name();
            }
        };
    }

    private sealed interface MultiFoldCmd {
    }

    private record Keep(String name) implements MultiFoldCmd {
    }

    private record FoldToNull() implements MultiFoldCmd {
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

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }
}
