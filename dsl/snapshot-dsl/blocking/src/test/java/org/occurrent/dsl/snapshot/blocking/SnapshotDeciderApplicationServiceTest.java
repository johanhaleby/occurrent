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
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SnapshotDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnapshotDeciderApplicationServiceTest {

    private InMemoryEventStore eventStore;
    private ApplicationService<DomainEvent> applicationService;
    private SnapshotDeciderApplicationService<DomainEvent> service;
    private SnapshotStore<String> store;
    private AtomicInteger evolveCount;
    private Decider<Cmd, String, DomainEvent> decider;
    private LocalDateTime time;

    @BeforeEach
    void setup() {
        time = LocalDateTime.now();
        eventStore = new InMemoryEventStore();
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
        service = new SnapshotDeciderApplicationService<>(applicationService);
        store = SnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        decider = countingDecider(evolveCount, time);
    }

    @Test
    void first_execute_folds_from_initial_and_saves_a_snapshot_when_the_policy_fires() {
        String streamId = UUID.randomUUID().toString();

        service.execute(streamId, new Define("Jane"), SnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        Optional<Snapshot<String>> snapshot = store.findLatest(streamId);
        assertAll(
                () -> assertThat(snapshot).isPresent(),
                () -> assertThat(snapshot.orElseThrow().state()).isEqualTo("Jane"),
                () -> assertThat(snapshot.orElseThrow().version()).isEqualTo(1L),
                () -> assertThat(snapshot.orElseThrow().schemaVersion()).isEqualTo(1)
        );
    }

    @Test
    void a_failing_snapshot_save_does_not_fail_execute_and_the_write_is_committed() {
        String streamId = UUID.randomUUID().toString();
        SnapshotStore<String> failingStore = new ThrowingSnapshotStore<>();

        WriteResult result = service.execute(streamId, new Define("Jane"), SnapshotDecider.from(decider, failingStore, SnapshotOptions.of(1, SnapshotPolicy.always())));

        assertAll(
                () -> assertThat(result).isNotNull(),
                () -> assertThat(result.newStreamVersion()).isEqualTo(1L),
                () -> assertThat(eventStore.read(streamId).version()).isEqualTo(1L)
        );
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDecider.from(decider, store, options);
        service.execute(streamId, new Define("A"), account);
        service.execute(streamId, new Change("B"), account);

        evolveCount.set(0);
        String state = service.executeAndReturnState(streamId, new Change("C"), account);

        assertAll(
                // Only the produced event is folded (1). A full replay of the two history events plus the produced one would be 3.
                () -> assertThat(evolveCount.get()).isEqualTo(1),
                () -> assertThat(state).isEqualTo("C"),
                () -> assertThat(store.findLatest(streamId).orElseThrow().version()).isEqualTo(3L),
                () -> assertThat(store.findLatest(streamId).orElseThrow().state()).isEqualTo("C")
        );
    }

    @Test
    void everyNEvents_saves_only_when_the_version_delta_crosses_the_threshold() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.everyNEvents(1, 2);
        var account = SnapshotDecider.from(decider, store, options);

        service.execute(streamId, new Define("A"), account);
        assertThat(store.findLatest(streamId)).as("delta 1 < 2, no snapshot").isEmpty();

        service.execute(streamId, new Change("B"), account);
        assertThat(store.findLatest(streamId)).as("delta 2 >= 2, snapshot").hasValueSatisfying(s -> {
            assertThat(s.version()).isEqualTo(2L);
            assertThat(s.state()).isEqualTo("B");
        });
    }

    @Test
    void never_policy_never_saves() {
        String streamId = UUID.randomUUID().toString();
        service.execute(streamId, new Define("A"), SnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.never())));
        assertThat(store.findLatest(streamId)).isEmpty();
    }

    @Test
    void when_terminal_saves_at_the_closing_state() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider));
        var account = SnapshotDecider.from(decider, store, options);

        service.execute(streamId, new Define("A"), account);
        assertThat(store.findLatest(streamId)).as("not terminal, no snapshot").isEmpty();

        service.execute(streamId, new Close(), account);
        assertThat(store.findLatest(streamId)).as("terminal, snapshot").hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("CLOSED"));
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot_and_replays_the_whole_stream() {
        String streamId = UUID.randomUUID().toString();
        service.execute(streamId, new Define("A"), SnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicy.always())));

        evolveCount.set(0);
        // Schema 2 does not match the stored schema 1, so the snapshot is ignored and the state is rebuilt from scratch.
        String state = service.executeAndReturnState(streamId, new Change("B"), SnapshotDecider.from(decider, store, SnapshotOptions.of(2, SnapshotPolicy.always())));

        assertAll(
                () -> assertThat(state).isEqualTo("B"),
                // Full replay: the history event A (1) plus the produced event B (1) = 2. A resume would have been 1.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(streamId).orElseThrow().schemaVersion()).isEqualTo(2)
        );
    }

    @Test
    void a_reset_stream_with_a_surviving_snapshot_does_not_throw_and_folds_from_initial() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDecider.from(decider, store, options);
        service.execute(streamId, new Define("A"), account);
        service.execute(streamId, new Change("B"), account);
        assertThat(store.findLatest(streamId).orElseThrow().version()).as("snapshot ahead of the reset stream").isEqualTo(2L);

        // Reset the stream below the surviving snapshot without deleting the snapshot: the misuse the head guard covers.
        eventStore.deleteEventStream(streamId);

        // The first post-reset command must not throw even though the snapshot's version (2) is ahead of the empty stream.
        WriteResult reset = service.execute(streamId, new Define("C"), account);
        assertAll(
                () -> assertThat(reset.oldStreamVersion()).as("wrote against the reset (empty) head").isEqualTo(0L),
                () -> assertThat(reset.newStreamVersion()).isEqualTo(1L),
                // Self-heal dropped the stale snapshot so the next command folds fresh.
                () -> assertThat(store.findLatest(streamId)).as("stale snapshot deleted").isEmpty()
        );

        // The next command reads the reset stream fresh, folds from initial, and stays consistent.
        String state = service.executeAndReturnState(streamId, new Change("D"), account);
        assertAll(
                () -> assertThat(state).isEqualTo("D"),
                () -> assertThat(store.findLatest(streamId).orElseThrow().version()).isEqualTo(2L)
        );
    }

    @Test
    void sequential_executes_resuming_from_snapshots_stay_consistent() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        var account = SnapshotDecider.from(decider, store, options);
        service.execute(streamId, new Define("A"), account);
        service.execute(streamId, new Change("B"), account);
        String finalState = service.executeAndReturnState(streamId, new Change("C"), account);
        assertThat(finalState).isEqualTo("C");
    }

    @Test
    void from_throws_NullPointerException_when_the_decider_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> SnapshotDecider.from(null, store, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("decider");
    }

    @Test
    void from_throws_NullPointerException_when_the_store_is_null() {
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        assertThatThrownBy(() -> SnapshotDecider.from(decider, null, options))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");
    }

    @Test
    void from_throws_NullPointerException_when_the_options_are_null() {
        assertThatThrownBy(() -> SnapshotDecider.from(decider, store, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("options");
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
