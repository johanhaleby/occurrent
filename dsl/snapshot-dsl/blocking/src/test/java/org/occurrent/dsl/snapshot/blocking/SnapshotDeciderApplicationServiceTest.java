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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
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
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SnapshotDeciderApplicationService")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnapshotDeciderApplicationServiceTest {

    private InMemoryEventStore eventStore;
    private ApplicationService<DomainEvent> applicationService;
    private SnapshotDeciderApplicationService<DomainEvent> snapshotService;
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
        snapshotService = new SnapshotDeciderApplicationService<>(applicationService);
        store = SnapshotStore.inMemory();
        evolveCount = new AtomicInteger();
        decider = countingDecider(evolveCount, time);
    }

    @Test
    void first_execute_folds_from_initial_and_saves_a_snapshot_when_the_policy_fires() {
        String streamId = UUID.randomUUID().toString();

        snapshotService.execute(streamId, new Define("Jane"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        Optional<Snapshot<String>> snapshot = store.findLatest(streamId);
        assertAll(
                () -> assertThat(snapshot).isPresent(),
                () -> assertThat(snapshot.orElseThrow().state()).isEqualTo("Jane"),
                () -> assertThat(snapshot.orElseThrow().version()).isEqualTo(1L),
                () -> assertThat(snapshot.orElseThrow().schemaVersion()).isEqualTo(1)
        );
    }

    @Test
    void second_execute_resumes_from_the_snapshot_and_folds_only_the_tail() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        snapshotService.execute(streamId, new Define("A"), decider, store, options);
        snapshotService.execute(streamId, new Change("B"), decider, store, options);

        evolveCount.set(0);
        String state = snapshotService.executeAndReturnState(streamId, new Change("C"), decider, store, options);

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

        snapshotService.execute(streamId, new Define("A"), decider, store, options);
        assertThat(store.findLatest(streamId)).as("delta 1 < 2, no snapshot").isEmpty();

        snapshotService.execute(streamId, new Change("B"), decider, store, options);
        assertThat(store.findLatest(streamId)).as("delta 2 >= 2, snapshot").hasValueSatisfying(s -> {
            assertThat(s.version()).isEqualTo(2L);
            assertThat(s.state()).isEqualTo("B");
        });
    }

    @Test
    void never_policy_never_saves() {
        String streamId = UUID.randomUUID().toString();
        snapshotService.execute(streamId, new Define("A"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.never()));
        assertThat(store.findLatest(streamId)).isEmpty();
    }

    @Test
    void when_terminal_saves_at_the_closing_state() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider));

        snapshotService.execute(streamId, new Define("A"), decider, store, options);
        assertThat(store.findLatest(streamId)).as("not terminal, no snapshot").isEmpty();

        snapshotService.execute(streamId, new Close(), decider, store, options);
        assertThat(store.findLatest(streamId)).as("terminal, snapshot").hasValueSatisfying(s -> assertThat(s.state()).isEqualTo("CLOSED"));
    }

    @Test
    void a_schema_version_bump_ignores_the_old_snapshot_and_replays_the_whole_stream() {
        String streamId = UUID.randomUUID().toString();
        snapshotService.execute(streamId, new Define("A"), decider, store, SnapshotOptions.of(1, SnapshotPolicy.always()));

        evolveCount.set(0);
        // Schema 2 does not match the stored schema 1, so the snapshot is ignored and the state is rebuilt from scratch.
        String state = snapshotService.executeAndReturnState(streamId, new Change("B"), decider, store, SnapshotOptions.of(2, SnapshotPolicy.always()));

        assertAll(
                () -> assertThat(state).isEqualTo("B"),
                // Full replay: the history event A (1) plus the produced event B (1) = 2. A resume would have been 1.
                () -> assertThat(evolveCount.get()).isEqualTo(2),
                () -> assertThat(store.findLatest(streamId).orElseThrow().schemaVersion()).isEqualTo(2)
        );
    }

    @Test
    void sequential_executes_resuming_from_snapshots_stay_consistent() {
        String streamId = UUID.randomUUID().toString();
        SnapshotOptions<String, DomainEvent> options = SnapshotOptions.of(1, SnapshotPolicy.always());
        snapshotService.execute(streamId, new Define("A"), decider, store, options);
        snapshotService.execute(streamId, new Change("B"), decider, store, options);
        String finalState = snapshotService.executeAndReturnState(streamId, new Change("C"), decider, store, options);
        assertThat(finalState).isEqualTo("C");
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
