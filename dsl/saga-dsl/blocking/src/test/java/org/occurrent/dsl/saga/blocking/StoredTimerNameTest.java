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

package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.TimerName;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A timer's name became a {@link TimerName} value while the stored form stayed the string it always was, so an instance
 * with a pending timer keeps firing across the upgrade. Each test here seeds a store with a name exactly as it was
 * written before that change and runs the real timer poll over it, so a change to how a stored name is read back or
 * matched against a registration fails here.
 */
@DisplayName("A pending timer stored under its 0.32.0 name")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StoredTimerNameTest {

    // The exact strings a pending timer was stored under before TimerName existed. Written out rather than produced by
    // stepTimer(..) or TimerName.encode(), because a name built by today's code still matches today's code after a
    // format change, and every stored timer would stop firing with these tests still green.
    private static final String STORED_STEP_TIMER = "step:awaiting-players";
    private static final String STORED_TIMER_WITHOUT_A_NAMESPACE = "payment";
    private static final String STORED_TIMER_CONTAINING_A_COLON = "a:b";

    // Epoch millis far enough in the past that the poll always finds the timer due, whatever the wall clock says.
    private static final long LONG_OVERDUE = 1_000;

    private final CloudEventConverter<LobbyEvent> converter =
            new JacksonCloudEventConverter.Builder<LobbyEvent>(new ObjectMapper(), URI.create("urn:occurrent:stored-timer-name-test")).build();

    @Test
    void a_flow_saga_step_timeout_stored_as_step_awaiting_players_still_fires() {
        Saga<LobbyEvent, FlowState<LobbyEvent>, LobbyCommand> saga = lobbySaga();
        SagaStateStore<FlowState<LobbyEvent>> store = SagaStateStore.inMemory();
        FlowState<LobbyEvent> awaitingPlayers = saga.evolve(saga.initialState(), SagaInput.event(new GameCreated("game-1")));
        store.compareAndSave("game-1", storedWith(awaitingPlayers, "game-1", STORED_STEP_TIMER), 0);
        List<LobbyCommand> dispatched = new ArrayList<>();

        execution(saga, store, dispatched::add).pollTimers();

        // Ending the step completes the instance and clears every pending timer, so completion is what says the timeout
        // ran. Consuming a fired timer is checked on the two core DSL timers, which stay active after firing.
        assertAll(
                () -> assertThat(dispatched).as("the stored step timer still reaches the step's timeout reaction").containsExactly(new CancelGame("game-1")),
                () -> assertThat(store.find("game-1")).hasValueSatisfying(e -> assertThat(e.isCompleted()).isTrue())
        );
    }

    @Test
    void a_core_dsl_timer_stored_without_a_namespace_fires_and_is_consumed() {
        Saga<LobbyEvent, String, LobbyCommand> saga = coreSaga(STORED_TIMER_WITHOUT_A_NAMESPACE);
        SagaStateStore<String> store = SagaStateStore.inMemory();
        store.compareAndSave("game-2", storedWith("waiting", "game-2", STORED_TIMER_WITHOUT_A_NAMESPACE), 0);
        List<LobbyCommand> dispatched = new ArrayList<>();

        execution(saga, store, dispatched::add).pollTimers();

        assertAll(
                () -> assertThat(dispatched).containsExactly(new CancelGame("game-2")),
                () -> assertThat(store.find("game-2")).hasValueSatisfying(e -> assertThat(e.timers()).isEmpty())
        );
    }

    @Test
    void a_core_dsl_timer_whose_stored_name_contains_a_colon_fires_and_is_consumed() {
        // The subtle one. "a:b" is registered through the string-taking reactOnTimeout and read back out of the store as
        // a plain string, and the two only meet because both go through TimerName.parse and land on the same value. A
        // change that read one side differently would stop this timer firing with nothing thrown anywhere.
        Saga<LobbyEvent, String, LobbyCommand> saga = coreSaga(STORED_TIMER_CONTAINING_A_COLON);
        SagaStateStore<String> store = SagaStateStore.inMemory();
        store.compareAndSave("game-3", storedWith("waiting", "game-3", STORED_TIMER_CONTAINING_A_COLON), 0);
        List<LobbyCommand> dispatched = new ArrayList<>();

        execution(saga, store, dispatched::add).pollTimers();

        assertAll(
                () -> assertThat(dispatched).containsExactly(new CancelGame("game-3")),
                () -> assertThat(store.find("game-3")).hasValueSatisfying(e -> assertThat(e.timers()).isEmpty())
        );
    }

    private <S> SagaExecution<LobbyEvent, S, LobbyCommand> execution(Saga<LobbyEvent, S, LobbyCommand> saga,
                                                                     SagaStateStore<S> store,
                                                                     CommandDispatcher<LobbyCommand> dispatcher) {
        return new SagaExecution<>("stored-timer-name", saga, store, dispatcher, converter, SagaRunnerConfig.defaults(), event -> true);
    }

    private static <S> SagaEnvelope<S> storedWith(S state, String sagaId, String storedTimerName) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, 1, List.of(new TimerEntry(storedTimerName, LONG_OVERDUE)),
                Map.of(), null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), null, null);
    }

    private static Saga<LobbyEvent, FlowState<LobbyEvent>, LobbyCommand> lobbySaga() {
        return FlowSaga.<LobbyEvent, LobbyCommand>builder()
                .startsOn(GameCreated.class)
                .correlate(GameCreated.class, GameCreated::gameId)
                .correlate(PlayerJoined.class, PlayerJoined::gameId)
                .step("awaiting-players", step -> step
                        .on(PlayerJoined.class, Continuation.end())
                        .timeout(Duration.ofMinutes(30), Continuation.end(),
                                r -> List.of(new CancelGame(r.initiating(GameCreated.class).gameId()))))
                .build();
    }

    private static Saga<LobbyEvent, String, LobbyCommand> coreSaga(String timerName) {
        return Saga.<LobbyEvent, String, LobbyCommand>builder("waiting")
                .correlate(GameCreated.class, GameCreated::gameId)
                .startsOn(GameCreated.class)
                .evolve(GameCreated.class, (state, event) -> "waiting")
                .evolveOnTimeout(timerName, (state, timeout) -> "cancelled")
                .reactOnTimeout(timerName, (state, timeout) -> List.of(SagaEffect.issue(new CancelGame(timeout.sagaId()))))
                .build();
    }

    sealed interface LobbyEvent permits GameCreated, PlayerJoined {
    }

    record GameCreated(String gameId) implements LobbyEvent {
    }

    record PlayerJoined(String gameId) implements LobbyEvent {
    }

    sealed interface LobbyCommand permits CancelGame {
    }

    record CancelGame(String gameId) implements LobbyCommand {
    }
}
