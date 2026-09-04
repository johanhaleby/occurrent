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

package org.occurrent.dsl.saga

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.occurrent.cloudevents.EventMetadata
import org.occurrent.cloudevents.OccurrentCloudEventExtension
import org.occurrent.filter.Filter
import java.time.Duration
import java.time.Instant

@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class SagaExtensionsTest {

    sealed interface GameEvent {
        val gameId: String
    }

    data class GameStarted(override val gameId: String) : GameEvent
    data class MoveMade(override val gameId: String, val player: String) : GameEvent
    data class GameWon(override val gameId: String, val winner: String) : GameEvent

    /** Not sealed, so its concrete types cannot be enumerated and a derived filter would miss them. */
    interface OpenGameEvent {
        val gameId: String
    }

    sealed interface GameCommand
    data class NotifyPlayer(val gameId: String, val message: String) : GameCommand
    data class ArchiveGame(val gameId: String) : GameCommand

    sealed interface GameState
    data class InProgress(val gameId: String, val moves: Int = 0) : GameState
    data class Finished(val gameId: String, val winner: String) : GameState

    companion object {
        private const val TURN_TIMER = "turn"

        private fun gameSaga(): Saga<GameEvent, GameState?, GameCommand> =
            saga(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                evolve<GameStarted> { _, e -> InProgress(e.gameId) }
                react<GameStarted> { _, e ->
                    issue(NotifyPlayer(e.gameId, "started"))
                    startTimeout(TURN_TIMER, Duration.ofMinutes(5))
                }
                evolve<MoveMade> { state, _ -> (state as InProgress).copy(moves = state.moves + 1) }
                evolve<GameWon> { state, e -> Finished((state as InProgress).gameId, e.winner) }
                react<GameWon> { _, e ->
                    issue(NotifyPlayer(e.gameId, "won by ${e.winner}"))
                    cancelTimeout(TURN_TIMER)
                }
                reactOnTimeout(TURN_TIMER) { _, t -> issue(ArchiveGame(t.sagaId)) }
                onStart { _, e -> issue(NotifyPlayer(e.gameId, "welcome")) }
                isTerminal { it is Finished }
            }
    }

    @Nested
    inner class Dispatch {

        @Test
        fun `evolve folds a start event into the initial state`() {
            val saga = gameSaga()

            val step = saga.step(null, SagaInput.event(GameStarted("game-1")))

            assertThat(step.state()).isEqualTo(InProgress("game-1"))
        }

        @Test
        fun `evolve dispatches reified per-event-type handlers by concrete type`() {
            val saga = gameSaga()

            val state = saga.evolve(InProgress("game-1", moves = 2), SagaInput.event(MoveMade("game-1", "alice")))

            assertThat(state).isEqualTo(InProgress("game-1", moves = 3))
        }

        @Test
        fun `react sees the post-evolve state`() {
            val saga = gameSaga()

            val step = saga.step(InProgress("game-1", moves = 4), SagaInput.event(GameWon("game-1", "alice")))

            assertAll(
                { assertThat(step.state()).isEqualTo(Finished("game-1", "alice")) },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(NotifyPlayer("game-1", "won by alice")), SagaEffect.cancelTimeout(TURN_TIMER)) }
            )
        }
    }

    @Nested
    inner class SagaEffectsCollector {

        @Test
        fun `preserves the call order of issue then startTimeout`() {
            val saga = gameSaga()

            val effects = saga.react(InProgress("game-1"), SagaInput.event(GameStarted("game-1")))

            assertThat(effects).containsExactly(
                SagaEffect.issue(NotifyPlayer("game-1", "started")),
                SagaEffect.startTimeout(TURN_TIMER, Duration.ofMinutes(5))
            )
        }

        @Test
        fun `chaining off the receiver returned by issue records both effects`() {
            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                evolve<GameStarted> { _, e -> InProgress(e.gameId) }
                react<GameStarted> { _, e -> issue(NotifyPlayer(e.gameId, "started")).cancelTimeout(TURN_TIMER) }
            }

            val effects = saga.react(InProgress("game-1"), SagaInput.event(GameStarted("game-1")))

            assertThat(effects).containsExactly(
                SagaEffect.issue(NotifyPlayer("game-1", "started")),
                SagaEffect.cancelTimeout(TURN_TIMER)
            )
        }

        @Test
        fun `a reaction that ends on a conditional compiles when terminated with nothing`() {
            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                evolve<GameStarted> { _, e -> InProgress(e.gameId) }
                react<GameStarted> { _, e ->
                    if (e.gameId == "nonexistent") {
                        issue(NotifyPlayer(e.gameId, "started"))
                    }
                    nothing
                }
            }

            val effects = saga.react(InProgress("game-1"), SagaInput.event(GameStarted("game-1")))

            assertThat(effects).isEmpty()
        }
    }

    @Nested
    inner class ReactOnTimeout {

        @Test
        fun `dispatches by the registered timer name`() {
            val saga = gameSaga()

            val step = saga.step(InProgress("game-1"), SagaInput.timeout("game-1", TimerName.parse(TURN_TIMER)))

            assertThat(step.effects()).containsExactly(SagaEffect.issue(ArchiveGame("game-1")))
        }

        @Test
        fun `an unregistered timer name produces no effects`() {
            val saga = gameSaga()

            val step = saga.step(InProgress("game-1"), SagaInput.timeout("game-1", TimerName.parse("unknown")))

            assertAll(
                { assertThat(step.state()).isEqualTo(InProgress("game-1")) },
                { assertThat(step.effects()).isEmpty() }
            )
        }
    }

    @Nested
    inner class TimerNameOverloads {

        private val stepTurn = TimerName.of("step", "turn")

        private fun namespacedSaga(): Saga<GameEvent, GameState?, GameCommand> =
            saga(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                evolve<GameStarted> { _, e -> InProgress(e.gameId) }
                react<GameStarted> { _, _ -> startTimeout(stepTurn, Duration.ofMinutes(5)) }
                evolve<GameWon> { state, e -> Finished((state as InProgress).gameId, e.winner) }
                react<GameWon> { _, _ -> cancelTimeout(stepTurn) }
                evolveOnTimeout(stepTurn) { _, t -> Finished(t.sagaId, "nobody") }
                reactOnTimeout(stepTurn) { _, t -> issue(ArchiveGame(t.sagaId)) }
            }

        @Test
        fun `startTimeout takes a TimerName`() {
            val effects = namespacedSaga().react(InProgress("game-1"), SagaInput.event(GameStarted("game-1")))

            assertThat(effects).containsExactly(SagaEffect.startTimeout<GameCommand>(stepTurn, Duration.ofMinutes(5)))
        }

        @Test
        fun `cancelTimeout takes a TimerName`() {
            val effects = namespacedSaga().react(Finished("game-1", "alice"), SagaInput.event(GameWon("game-1", "alice")))

            assertThat(effects).containsExactly(SagaEffect.cancelTimeout<GameCommand>(stepTurn))
        }

        @Test
        fun `startTimeoutAt takes a TimerName`() {
            val at = Instant.parse("2026-01-01T00:00:00Z")
            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                react<GameStarted> { _, _ -> startTimeoutAt(stepTurn, at) }
            }

            val effects = saga.react(null, SagaInput.event(GameStarted("game-1")))

            assertThat(effects).containsExactly(SagaEffect.startTimeoutAt<GameCommand>(stepTurn, at))
        }

        @Test
        fun `a timer registered with a TimerName fires for the same name read out of its stored string`() {
            val step = namespacedSaga().step(InProgress("game-1"), SagaInput.timeout("game-1", TimerName.parse("step:turn")))

            assertAll(
                { assertThat(step.state()).isEqualTo(Finished("game-1", "nobody")) },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(ArchiveGame("game-1"))) }
            )
        }
    }

    @Nested
    inner class OnStart {

        @Test
        fun `runs the registered handler once the instance is created`() {
            val saga = gameSaga()

            val effects = saga.onStart(InProgress("game-1"), GameStarted("game-1"))

            assertThat(effects).containsExactly(SagaEffect.issue(NotifyPlayer("game-1", "welcome")))
        }
    }

    @Nested
    inner class IsTerminal {

        @Test
        fun `reflects the registered predicate`() {
            val saga = gameSaga()

            assertAll(
                { assertThat(saga.isTerminal(Finished("game-1", "alice"))).isTrue() },
                { assertThat(saga.isTerminal(InProgress("game-1"))).isFalse() }
            )
        }
    }

    @Nested
    inner class MetadataOverloads {

        private fun metadata(streamId: String, streamVersion: Long, position: Long) = EventMetadata(
            mapOf(
                OccurrentCloudEventExtension.STREAM_ID to streamId,
                OccurrentCloudEventExtension.STREAM_VERSION to streamVersion,
                OccurrentCloudEventExtension.POSITION to position
            )
        )

        @Test
        fun `the three-argument evolve and react overloads receive the delivered event's metadata`() {
            var seenByEvolve: EventMetadata? = null
            var seenByReact: EventMetadata? = null
            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                evolve<GameStarted> { _, metadata, e ->
                    seenByEvolve = metadata
                    InProgress(e.gameId)
                }
                react<GameStarted> { _, metadata, e ->
                    seenByReact = metadata
                    issue(NotifyPlayer(e.gameId, "started"))
                }
            }

            val step = saga.step(null, SagaInput.event(GameStarted("game-1"), metadata("game-1", 2L, 9L)))

            assertAll(
                { assertThat(step.state()).isEqualTo(InProgress("game-1")) },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(NotifyPlayer("game-1", "started"))) },
                { assertThat(seenByEvolve?.streamId).isEqualTo("game-1") },
                { assertThat(seenByEvolve?.streamVersion).isEqualTo(2L) },
                { assertThat(seenByEvolve?.position).isEqualTo(9L) },
                { assertThat(seenByReact?.streamId).isEqualTo("game-1") },
                { assertThat(seenByReact?.position).isEqualTo(9L) }
            )
        }

        @Test
        fun `the two-argument overloads still work when the input carries metadata`() {
            val saga = gameSaga()

            val step = saga.step(null, SagaInput.event(GameStarted("game-1"), metadata("game-1", 1L, 1L)))

            assertThat(step.state()).isEqualTo(InProgress("game-1"))
        }
    }

    @Nested
    inner class StartsOnAndCorrelation {

        @Test
        fun `startsOn registers the type that creates an instance`() {
            val saga = gameSaga()

            assertThat(saga.startEventTypes()).containsExactly(GameStarted::class.java)
        }

        @Test
        fun `correlateAll derives the correlation id for every registered type`() {
            val saga = gameSaga()

            assertAll(
                { assertThat(saga.sagaId(GameStarted("game-1"))).isEqualTo("game-1") },
                { assertThat(saga.sagaId(MoveMade("game-1", "alice"))).isEqualTo("game-1") }
            )
        }
    }

    @Nested
    inner class ExplicitFilter {

        @Test
        fun `filter sets the selector the saga subscribes on`() {
            val subject = Filter.subject("game-1")

            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                replacementFilter(subject)
            }

            assertThat(saga.replacementFilter()).isSameAs(subject)
        }

        @Test
        fun `a saga without one reports no filter`() {
            assertThat(gameSaga().replacementFilter()).isNull()
        }

        @Test
        fun `narrowingFilter sets the extra condition the saga also requires`() {
            val subject = Filter.subject("game-1")

            val saga = saga<GameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
                narrowingFilter(subject)
            }

            assertThat(saga.narrowingFilter()).isSameAs(subject)
            assertThat(saga.replacementFilter()).isNull()
        }

        @Test
        fun `a saga without one reports no narrowing filter`() {
            assertThat(gameSaga().narrowingFilter()).isNull()
        }

        @Test
        fun `filter builds a saga on an open supertype that is otherwise refused`() {
            val saga = saga<OpenGameEvent, GameState?, GameCommand>(initialState = null) {
                correlateAll { it.gameId }
                startsOn<OpenGameEvent>()
                evolve<OpenGameEvent> { state, _ -> state }
                replacementFilter(Filter.type("game-event"))
            }

            assertThat(saga.eventTypes()).containsExactly(OpenGameEvent::class.java)
        }
    }

    @Nested
    inner class NoArgDsl {

        @Test
        fun `saga with no argument starts from null like initialState null`() {
            val saga = saga<GameEvent, GameState?, GameCommand> {
                correlateAll { it.gameId }
                startsOn<GameStarted>()
            }

            assertThat(saga.initialState()).isNull()
        }
    }
}
