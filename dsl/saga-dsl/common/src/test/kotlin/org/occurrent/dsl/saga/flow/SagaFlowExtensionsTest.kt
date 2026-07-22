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

package org.occurrent.dsl.saga.flow

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.SagaEffect
import org.occurrent.dsl.saga.SagaInput
import org.occurrent.dsl.saga.SagaTimeout
import java.time.Duration

/**
 * Proves that the flow DSL lowers to the machine-core [Saga] contract correctly: every assertion here goes through
 * [Saga.step], [Saga.onStart], [Saga.react] and [Saga.isTerminal], never through some parallel notion of "what the flow
 * should do". [Saga.step] folds [Saga.evolve] and [Saga.react] but deliberately excludes [Saga.onStart] (see its
 * kdoc), so the effects of a start event are computed here as `onStart(s1, e) + react(s1, e)`, exactly as an executor
 * would.
 */
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class SagaFlowExtensionsTest {

    companion object {
        /** Applies a start event the way an executor would: evolve, then concatenate onStart's and react's effects. */
        private fun <E : Any, C : Any> start(saga: Saga<E, FlowState<E>, C>, event: E): Saga.Step<FlowState<E>, C> {
            val state = saga.evolve(saga.initialState(), SagaInput.event(event))
            val effects = saga.onStart(state, event) + saga.react(state, SagaInput.event(event))
            return Saga.Step(state, effects)
        }
    }

    // --- Scenario A: close-abandoned-game (issue #124 timeout) -------------------------------------------------------

    sealed interface GameEvent
    data class GameCreated(val gameId: String) : GameEvent
    data class PlayerJoinedGame(val gameId: String) : GameEvent

    sealed interface GameCommand
    data class CloseGame(val gameId: String) : GameCommand

    private fun closeAbandonedGameSaga(): Saga<GameEvent, FlowState<GameEvent>, GameCommand> =
        saga {
            startsOn<GameCreated>({ it.gameId })
            correlate<PlayerJoinedGame> { it.gameId }
            step("awaiting-first-player") {
                on<PlayerJoinedGame>(then = end) {}
                timeout(after = Duration.ofMinutes(10), then = end) { r ->
                    issue(CloseGame(r.initiating<GameCreated>().gameId))
                }
            }
        }

    @Nested
    inner class CloseAbandonedGame {

        private val saga = closeAbandonedGameSaga()

        @Test
        fun `the start event enters the first step and arms its timeout`() {
            val started = start(saga, GameCreated("g1"))

            assertAll(
                { assertThat(started.state().currentStep()).isEqualTo("awaiting-first-player") },
                {
                    assertThat(started.effects()).containsExactly(
                        SagaEffect.startTimeout("step:awaiting-first-player", Duration.ofMinutes(10))
                    )
                }
            )
        }

        @Test
        fun `the timeout firing closes the game and completes the saga`() {
            val started = start(saga, GameCreated("g1"))

            val step = saga.step(started.state(), SagaInput.timeout(SagaTimeout("g1", "step:awaiting-first-player")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(CloseGame("g1"))) }
            )
        }

        @Test
        fun `a player joining before the timeout completes the saga and only cancels the timeout`() {
            val started = start(saga, GameCreated("g1"))

            val step = saga.step(started.state(), SagaInput.event(PlayerJoinedGame("g1")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout("step:awaiting-first-player")) }
            )
        }
    }

    // --- Scenario B: game-start join with an ifNotFulfilled timeout ---------------------------------------------------

    sealed interface LobbyEvent
    data class LobbyOpened(val gameId: String) : LobbyEvent
    data class PlayerJoined(val gameId: String) : LobbyEvent
    data class FirstPlayerMadeMove(val gameId: String) : LobbyEvent

    sealed interface LobbyCommand
    data class SendStartEmail(val gameId: String) : LobbyCommand
    data class RemindPlayers(val gameId: String) : LobbyCommand

    private fun gameStartSaga(): Saga<LobbyEvent, FlowState<LobbyEvent>, LobbyCommand> =
        saga {
            startsOn<LobbyOpened>({ it.gameId })
            correlate<PlayerJoined> { it.gameId }
            correlate<FirstPlayerMadeMove> { it.gameId }
            step("awaiting-game-start") {
                join(expect<PlayerJoined>(2), expect<FirstPlayerMadeMove>(), then = end) { r ->
                    issue(SendStartEmail(r.initiating<LobbyOpened>().gameId))
                }
                timeout(after = Duration.ofMinutes(10), then = end) { r ->
                    issue(RemindPlayers(r.initiating<LobbyOpened>().gameId))
                }
            }
        }

    @Nested
    inner class GameStartJoin {

        private val saga = gameStartSaga()

        @Test
        fun `the start event arms the join step's timeout`() {
            val started = start(saga, LobbyOpened("g1"))

            assertThat(started.effects()).containsExactly(
                SagaEffect.startTimeout("step:awaiting-game-start", Duration.ofMinutes(10))
            )
        }

        @Test
        fun `one PlayerJoined does not fulfil the join`() {
            val started = start(saga, LobbyOpened("g1"))

            val step = saga.step(started.state(), SagaInput.event(PlayerJoined("g1")))

            assertAll(
                { assertThat(step.state().currentStep()).isEqualTo("awaiting-game-start") },
                { assertThat(step.effects()).isEmpty() }
            )
        }

        @Test
        fun `two PlayerJoined without a first move still does not fulfil the join`() {
            val started = start(saga, LobbyOpened("g1"))
            val afterFirstJoin = saga.step(started.state(), SagaInput.event(PlayerJoined("g1")))

            val afterSecondJoin = saga.step(afterFirstJoin.state(), SagaInput.event(PlayerJoined("g1")))

            assertAll(
                { assertThat(afterSecondJoin.state().currentStep()).isEqualTo("awaiting-game-start") },
                { assertThat(afterSecondJoin.effects()).isEmpty() }
            )
        }

        @Test
        fun `the first move fulfils the join, sends the start email and cancels the timeout`() {
            val started = start(saga, LobbyOpened("g1"))
            val afterFirstJoin = saga.step(started.state(), SagaInput.event(PlayerJoined("g1")))
            val afterSecondJoin = saga.step(afterFirstJoin.state(), SagaInput.event(PlayerJoined("g1")))

            val afterMove = saga.step(afterSecondJoin.state(), SagaInput.event(FirstPlayerMadeMove("g1")))

            assertAll(
                { assertThat(saga.isTerminal(afterMove.state())).isTrue() },
                {
                    assertThat(afterMove.effects()).containsExactly(
                        SagaEffect.issue(SendStartEmail("g1")),
                        SagaEffect.cancelTimeout("step:awaiting-game-start")
                    )
                }
            )
        }

        @Test
        fun `a single PlayerJoined plus the first move does not fulfil the join because two joins are expected`() {
            val started = start(saga, LobbyOpened("g1"))
            val afterFirstJoin = saga.step(started.state(), SagaInput.event(PlayerJoined("g1")))

            val afterMove = saga.step(afterFirstJoin.state(), SagaInput.event(FirstPlayerMadeMove("g1")))

            assertAll(
                { assertThat(afterMove.state().currentStep()).isEqualTo("awaiting-game-start") },
                { assertThat(saga.isTerminal(afterMove.state())).isFalse() },
                { assertThat(afterMove.effects()).isEmpty() }
            )
        }

        @Test
        fun `the second PlayerJoined is what tips the join over its expected count, regardless of arrival order`() {
            val started = start(saga, LobbyOpened("g1"))
            val afterFirstJoin = saga.step(started.state(), SagaInput.event(PlayerJoined("g1")))
            val afterMove = saga.step(afterFirstJoin.state(), SagaInput.event(FirstPlayerMadeMove("g1")))

            val afterSecondJoin = saga.step(afterMove.state(), SagaInput.event(PlayerJoined("g1")))

            assertAll(
                { assertThat(saga.isTerminal(afterSecondJoin.state())).isTrue() },
                {
                    assertThat(afterSecondJoin.effects()).containsExactly(
                        SagaEffect.issue(SendStartEmail("g1")),
                        SagaEffect.cancelTimeout("step:awaiting-game-start")
                    )
                }
            )
        }

        @Test
        fun `the timeout firing before the join is fulfilled reminds the players and completes the saga`() {
            val started = start(saga, LobbyOpened("g1"))

            val step = saga.step(started.state(), SagaInput.timeout(SagaTimeout("g1", "step:awaiting-game-start")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(RemindPlayers("g1"))) }
            )
        }
    }

    // --- Scenario C: order-fulfillment with a retry loop --------------------------------------------------------------

    sealed interface OrderEvent
    data class OrderPlaced(val orderId: String, val amount: Int) : OrderEvent
    data class PaymentReserved(val orderId: String) : OrderEvent
    data class PaymentFailed(val orderId: String, val amount: Int) : OrderEvent

    sealed interface OrderCommand
    data class ReservePayment(val orderId: String, val amount: Int) : OrderCommand
    data class ShipOrder(val orderId: String) : OrderCommand
    data class CancelOrder(val orderId: String) : OrderCommand

    private fun orderFulfillmentSaga(): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
        saga {
            startsOn<OrderPlaced>({ it.orderId }) { o -> issue(ReservePayment(o.orderId, o.amount)) }
            correlate<PaymentReserved> { it.orderId }
            correlate<PaymentFailed> { it.orderId }
            step("awaiting-payment") {
                on<PaymentReserved>(then = end) { p -> issue(ShipOrder(p.orderId)) }
                on<PaymentFailed>(
                    then = goTo("awaiting-payment"),
                    onlyIf = { _, r -> r.count<PaymentFailed>() < 3 }
                ) { f -> issue(ReservePayment(f.orderId, f.amount)) }
                on<PaymentFailed>(then = end) { f -> issue(CancelOrder(f.orderId)) }
                timeout(after = Duration.ofMinutes(30), then = end) { r ->
                    issue(CancelOrder(r.initiating<OrderPlaced>().orderId))
                }
            }
        }

    @Nested
    inner class OrderFulfillmentRetryLoop {

        private val saga = orderFulfillmentSaga()

        @Test
        fun `the start event reserves payment and arms the step timeout`() {
            val started = start(saga, OrderPlaced("o1", 100))

            assertThat(started.effects()).containsExactly(
                SagaEffect.issue(ReservePayment("o1", 100)),
                SagaEffect.startTimeout("step:awaiting-payment", Duration.ofMinutes(30))
            )
        }

        @Test
        fun `a reserved payment ships the order, completes the saga and cancels the timeout`() {
            val started = start(saga, OrderPlaced("o1", 100))

            val step = saga.step(started.state(), SagaInput.event(PaymentReserved("o1")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                {
                    assertThat(step.effects()).containsExactly(
                        SagaEffect.issue(ShipOrder("o1")),
                        SagaEffect.cancelTimeout("step:awaiting-payment")
                    )
                }
            )
        }

        @Test
        fun `a payment failure below the retry cap re-arms payment and re-arms the timeout`() {
            val started = start(saga, OrderPlaced("o1", 100))

            val firstFailure = saga.step(started.state(), SagaInput.event(PaymentFailed("o1", 100)))

            assertAll(
                { assertThat(firstFailure.state().currentStep()).isEqualTo("awaiting-payment") },
                { assertThat(saga.isTerminal(firstFailure.state())).isFalse() },
                {
                    assertThat(firstFailure.effects()).containsExactly(
                        SagaEffect.issue(ReservePayment("o1", 100)),
                        SagaEffect.cancelTimeout("step:awaiting-payment"),
                        SagaEffect.startTimeout("step:awaiting-payment", Duration.ofMinutes(30))
                    )
                }
            )
        }

        @Test
        fun `a second payment failure below the retry cap retries again with the same effect shape`() {
            val started = start(saga, OrderPlaced("o1", 100))
            val firstFailure = saga.step(started.state(), SagaInput.event(PaymentFailed("o1", 100)))

            val secondFailure = saga.step(firstFailure.state(), SagaInput.event(PaymentFailed("o1", 100)))

            assertAll(
                { assertThat(secondFailure.state().currentStep()).isEqualTo("awaiting-payment") },
                { assertThat(saga.isTerminal(secondFailure.state())).isFalse() },
                {
                    assertThat(secondFailure.effects()).containsExactly(
                        SagaEffect.issue(ReservePayment("o1", 100)),
                        SagaEffect.cancelTimeout("step:awaiting-payment"),
                        SagaEffect.startTimeout("step:awaiting-payment", Duration.ofMinutes(30))
                    )
                }
            )
        }

        @Test
        fun `a third payment failure exhausts the retry cap, cancels the order and completes the saga`() {
            val started = start(saga, OrderPlaced("o1", 100))
            val firstFailure = saga.step(started.state(), SagaInput.event(PaymentFailed("o1", 100)))
            val secondFailure = saga.step(firstFailure.state(), SagaInput.event(PaymentFailed("o1", 100)))

            val thirdFailure = saga.step(secondFailure.state(), SagaInput.event(PaymentFailed("o1", 100)))

            assertAll(
                { assertThat(saga.isTerminal(thirdFailure.state())).isTrue() },
                {
                    assertThat(thirdFailure.effects()).containsExactly(
                        SagaEffect.issue(CancelOrder("o1")),
                        SagaEffect.cancelTimeout("step:awaiting-payment")
                    )
                }
            )
        }

        @Test
        fun `the step timeout firing before any resolution cancels the order and completes the saga`() {
            val started = start(saga, OrderPlaced("o1", 100))

            val step = saga.step(started.state(), SagaInput.timeout(SagaTimeout("o1", "step:awaiting-payment")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(CancelOrder("o1"))) }
            )
        }
    }

    // --- Build-time validation ------------------------------------------------------------------------------------

    sealed interface ValidationEvent
    data class Started(val id: String) : ValidationEvent
    data class Foo(val id: String) : ValidationEvent
    data class Bar(val id: String) : ValidationEvent

    sealed interface ValidationCommand

    @Nested
    inner class BuildValidation {

        @Test
        fun `a goTo target that is not a declared step fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>({ it.id })
                    correlate<Foo> { it.id }
                    step("first") {
                        on<Foo>(then = goTo("does-not-exist")) {}
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("does-not-exist")
        }

        @Test
        fun `an event type used in a step with no correlation fails to build naming the type`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>({ it.id })
                    step("first") {
                        on<Foo>(then = end) {}
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("Foo")
        }

        @Test
        fun `a duplicate step name fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>({ it.id })
                    correlate<Foo> { it.id }
                    step("first") { on<Foo>(then = end) {} }
                    step("first") { on<Foo>(then = end) {} }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("first")
        }

        @Test
        fun `a step mixing a branch and a join fails immediately`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>({ it.id })
                    correlate<Foo> { it.id }
                    correlate<Bar> { it.id }
                    step("first") {
                        on<Foo>(then = end) {}
                        join(expect<Bar>(), then = end) {}
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
        }

        @Test
        fun `missing startsOn fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    correlate<Foo> { it.id }
                    step("first") { on<Foo>(then = end) {} }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("startsOn")
        }

        @Test
        fun `zero steps fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>({ it.id })
                }
            }.isInstanceOf(IllegalStateException::class.java)
        }

        @Test
        fun `an Expectation of zero count is rejected`() {
            assertThatThrownBy { Expectation.of(Foo::class.java, 0) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
