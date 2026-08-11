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
import org.junit.jupiter.api.*
import org.occurrent.cloudevents.EventMetadata
import org.occurrent.cloudevents.OccurrentCloudEventExtension
import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.SagaEffect
import org.occurrent.dsl.saga.SagaInput
import org.occurrent.dsl.saga.TimerName
import java.time.Duration

/**
 * Proves that the flow DSL lowers to the core [Saga] contract correctly: every assertion here goes through
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
            startsOn<GameCreated>()
            correlate<GameCreated> { it.gameId }
            correlate<PlayerJoinedGame> { it.gameId }
            step("awaiting-first-player") {
                on<PlayerJoinedGame>(then = end)
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
                        SagaEffect.startTimeout(stepTimer("awaiting-first-player"), Duration.ofMinutes(10))
                    )
                }
            )
        }

        @Test
        fun `the timeout firing closes the game and completes the saga`() {
            val started = start(saga, GameCreated("g1"))

            val step = saga.step(started.state(), SagaInput.timeout("g1", stepTimer("awaiting-first-player")))

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
                { assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout(stepTimer("awaiting-first-player"))) }
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
            startsOn<LobbyOpened>()
            correlate<LobbyOpened> { it.gameId }
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
                SagaEffect.startTimeout(stepTimer("awaiting-game-start"), Duration.ofMinutes(10))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-game-start"))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-game-start"))
                    )
                }
            )
        }

        @Test
        fun `the timeout firing before the join is fulfilled reminds the players and completes the saga`() {
            val started = start(saga, LobbyOpened("g1"))

            val step = saga.step(started.state(), SagaInput.timeout("g1", stepTimer("awaiting-game-start")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(RemindPlayers("g1"))) }
            )
        }
    }

    // --- Scenario C: order-fulfillment with a retry loop --------------------------------------------------------------

    sealed interface OrderEvent {
        val orderId: String
    }
    data class OrderPlaced(override val orderId: String, val amount: Int) : OrderEvent
    data class PaymentReserved(override val orderId: String) : OrderEvent
    data class PaymentFailed(override val orderId: String, val amount: Int) : OrderEvent

    sealed interface OrderCommand
    data class ReservePayment(val orderId: String, val amount: Int) : OrderCommand
    data class ShipOrder(val orderId: String) : OrderCommand
    data class CancelOrder(val orderId: String) : OrderCommand

    private fun orderFulfillmentSaga(): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
        saga {
            correlateAll { it.orderId }
            startsOn<OrderPlaced> { o -> issue(ReservePayment(o.orderId, o.amount)) }
            step("awaiting-payment") {
                on<PaymentReserved>(then = end) { p -> issue(ShipOrder(p.orderId)) }
                on<PaymentFailed>(
                    then = transitionTo("awaiting-payment"),
                    onlyIf = { _, r -> r.count<PaymentFailed>() < 3 }
                ) { f -> issue(ReservePayment(f.orderId, f.amount)) }
                on<PaymentFailed>(then = end) { f -> issue(CancelOrder(f.orderId)) }
                timeout(after = Duration.ofMinutes(30), then = end) { r ->
                    issue(CancelOrder(r.initiating<OrderPlaced>().orderId))
                }
            }
        }

    @Nested
    inner class CorrelateAll {

        private fun minimalOrderSaga(configure: FlowSagaBuilder<OrderEvent, OrderCommand>.() -> Unit): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
            saga {
                configure()
                startsOn<OrderPlaced>()
                step("awaiting-payment") { on<PaymentReserved>(then = end) }
            }

        @Test
        fun `correlateAll correlates every event type without a per-type correlate`() {
            val saga = minimalOrderSaga { correlateAll { it.orderId } }

            assertAll(
                { assertThat(saga.sagaId(OrderPlaced("o1", 100))).isEqualTo("o1") },
                { assertThat(saga.sagaId(PaymentReserved("o2"))).isEqualTo("o2") },
                { assertThat(saga.sagaId(PaymentFailed("o3", 100))).isEqualTo("o3") }
            )
        }

        @Test
        fun `a per-type correlate overrides the correlateAll fallback for its type`() {
            val saga = minimalOrderSaga {
                correlateAll { it.orderId }
                correlate<PaymentReserved> { "reserved-" + it.orderId }
            }

            assertAll(
                { assertThat(saga.sagaId(PaymentReserved("o1"))).isEqualTo("reserved-o1") },
                { assertThat(saga.sagaId(PaymentFailed("o2", 100))).isEqualTo("o2") },
                { assertThat(saga.sagaId(OrderPlaced("o3", 100))).isEqualTo("o3") }
            )
        }

        @Test
        fun `correlateAll can only be set once`() {
            assertThatThrownBy {
                minimalOrderSaga {
                    correlateAll { it.orderId }
                    correlateAll { it.orderId }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("correlateAll")
        }

        @Test
        fun `the start event is correlated by correlateAll`() {
            val saga = minimalOrderSaga { correlateAll { it.orderId } }

            assertThat(saga.sagaId(OrderPlaced("o1", 100))).isEqualTo("o1")
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
                SagaEffect.startTimeout(stepTimer("awaiting-payment"), Duration.ofMinutes(30))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-payment"))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-payment")),
                        SagaEffect.startTimeout(stepTimer("awaiting-payment"), Duration.ofMinutes(30))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-payment")),
                        SagaEffect.startTimeout(stepTimer("awaiting-payment"), Duration.ofMinutes(30))
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
                        SagaEffect.cancelTimeout(stepTimer("awaiting-payment"))
                    )
                }
            )
        }

        @Test
        fun `the step timeout firing before any resolution cancels the order and completes the saga`() {
            val started = start(saga, OrderPlaced("o1", 100))

            val step = saga.step(started.state(), SagaInput.timeout("o1", stepTimer("awaiting-payment")))

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(CancelOrder("o1"))) }
            )
        }
    }

    // --- Scenario D: a join whose whenFulfilled reads the joined events -----------------------------------------------

    sealed interface ReviewEvent
    data class ReviewRequested(val documentId: String) : ReviewEvent
    data class Approved(val documentId: String, val reviewer: String) : ReviewEvent
    data class BudgetAssigned(val documentId: String, val amount: Int) : ReviewEvent

    sealed interface ReviewCommand
    data class NotifyReviewer(val reviewer: String) : ReviewCommand
    data class Publish(val documentId: String, val amount: Int) : ReviewCommand

    /**
     * The canonical `whenFulfilled` example: a join does not just fire, it hands the block every event it collected while
     * waiting. Here two [Approved] and one [BudgetAssigned] must arrive; once they do, `whenFulfilled` reads each approving
     * reviewer via [ReceivedEvents.all] and the assigned amount via [ReceivedEvents.first] to build commands from the actual
     * joined payloads, not just the initiating event.
     */
    private fun documentReviewSaga(): Saga<ReviewEvent, FlowState<ReviewEvent>, ReviewCommand> =
        saga {
            startsOn<ReviewRequested>()
            correlate<ReviewRequested> { it.documentId }
            correlate<Approved> { it.documentId }
            correlate<BudgetAssigned> { it.documentId }
            step("awaiting-approvals") {
                join(expect<Approved>(2), expect<BudgetAssigned>(), then = end) { received ->
                    received.all<Approved>().forEach { approval -> issue(NotifyReviewer(approval.reviewer)) }
                    val budget = received.first<BudgetAssigned>()
                    issue(Publish(received.initiating<ReviewRequested>().documentId, budget!!.amount))
                }
            }
        }

    @Nested
    inner class DocumentReviewJoin {

        private val saga = documentReviewSaga()

        @Test
        fun `the join stays open until every expected event has arrived`() {
            val started = start(saga, ReviewRequested("d1"))
            val afterFirstApproval = saga.step(started.state(), SagaInput.event(Approved("d1", "alice")))

            val afterBudget = saga.step(afterFirstApproval.state(), SagaInput.event(BudgetAssigned("d1", 500)))

            assertAll(
                { assertThat(saga.isTerminal(afterBudget.state())).isFalse() },
                { assertThat(afterBudget.state().currentStep()).isEqualTo("awaiting-approvals") },
                { assertThat(afterBudget.effects()).isEmpty() }
            )
        }

        @Test
        fun `whenFulfilled reads every joined event to build its commands`() {
            val started = start(saga, ReviewRequested("d1"))
            val afterFirstApproval = saga.step(started.state(), SagaInput.event(Approved("d1", "alice")))
            val afterBudget = saga.step(afterFirstApproval.state(), SagaInput.event(BudgetAssigned("d1", 500)))

            val afterSecondApproval = saga.step(afterBudget.state(), SagaInput.event(Approved("d1", "bob")))

            assertAll(
                { assertThat(saga.isTerminal(afterSecondApproval.state())).isTrue() },
                {
                    assertThat(afterSecondApproval.effects()).containsExactly(
                        SagaEffect.issue(NotifyReviewer("alice")),
                        SagaEffect.issue(NotifyReviewer("bob")),
                        SagaEffect.issue(Publish("d1", 500))
                    )
                }
            )
        }
    }

    // --- Scenario E: historyWindow reaches the Java builder through the Kotlin block ------------------------------------

    sealed interface WinEvent {
        val id: String
    }
    data class Begin(override val id: String) : WinEvent
    data class Tick(override val id: String) : WinEvent

    sealed interface WinCommand
    object Noop : WinCommand

    /** A two-step flow that ping-pongs between "a" and "b" on every Tick, so every event drives a transition. */
    private fun pingPong(historyWindow: Int): Saga<WinEvent, FlowState<WinEvent>, WinCommand> =
        saga {
            historyWindow(historyWindow)
            correlateAll { it.id }
            startsOn<Begin>()
            step("a") { on<Tick>(then = transitionTo("b")) }
            step("b") { on<Tick>(then = transitionTo("a")) }
        }

    private fun runTicks(saga: Saga<WinEvent, FlowState<WinEvent>, WinCommand>, ticks: Int): FlowState<WinEvent> {
        var state = saga.evolve(saga.initialState(), SagaInput.event(Begin("w")))
        repeat(ticks) { state = saga.evolve(state, SagaInput.event(Tick("w"))) }
        return state
    }

    @Nested
    inner class HistoryWindow {

        @Test
        fun `historyWindow set through the Kotlin block bounds the retained event count`() {
            val saga = pingPong(3)

            val atTen = runTicks(saga, 10).received().size
            val atHundred = runTicks(saga, 100).received().size

            assertAll(
                { assertThat(atHundred).describedAs("constant once past the window").isEqualTo(atTen) },
                { assertThat(atTen).describedAs("bounded by the window plus the pinned initiating event").isLessThanOrEqualTo(3 + 2) }
            )
        }
    }

    // --- Scenario F: the metadata-aware `on` overload receives the delivered event's real metadata -------------------

    sealed interface TicketEvent {
        val ticketId: String
    }

    data class TicketOpened(override val ticketId: String) : TicketEvent
    data class TicketEscalated(override val ticketId: String) : TicketEvent

    sealed interface TicketCommand
    data class Page(val ticketId: String, val streamId: String, val streamVersion: Long) : TicketCommand

    private fun metadata(streamId: String, streamVersion: Long) = EventMetadata(
        mapOf(
            OccurrentCloudEventExtension.STREAM_ID to streamId,
            OccurrentCloudEventExtension.STREAM_VERSION to streamVersion
        )
    )

    private fun ticketPagingSaga(): Saga<TicketEvent, FlowState<TicketEvent>, TicketCommand> =
        saga {
            startsOn<TicketOpened>()
            correlateAll { it.ticketId }
            step("awaiting-escalation") {
                on<TicketEscalated>(then = end) { metadata, escalated ->
                    issue(Page(escalated.ticketId, metadata.streamId, metadata.streamVersion))
                }
            }
        }

    @Nested
    inner class MetadataAwareOnOverload {

        private val saga = ticketPagingSaga()

        @Test
        fun `the metadata-aware on overload receives the real stream id and version, not empty metadata`() {
            val started = start(saga, TicketOpened("t1"))

            val step = saga.step(
                started.state(),
                SagaInput.event(TicketEscalated("t1"), metadata("t1", 3L))
            )

            assertAll(
                { assertThat(saga.isTerminal(step.state())).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(Page("t1", "t1", 3L))) }
            )
        }

        @Test
        fun `the one-parameter on overload still binds to the event-only form when metadata is present on the input`() {
            var sawEventOnlyCommand = false
            val eventOnlySaga = saga<TicketEvent, TicketCommand> {
                startsOn<TicketOpened>()
                correlateAll { it.ticketId }
                step("awaiting-escalation") {
                    on<TicketEscalated>(then = end) { escalated ->
                        sawEventOnlyCommand = true
                        issue(Page(escalated.ticketId, "unused", -1L))
                    }
                }
            }

            val started = start(eventOnlySaga, TicketOpened("t2"))
            val step = eventOnlySaga.step(
                started.state(),
                SagaInput.event(TicketEscalated("t2"), metadata("t2", 7L))
            )

            assertAll(
                { assertThat(sawEventOnlyCommand).isTrue() },
                { assertThat(step.effects()).containsExactly(SagaEffect.issue(Page("t2", "unused", -1L))) }
            )
        }
    }

    // --- Scenario G: the Kotlin StepCondition sugar (event, allOf/anyOf, KClass and reified arity, on(condition)) -----

    sealed interface CondEvent {
        val id: String
    }
    data class CondStarted(override val id: String) : CondEvent
    data class CondEventA(override val id: String, val value: Int) : CondEvent
    data class CondEventB(override val id: String) : CondEvent
    data class CondEventC(override val id: String) : CondEvent

    sealed interface CondCommand
    data class CondRecorded(val note: String) : CondCommand

    @Nested
    inner class StepConditionSugar {

        private fun conditionSaga(build: StepScope<CondEvent, CondCommand>.() -> StepCondition<CondEvent>): Saga<CondEvent, FlowState<CondEvent>, CondCommand> =
            saga {
                startsOn<CondStarted>()
                correlateAll { it.id }
                step("wait") {
                    on(build(), then = end)
                }
            }

        @Test
        fun `event with a count and a predicate only counts a matching event`() {
            val saga = conditionSaga { event<CondEventA>(count = 1) { a -> a.value > 10 } }
            val started = start(saga, CondStarted("s1"))

            val afterLow = saga.evolve(started.state, SagaInput.event(CondEventA("s1", 1)))
            val afterHigh = saga.evolve(afterLow, SagaInput.event(CondEventA("s1", 20)))

            assertAll(
                { assertThat(saga.isTerminal(afterLow)).describedAs("value 1 fails the predicate").isFalse() },
                { assertThat(saga.isTerminal(afterHigh)).describedAs("value 20 satisfies it").isTrue() }
            )
        }

        @Test
        fun `allOf and anyOf reified two-type arity sugar`() {
            val allSaga = conditionSaga { allOf<CondEventA, CondEventB>() }
            val anySaga = conditionSaga { anyOf<CondEventA, CondEventB>() }

            val afterAOnly = allSaga.evolve(start(allSaga, CondStarted("s1")).state, SagaInput.event(CondEventA("s1", 1)))
            val afterAThenB = allSaga.evolve(afterAOnly, SagaInput.event(CondEventB("s1")))
            val afterEitherAlone = anySaga.evolve(start(anySaga, CondStarted("s2")).state, SagaInput.event(CondEventA("s2", 1)))

            assertAll(
                { assertThat(allSaga.isTerminal(afterAOnly)).describedAs("allOf needs both").isFalse() },
                { assertThat(allSaga.isTerminal(afterAThenB)).isTrue() },
                { assertThat(anySaga.isTerminal(afterEitherAlone)).describedAs("anyOf needs only one").isTrue() }
            )
        }

        @Test
        fun `allOf and anyOf reified three-type arity sugar`() {
            val allSaga = conditionSaga { allOf<CondEventA, CondEventB, CondEventC>() }
            val anySaga = conditionSaga { anyOf<CondEventA, CondEventB, CondEventC>() }

            var state = start(allSaga, CondStarted("s1")).state
            state = allSaga.evolve(state, SagaInput.event(CondEventA("s1", 1)))
            state = allSaga.evolve(state, SagaInput.event(CondEventB("s1")))
            val beforeThird = state
            state = allSaga.evolve(state, SagaInput.event(CondEventC("s1")))
            val afterEitherAlone = anySaga.evolve(start(anySaga, CondStarted("s2")).state, SagaInput.event(CondEventC("s2")))

            assertAll(
                { assertThat(allSaga.isTerminal(beforeThird)).describedAs("allOf needs all three").isFalse() },
                { assertThat(allSaga.isTerminal(state)).isTrue() },
                { assertThat(anySaga.isTerminal(afterEitherAlone)).describedAs("anyOf needs only one").isTrue() }
            )
        }

        @Test
        fun `allOf and anyOf KClass shortcuts expand to count-one leaves`() {
            val allSaga = conditionSaga { allOf(CondEventA::class, CondEventB::class) }
            val anySaga = conditionSaga { anyOf(CondEventA::class, CondEventB::class) }

            val afterAOnly = allSaga.evolve(start(allSaga, CondStarted("s1")).state, SagaInput.event(CondEventA("s1", 1)))
            val afterAThenB = allSaga.evolve(afterAOnly, SagaInput.event(CondEventB("s1")))
            val afterEitherAlone = anySaga.evolve(start(anySaga, CondStarted("s2")).state, SagaInput.event(CondEventB("s2")))

            assertAll(
                { assertThat(allSaga.isTerminal(afterAOnly)).isFalse() },
                { assertThat(allSaga.isTerminal(afterAThenB)).isTrue() },
                { assertThat(anySaga.isTerminal(afterEitherAlone)).isTrue() }
            )
        }

        @Test
        fun `on with a condition binds its trailing lambda as whenFulfilled`() {
            var recordedNote: String? = null
            val saga = saga<CondEvent, CondCommand> {
                startsOn<CondStarted>()
                correlateAll { it.id }
                step("wait") {
                    on(event<CondEventA>(), then = end) { received ->
                        recordedNote = "count=" + received.count(CondEventA::class.java)
                        issue(CondRecorded("done"))
                    }
                }
            }
            val started = start(saga, CondStarted("s1"))

            val step = saga.step(started.state, SagaInput.event(CondEventA("s1", 1)))

            assertAll(
                { assertThat(step.effects).containsExactly(SagaEffect.issue(CondRecorded("done"))) },
                { assertThat(recordedNote).isEqualTo("count=1") }
            )
        }

        @Test
        fun `on with a condition and no trailing lambda issues nothing, the nothing default`() {
            val saga = conditionSaga { event<CondEventA>() }
            val started = start(saga, CondStarted("s1"))

            val step = saga.step(started.state, SagaInput.event(CondEventA("s1", 1)))

            assertAll(
                { assertThat(saga.isTerminal(step.state)).isTrue() },
                { assertThat(step.effects).isEmpty() }
            )
        }
    }

    // --- Build-time validation ------------------------------------------------------------------------------------

    sealed interface ValidationEvent
    data class Started(val id: String) : ValidationEvent
    data class Foo(val id: String) : ValidationEvent
    data class Bar(val id: String) : ValidationEvent

    sealed interface ValidationCommand
    data class RecordValidation(val id: String) : ValidationCommand

    @Nested
    inner class BuildValidation {

        @Test
        fun `a single-expectation join builds and fulfils on that one event`() {
            val saga = saga<ValidationEvent, ValidationCommand> {
                startsOn<Started>()
                correlate<Started> { it.id }
                correlate<Foo> { it.id }
                step("await-foo") {
                    join(expect<Foo>(), then = end) { r ->
                        issue(RecordValidation(r.initiating<Started>().id))
                    }
                }
            }

            val started = start(saga, Started("v1"))
            val afterFoo = saga.step(started.state(), SagaInput.event(Foo("v1")))

            assertAll(
                { assertThat(saga.isTerminal(afterFoo.state())).isTrue() },
                { assertThat(afterFoo.effects()).containsExactly(SagaEffect.issue(RecordValidation("v1"))) }
            )
        }

        @Test
        fun `a transitionTo target that is not a declared step fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    correlate<Foo> { it.id }
                    step("first") {
                        on<Foo>(then = transitionTo("does-not-exist"))
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("does-not-exist")
        }

        @Test
        fun `an event type used in a step with no correlation fails to build naming the type`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    step("first") {
                        on<Foo>(then = end)
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("Foo")
                .hasMessageContaining("is used by a step")
        }

        @Test
        fun `a duplicate step name fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    correlate<Foo> { it.id }
                    step("first") { on<Foo>(then = end) }
                    step("first") { on<Foo>(then = end) }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("first")
        }

        @Test
        fun `a step mixing a branch and a join fails immediately`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    correlate<Foo> { it.id }
                    correlate<Bar> { it.id }
                    step("first") {
                        on<Foo>(then = end)
                        join(expect<Bar>(), then = end)
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
        }

        @Test
        fun `missing startsOn fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    correlate<Foo> { it.id }
                    step("first") { on<Foo>(then = end) }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("startsOn")
        }

        @Test
        fun `zero steps fails to build`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                }
            }.isInstanceOf(IllegalStateException::class.java)
        }

        @Test
        fun `an Expectation of zero count is rejected`() {
            assertThatThrownBy { Expectation.of(Foo::class.java, 0) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        @Test
        fun `an empty allOf or anyOf collection is rejected`() {
            assertThatThrownBy { StepCondition.allOf(emptyList<StepCondition<ValidationEvent>>()) }
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { StepCondition.anyOf(emptyList<StepCondition<ValidationEvent>>()) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        @Test
        fun `a condition leaf type used in a step with no correlation fails to build naming the type`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    step("first") {
                        on(event<Foo>(), then = end)
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("Foo")
                .hasMessageContaining("is used by a step")
        }

        @Test
        fun `a step mixing a window-condition branch and a join fails immediately, same as mixing a classic branch and a join`() {
            assertThatThrownBy {
                saga<ValidationEvent, ValidationCommand> {
                    startsOn<Started>()
                    correlate<Started> { it.id }
                    correlate<Foo> { it.id }
                    correlate<Bar> { it.id }
                    step("first") {
                        on(event<Foo>(), then = end)
                        join(expect<Bar>(), then = end)
                    }
                }
            }.isInstanceOf(IllegalStateException::class.java)
        }
    }

    @Nested
    inner class StepTimerName {

        @Test
        fun `the top-level stepTimer names a step's timer inside the step namespace`() {
            assertThat(stepTimer("awaiting-payment")).isEqualTo(TimerName.of("step", "awaiting-payment"))
        }
    }
}
