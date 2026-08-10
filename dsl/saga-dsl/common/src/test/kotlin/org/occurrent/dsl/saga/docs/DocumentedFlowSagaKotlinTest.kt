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

package org.occurrent.dsl.saga.docs

// The reified initiating<T>() is a top-level extension in org.occurrent.dsl.saga.flow, so a file in another package
// imports it by name like any extension. ReceivedEvents also has a no-arg initiating() member, and a member wins over
// an extension, so the type argument has to be explicit: without it the member is chosen instead. That member is also
// why omitting the import reports "No type arguments expected" rather than an unresolved reference.
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.SagaEffect
import org.occurrent.dsl.saga.SagaInput
import org.occurrent.dsl.saga.SagaTimeout
import org.occurrent.dsl.saga.flow.FlowState
import org.occurrent.dsl.saga.flow.initiating
import org.occurrent.dsl.saga.flow.saga
import java.time.Duration
import java.time.Instant

/**
 * The flow sagas the documentation's Testing chapter shows, kept compiling and passing here so a published snippet
 * cannot drift from the API. Every assertion goes through [Saga.step], which folds evolve and react without any clock,
 * store or subscription, so a timeout is fired by naming its timer rather than by letting time pass.
 */
@DisplayName("DocumentedFlowSaga (Kotlin)")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DocumentedFlowSagaKotlinTest {

    @Nested
    @DisplayName("when a lobby waits for a player to join")
    inner class WhenALobbyWaitsForAPlayerToJoin {

        @Test
        fun `a player joining advances to the next step and cancels the step's timeout`() {
            // Given
            val started = start(lobby(), GameCreated(GAME_ID))

            // When
            val step = lobby().step(started.state, SagaInput.event(PlayerJoined(GAME_ID)))

            // Then
            assertAll(
                { assertThat(step.state.currentStep()).isEqualTo("waiting-for-both-players") },
                { assertThat(step.effects).containsExactly(SagaEffect.cancelTimeout("step:awaiting-players")) }
            )
        }

        @Test
        fun `a player joining issues no command even though it produces an effect`() {
            // Given
            val started = start(lobby(), GameCreated(GAME_ID))

            // When
            val step = lobby().step(started.state, SagaInput.event(PlayerJoined(GAME_ID)))

            // Then
            assertThat(step.issuedCommands()).isEmpty()
        }

        @Test
        fun `the timeout firing closes the game and completes the saga`() {
            // Given
            val started = start(lobby(), GameCreated(GAME_ID))

            // When
            val step = lobby().step(started.state, SagaInput.timeout(SagaTimeout(GAME_ID, "step:awaiting-players")))

            // Then
            assertAll(
                { assertThat(step.effects).containsExactly(SagaEffect.issue(CloseGame(GAME_ID))) },
                { assertThat(step.state.completed()).isTrue() }
            )
        }

        @Test
        fun `a timer name the saga does not know leaves the state and the effects untouched`() {
            // Given
            val started = start(lobby(), GameCreated(GAME_ID))

            // When
            val step = lobby().step(started.state, SagaInput.timeout(SagaTimeout(GAME_ID, "step:no-such-step")))

            // Then
            assertAll(
                { assertThat(step.state.currentStep()).isEqualTo("awaiting-players") },
                { assertThat(step.effects).isEmpty() }
            )
        }
    }

    @Nested
    @DisplayName("when a step joins on two players readying up")
    inner class WhenAStepJoinsOnTwoPlayersReadyingUp {

        @Test
        fun `one player readying up does not leave the join step`() {
            // Given
            val joining = joinStepEntered()

            // When
            val step = lobby().step(joining, SagaInput.event(PlayerReady(GAME_ID)))

            // Then
            assertAll(
                { assertThat(step.state.currentStep()).isEqualTo("waiting-for-both-players") },
                { assertThat(step.state.completed()).isFalse() }
            )
        }

        @Test
        fun `the second player readying up fulfils the join and completes the saga`() {
            // Given
            val afterFirst = lobby().step(joinStepEntered(), SagaInput.event(PlayerReady(GAME_ID)))

            // When
            val afterSecond = lobby().step(afterFirst.state, SagaInput.event(PlayerReady(GAME_ID)))

            // Then
            assertThat(afterSecond.state.completed()).isTrue()
        }

        private fun joinStepEntered(): FlowState<GameEvent> {
            val started = start(lobby(), GameCreated(GAME_ID))
            return lobby().step(started.state, SagaInput.event(PlayerJoined(GAME_ID))).state
        }
    }

    @Nested
    @DisplayName("when an auction re-enters its bidding step on every bid")
    inner class WhenAnAuctionReEntersItsBiddingStepOnEveryBid {

        @Test
        fun `a bid keeps the saga in the bidding step`() {
            // Given
            val started = start(auction(), AuctionStarted(AUCTION_ID, ENDS_AT))

            // When
            val step = auction().step(started.state, SagaInput.event(BidPlaced(AUCTION_ID, 100)))

            // Then
            assertAll(
                { assertThat(step.state.currentStep()).isEqualTo("bidding") },
                { assertThat(step.state.completed()).isFalse() }
            )
        }

        @Test
        fun `a second bid still keeps the saga in the bidding step`() {
            // Given
            val afterFirstBid = auction().step(
                start(auction(), AuctionStarted(AUCTION_ID, ENDS_AT)).state,
                SagaInput.event(BidPlaced(AUCTION_ID, 100))
            )

            // When
            val afterSecondBid = auction().step(afterFirstBid.state, SagaInput.event(BidPlaced(AUCTION_ID, 150)))

            // Then
            assertThat(afterSecondBid.state.currentStep()).isEqualTo("bidding")
        }

        @Test
        fun `the deadline firing closes the auction and completes the saga`() {
            // Given
            val started = start(auction(), AuctionStarted(AUCTION_ID, ENDS_AT))

            // When
            val step = auction().step(started.state, SagaInput.timeout(SagaTimeout(AUCTION_ID, "step:bidding")))

            // Then
            assertAll(
                { assertThat(step.effects).containsExactly(SagaEffect.issue(CloseAuction(AUCTION_ID))) },
                { assertThat(step.state.completed()).isTrue() }
            )
        }

        @Test
        fun `the deadline still closes the auction after bids have looped the step`() {
            // Given
            val afterBid = auction().step(
                start(auction(), AuctionStarted(AUCTION_ID, ENDS_AT)).state,
                SagaInput.event(BidPlaced(AUCTION_ID, 100))
            )

            // When
            val step = auction().step(afterBid.state, SagaInput.timeout(SagaTimeout(AUCTION_ID, "step:bidding")))

            // Then
            assertThat(step.effects).containsExactly(SagaEffect.issue(CloseAuction(AUCTION_ID)))
        }
    }

    @Nested
    @DisplayName("when a step waits for either two approvals or a single rejection")
    inner class WhenAStepWaitsForEitherTwoApprovalsOrASingleRejection {

        @Test
        fun `one approval does not fulfil the condition`() {
            // Given
            val started = start(review(), ReviewStarted(REVIEW_ID))

            // When
            val step = review().step(started.state, SagaInput.event(Approved(REVIEW_ID)))

            // Then
            assertThat(step.state.completed()).isFalse()
        }

        @Test
        fun `two approvals publish and complete the saga`() {
            // Given
            val started = start(review(), ReviewStarted(REVIEW_ID))
            val afterFirst = review().step(started.state, SagaInput.event(Approved(REVIEW_ID)))

            // When
            val afterSecond = review().step(afterFirst.state, SagaInput.event(Approved(REVIEW_ID)))

            // Then
            assertAll(
                { assertThat(afterSecond.state.completed()).isTrue() },
                { assertThat(afterSecond.effects).containsExactly(SagaEffect.issue(Publish(REVIEW_ID))) }
            )
        }

        @Test
        fun `a single rejection discards and completes the saga immediately`() {
            // Given
            val started = start(review(), ReviewStarted(REVIEW_ID))

            // When
            val step = review().step(started.state, SagaInput.event(Rejected(REVIEW_ID)))

            // Then
            assertAll(
                { assertThat(step.state.completed()).isTrue() },
                { assertThat(step.effects).containsExactly(SagaEffect.issue(Discard(REVIEW_ID))) }
            )
        }
    }

    @Nested
    @DisplayName("when a step needs two packed items plus either a courier or a pickup slot")
    inner class WhenAStepNeedsTwoPackedItemsPlusEitherACourierOrAPickupSlot {

        @Test
        fun `two packed items alone do not fulfil the condition`() {
            // Given
            val started = start(shipment(), ShipmentStarted(SHIPMENT_ID))
            val afterFirstItem = shipment().step(started.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-1")))

            // When
            val afterSecondItem = shipment().step(afterFirstItem.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-2")))

            // Then
            assertThat(afterSecondItem.state.completed()).isFalse()
        }

        @Test
        fun `two packed items and a courier assignment dispatch and complete the saga`() {
            // Given
            val started = start(shipment(), ShipmentStarted(SHIPMENT_ID))
            val afterFirstItem = shipment().step(started.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-1")))
            val afterItems = shipment().step(afterFirstItem.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-2")))

            // When
            val step = shipment().step(afterItems.state, SagaInput.event(CourierAssigned(SHIPMENT_ID)))

            // Then
            assertAll(
                { assertThat(step.state.completed()).isTrue() },
                { assertThat(step.effects).containsExactly(SagaEffect.issue(DispatchShipment(SHIPMENT_ID))) }
            )
        }

        @Test
        fun `two packed items and a pickup slot dispatch and complete the saga too`() {
            // Given
            val started = start(shipment(), ShipmentStarted(SHIPMENT_ID))
            val afterFirstItem = shipment().step(started.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-1")))
            val afterItems = shipment().step(afterFirstItem.state, SagaInput.event(ItemPacked(SHIPMENT_ID, "sku-2")))

            // When
            val step = shipment().step(afterItems.state, SagaInput.event(PickupScheduled(SHIPMENT_ID)))

            // Then
            assertAll(
                { assertThat(step.state.completed()).isTrue() },
                { assertThat(step.effects).containsExactly(SagaEffect.issue(DispatchShipment(SHIPMENT_ID))) }
            )
        }
    }

    @Nested
    @DisplayName("when a step waits for a reading above a threshold")
    inner class WhenAStepWaitsForAReadingAboveAThreshold {

        @Test
        fun `a reading at the threshold does not fulfil the condition`() {
            // Given
            val started = start(sensor(), SensorArmed(SENSOR_ID))

            // When
            val step = sensor().step(started.state, SagaInput.event(ReadingTaken(SENSOR_ID, 40)))

            // Then
            assertThat(step.state.completed()).isFalse()
        }

        @Test
        fun `a reading above the threshold raises the alarm and completes the saga`() {
            // Given
            val started = start(sensor(), SensorArmed(SENSOR_ID))

            // When
            val step = sensor().step(started.state, SagaInput.event(ReadingTaken(SENSOR_ID, 41)))

            // Then
            assertAll(
                { assertThat(step.state.completed()).isTrue() },
                { assertThat(step.effects).containsExactly(SagaEffect.issue(RaiseAlarm(SENSOR_ID))) }
            )
        }

        @Test
        fun `a low reading followed by a high one still raises the alarm`() {
            // Given
            val started = start(sensor(), SensorArmed(SENSOR_ID))
            val afterLow = sensor().step(started.state, SagaInput.event(ReadingTaken(SENSOR_ID, 10)))

            // When
            val afterHigh = sensor().step(afterLow.state, SagaInput.event(ReadingTaken(SENSOR_ID, 45)))

            // Then
            assertAll(
                { assertThat(afterLow.state.completed()).isFalse() },
                { assertThat(afterHigh.state.completed()).isTrue() },
                { assertThat(afterHigh.effects).containsExactly(SagaEffect.issue(RaiseAlarm(SENSOR_ID))) }
            )
        }
    }

    @Nested
    @DisplayName("when a step mixes a classic branch with a window condition")
    inner class WhenAStepMixesAClassicBranchWithAWindowCondition {

        @Test
        fun `a single full payment releases the goods through the classic branch`() {
            // Given
            val started = start(purchase(), PurchaseStarted(PURCHASE_ID, 80))

            // When
            val step = purchase().step(started.state, SagaInput.event(PaymentReceived(PURCHASE_ID, 80)))

            // Then
            assertAll(
                { assertThat(step.state.completed()).isTrue() },
                { assertThat(step.effects).containsExactly(SagaEffect.issue(ReleaseGoods(PURCHASE_ID))) }
            )
        }

        @Test
        fun `two partial payments release the goods through the window condition`() {
            // Given
            val started = start(purchase(), PurchaseStarted(PURCHASE_ID, 80))
            val afterFirst = purchase().step(started.state, SagaInput.event(PaymentReceived(PURCHASE_ID, 30)))

            // When
            val afterSecond = purchase().step(afterFirst.state, SagaInput.event(PaymentReceived(PURCHASE_ID, 30)))

            // Then
            assertAll(
                { assertThat(afterFirst.state.completed()).isFalse() },
                { assertThat(afterSecond.state.completed()).isTrue() },
                {
                    assertThat(afterSecond.effects).containsExactly(
                        SagaEffect.issue(ReleaseGoods(PURCHASE_ID)),
                        SagaEffect.issue(NotifyLayawayComplete(PURCHASE_ID))
                    )
                }
            )
        }

        @Test
        fun `a full payment arriving second satisfies both branches but the declared first classic branch wins`() {
            // Given
            val started = start(purchase(), PurchaseStarted(PURCHASE_ID, 80))
            val afterFirst = purchase().step(started.state, SagaInput.event(PaymentReceived(PURCHASE_ID, 30)))

            // When
            val afterSecond = purchase().step(afterFirst.state, SagaInput.event(PaymentReceived(PURCHASE_ID, 80)))

            // Then
            assertAll(
                { assertThat(afterSecond.state.completed()).isTrue() },
                {
                    // The window condition is also satisfied here (two PaymentReceived events), but the classic branch is declared first.
                    assertThat(afterSecond.effects).containsExactly(SagaEffect.issue(ReleaseGoods(PURCHASE_ID)))
                }
            )
        }
    }

    companion object {

        private const val GAME_ID = "game-1"
        private const val AUCTION_ID = "auction-1"
        private const val REVIEW_ID = "review-1"
        private const val SHIPMENT_ID = "shipment-1"
        private const val SENSOR_ID = "sensor-1"
        private const val PURCHASE_ID = "purchase-1"

        /** Fixed so the absolute timeout never depends on the machine's clock or zone. */
        private val ENDS_AT: Instant = Instant.parse("2026-07-28T18:00:00Z")

        sealed interface GameEvent {
            val gameId: String
        }

        data class GameCreated(override val gameId: String) : GameEvent
        data class PlayerJoined(override val gameId: String) : GameEvent
        data class PlayerReady(override val gameId: String) : GameEvent

        data class CloseGame(val gameId: String)

        sealed interface AuctionEvent {
            val auctionId: String
        }

        data class AuctionStarted(override val auctionId: String, val endsAt: Instant) : AuctionEvent
        data class BidPlaced(override val auctionId: String, val amount: Int) : AuctionEvent

        data class CloseAuction(val auctionId: String)

        sealed interface ReviewEvent {
            val reviewId: String
        }

        data class ReviewStarted(override val reviewId: String) : ReviewEvent
        data class Approved(override val reviewId: String) : ReviewEvent
        data class Rejected(override val reviewId: String) : ReviewEvent

        sealed interface ReviewCommand
        data class Publish(val reviewId: String) : ReviewCommand
        data class Discard(val reviewId: String) : ReviewCommand

        sealed interface ShipmentEvent {
            val shipmentId: String
        }

        data class ShipmentStarted(override val shipmentId: String) : ShipmentEvent
        data class ItemPacked(override val shipmentId: String, val sku: String) : ShipmentEvent
        data class CourierAssigned(override val shipmentId: String) : ShipmentEvent
        data class PickupScheduled(override val shipmentId: String) : ShipmentEvent

        data class DispatchShipment(val shipmentId: String)

        sealed interface SensorEvent {
            val sensorId: String
        }

        data class SensorArmed(override val sensorId: String) : SensorEvent
        data class ReadingTaken(override val sensorId: String, val celsius: Int) : SensorEvent

        data class RaiseAlarm(val sensorId: String)

        sealed interface PurchaseEvent {
            val purchaseId: String
        }

        data class PurchaseStarted(override val purchaseId: String, val total: Int) : PurchaseEvent
        data class PaymentReceived(override val purchaseId: String, val amount: Int) : PurchaseEvent

        sealed interface PurchaseCommand
        data class ReleaseGoods(val purchaseId: String) : PurchaseCommand
        data class NotifyLayawayComplete(val purchaseId: String) : PurchaseCommand

        private fun lobby(): Saga<GameEvent, FlowState<GameEvent>, CloseGame> = saga {
            startsOn<GameCreated>()
            correlateAll { it.gameId }
            step("awaiting-players") {
                on<PlayerJoined>(then = next)
                timeout(after = Duration.ofMinutes(10), then = end) { received ->
                    issue(CloseGame(received.initiating<GameCreated>().gameId))
                }
            }
            step("waiting-for-both-players") {
                join(expect<PlayerReady>(2), then = end)
            }
        }

        private fun auction(): Saga<AuctionEvent, FlowState<AuctionEvent>, CloseAuction> = saga {
            startsOn<AuctionStarted>()
            correlate<AuctionStarted> { it.auctionId }
            correlate<BidPlaced> { it.auctionId }
            step("bidding") {
                on<BidPlaced>(then = transitionTo("bidding"))
                timeout(at = { received -> received.initiating<AuctionStarted>().endsAt }, then = end) { received ->
                    issue(CloseAuction(received.initiating<AuctionStarted>().auctionId))
                }
            }
        }

        private fun review(): Saga<ReviewEvent, FlowState<ReviewEvent>, ReviewCommand> = saga {
            startsOn<ReviewStarted>()
            correlateAll { it.reviewId }
            step("awaiting-decision") {
                on(anyOf(event<Approved>(2), event<Rejected>()), then = end) { received ->
                    if (received.all(Rejected::class.java).isEmpty()) {
                        issue(Publish(received.initiating<ReviewStarted>().reviewId))
                    } else {
                        issue(Discard(received.initiating<ReviewStarted>().reviewId))
                    }
                }
            }
        }

        private fun shipment(): Saga<ShipmentEvent, FlowState<ShipmentEvent>, DispatchShipment> = saga {
            startsOn<ShipmentStarted>()
            correlateAll { it.shipmentId }
            step("packing") {
                on(allOf(event<ItemPacked>(2), anyOf(event<CourierAssigned>(), event<PickupScheduled>())), then = end) { received ->
                    issue(DispatchShipment(received.initiating<ShipmentStarted>().shipmentId))
                }
            }
        }

        private fun sensor(): Saga<SensorEvent, FlowState<SensorEvent>, RaiseAlarm> = saga {
            startsOn<SensorArmed>()
            correlateAll { it.sensorId }
            step("monitoring") {
                on(event<ReadingTaken> { it.celsius > 40 }, then = end) { received ->
                    issue(RaiseAlarm(received.initiating<SensorArmed>().sensorId))
                }
            }
        }

        private fun purchase(): Saga<PurchaseEvent, FlowState<PurchaseEvent>, PurchaseCommand> = saga {
            startsOn<PurchaseStarted>()
            correlateAll { it.purchaseId }
            // A single payment covering the total releases immediately; otherwise two installments, of any amount, do.
            step("collecting-payment") {
                on<PaymentReceived>(
                    then = end,
                    onlyIf = { payment, received -> payment.amount >= received.initiating<PurchaseStarted>().total }
                ) { payment ->
                    issue(ReleaseGoods(payment.purchaseId))
                }
                on(event<PaymentReceived>(2), then = end) { received ->
                    issue(ReleaseGoods(received.initiating<PurchaseStarted>().purchaseId))
                    issue(NotifyLayawayComplete(received.initiating<PurchaseStarted>().purchaseId))
                }
            }
        }

        /**
         * Applies a start event the way an executor would. [Saga.step] deliberately leaves out [Saga.onStart], so a
         * start event's effects are onStart's followed by react's.
         */
        private fun <E : Any, C : Any> start(saga: Saga<E, FlowState<E>, C>, event: E): Saga.Step<FlowState<E>, C> {
            val state = saga.evolve(saga.initialState(), SagaInput.event(event))
            val effects = saga.onStart(state, event) + saga.react(state, SagaInput.event(event))
            return Saga.Step(state, effects)
        }
    }
}
