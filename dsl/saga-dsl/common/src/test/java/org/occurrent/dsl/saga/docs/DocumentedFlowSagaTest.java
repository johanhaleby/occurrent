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

package org.occurrent.dsl.saga.docs;

import org.junit.jupiter.api.*;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.Expectation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.StepCondition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.flow.FlowSaga.stepTimer;

/**
 * The flow sagas the documentation's Testing chapter shows, kept compiling and passing here so a published snippet
 * cannot drift from the API. Every assertion goes through {@link Saga#step}, which folds evolve and react without any
 * clock, store or subscription, so a timeout is fired by naming its timer rather than by letting time pass.
 */
@DisplayName("DocumentedFlowSaga")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DocumentedFlowSagaTest {

    private static final String GAME_ID = "game-1";
    private static final String AUCTION_ID = "auction-1";

    /** Fixed so the absolute timeout never depends on the machine's clock or zone. */
    private static final Instant ENDS_AT = Instant.parse("2026-07-28T18:00:00Z");

    sealed interface GameEvent permits GameCreated, PlayerJoined, PlayerReady {
        String gameId();
    }

    record GameCreated(String gameId) implements GameEvent {
    }

    record PlayerJoined(String gameId) implements GameEvent {
    }

    record PlayerReady(String gameId) implements GameEvent {
    }

    record CloseGame(String gameId) {
    }

    sealed interface AuctionEvent permits AuctionStarted, BidPlaced {
        String auctionId();
    }

    record AuctionStarted(String auctionId, Instant endsAt) implements AuctionEvent {
    }

    record BidPlaced(String auctionId, int amount) implements AuctionEvent {
    }

    record CloseAuction(String auctionId) {
    }

    sealed interface ReviewEvent permits ReviewStarted, Approved, Rejected {
        String reviewId();
    }

    record ReviewStarted(String reviewId) implements ReviewEvent {
    }

    record Approved(String reviewId) implements ReviewEvent {
    }

    record Rejected(String reviewId) implements ReviewEvent {
    }

    sealed interface ReviewCommand permits Publish, Discard {
    }

    record Publish(String reviewId) implements ReviewCommand {
    }

    record Discard(String reviewId) implements ReviewCommand {
    }

    sealed interface ShipmentEvent permits ShipmentStarted, ItemPacked, CourierAssigned, PickupScheduled {
        String shipmentId();
    }

    record ShipmentStarted(String shipmentId) implements ShipmentEvent {
    }

    record ItemPacked(String shipmentId, String sku) implements ShipmentEvent {
    }

    record CourierAssigned(String shipmentId) implements ShipmentEvent {
    }

    record PickupScheduled(String shipmentId) implements ShipmentEvent {
    }

    record DispatchShipment(String shipmentId) {
    }

    sealed interface SensorEvent permits SensorArmed, ReadingTaken {
        String sensorId();
    }

    record SensorArmed(String sensorId) implements SensorEvent {
    }

    record ReadingTaken(String sensorId, int celsius) implements SensorEvent {
    }

    record RaiseAlarm(String sensorId) {
    }

    sealed interface PurchaseEvent permits PurchaseStarted, PaymentReceived {
        String purchaseId();
    }

    record PurchaseStarted(String purchaseId, int total) implements PurchaseEvent {
    }

    record PaymentReceived(String purchaseId, int amount) implements PurchaseEvent {
    }

    sealed interface PurchaseCommand permits ReleaseGoods, NotifyLayawayComplete {
    }

    record ReleaseGoods(String purchaseId) implements PurchaseCommand {
    }

    record NotifyLayawayComplete(String purchaseId) implements PurchaseCommand {
    }

    @Nested
    @DisplayName("when a lobby waits for a player to join")
    class When_a_lobby_waits_for_a_player_to_join {

        @Test
        void a_player_joining_advances_to_the_next_step_and_cancels_the_steps_timeout() {
            // Given
            Saga.Step<FlowState<GameEvent>, CloseGame> started = start(lobby(), new GameCreated(GAME_ID));

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> step = lobby().step(started.state(), SagaInput.event(new PlayerJoined(GAME_ID)));

            // Then
            assertAll(
                    () -> assertThat(step.state().currentStep()).isEqualTo("waiting-for-both-players"),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout(stepTimer("awaiting-players")))
            );
        }

        @Test
        void a_player_joining_issues_no_command_even_though_it_produces_an_effect() {
            // Given
            Saga.Step<FlowState<GameEvent>, CloseGame> started = start(lobby(), new GameCreated(GAME_ID));

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> step = lobby().step(started.state(), SagaInput.event(new PlayerJoined(GAME_ID)));

            // Then
            assertThat(step.issuedCommands()).isEmpty();
        }

        @Test
        void the_timeout_firing_closes_the_game_and_completes_the_saga() {
            // Given
            Saga.Step<FlowState<GameEvent>, CloseGame> started = start(lobby(), new GameCreated(GAME_ID));

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> step =
                    lobby().step(started.state(), SagaInput.timeout(GAME_ID, stepTimer("awaiting-players")));

            // Then
            assertAll(
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new CloseGame(GAME_ID))),
                    () -> assertThat(step.state().completed()).isTrue()
            );
        }

        @Test
        void a_timer_name_the_saga_does_not_know_leaves_the_state_and_the_effects_untouched() {
            // Given
            Saga.Step<FlowState<GameEvent>, CloseGame> started = start(lobby(), new GameCreated(GAME_ID));

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> step =
                    lobby().step(started.state(), SagaInput.timeout(GAME_ID, stepTimer("no-such-step")));

            // Then
            assertAll(
                    () -> assertThat(step.state().currentStep()).isEqualTo("awaiting-players"),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a step joins on two players readying up")
    class When_a_step_joins_on_two_players_readying_up {

        @Test
        void one_player_readying_up_does_not_leave_the_join_step() {
            // Given
            FlowState<GameEvent> joining = joinStepEntered();

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> step = lobby().step(joining, SagaInput.event(new PlayerReady(GAME_ID)));

            // Then
            assertAll(
                    () -> assertThat(step.state().currentStep()).isEqualTo("waiting-for-both-players"),
                    () -> assertThat(step.state().completed()).isFalse()
            );
        }

        @Test
        void the_second_player_readying_up_fulfils_the_join_and_completes_the_saga() {
            // Given
            Saga.Step<FlowState<GameEvent>, CloseGame> afterFirst =
                    lobby().step(joinStepEntered(), SagaInput.event(new PlayerReady(GAME_ID)));

            // When
            Saga.Step<FlowState<GameEvent>, CloseGame> afterSecond =
                    lobby().step(afterFirst.state(), SagaInput.event(new PlayerReady(GAME_ID)));

            // Then
            assertThat(afterSecond.state().completed()).isTrue();
        }

        private FlowState<GameEvent> joinStepEntered() {
            Saga.Step<FlowState<GameEvent>, CloseGame> started = start(lobby(), new GameCreated(GAME_ID));
            return lobby().step(started.state(), SagaInput.event(new PlayerJoined(GAME_ID))).state();
        }
    }

    @Nested
    @DisplayName("when an auction re-enters its bidding step on every bid")
    class When_an_auction_re_enters_its_bidding_step_on_every_bid {

        @Test
        void a_bid_keeps_the_saga_in_the_bidding_step() {
            // Given
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> started = start(auction(), new AuctionStarted(AUCTION_ID, ENDS_AT));

            // When
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> step =
                    auction().step(started.state(), SagaInput.event(new BidPlaced(AUCTION_ID, 100)));

            // Then
            assertAll(
                    () -> assertThat(step.state().currentStep()).isEqualTo("bidding"),
                    () -> assertThat(step.state().completed()).isFalse()
            );
        }

        @Test
        void a_second_bid_still_keeps_the_saga_in_the_bidding_step() {
            // Given
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> afterFirstBid = auction().step(
                    start(auction(), new AuctionStarted(AUCTION_ID, ENDS_AT)).state(),
                    SagaInput.event(new BidPlaced(AUCTION_ID, 100)));

            // When
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> afterSecondBid =
                    auction().step(afterFirstBid.state(), SagaInput.event(new BidPlaced(AUCTION_ID, 150)));

            // Then
            assertThat(afterSecondBid.state().currentStep()).isEqualTo("bidding");
        }

        @Test
        void the_deadline_firing_closes_the_auction_and_completes_the_saga() {
            // Given
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> started = start(auction(), new AuctionStarted(AUCTION_ID, ENDS_AT));

            // When
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> step =
                    auction().step(started.state(), SagaInput.timeout(AUCTION_ID, stepTimer("bidding")));

            // Then
            assertAll(
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new CloseAuction(AUCTION_ID))),
                    () -> assertThat(step.state().completed()).isTrue()
            );
        }

        @Test
        void the_deadline_still_closes_the_auction_after_bids_have_looped_the_step() {
            // Given
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> afterBid = auction().step(
                    start(auction(), new AuctionStarted(AUCTION_ID, ENDS_AT)).state(),
                    SagaInput.event(new BidPlaced(AUCTION_ID, 100)));

            // When
            Saga.Step<FlowState<AuctionEvent>, CloseAuction> step =
                    auction().step(afterBid.state(), SagaInput.timeout(AUCTION_ID, stepTimer("bidding")));

            // Then
            assertThat(step.effects()).containsExactly(SagaEffect.issue(new CloseAuction(AUCTION_ID)));
        }
    }

    @Nested
    @DisplayName("when a step waits for either two approvals or a single rejection")
    class When_a_step_waits_for_either_two_approvals_or_a_single_rejection {

        private static final String REVIEW_ID = "review-1";

        @Test
        void one_approval_does_not_fulfil_the_condition() {
            // Given
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> started = start(review(), new ReviewStarted(REVIEW_ID));

            // When
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> step = review().step(started.state(), SagaInput.event(new Approved(REVIEW_ID)));

            // Then
            assertThat(step.state().completed()).isFalse();
        }

        @Test
        void two_approvals_publish_and_complete_the_saga() {
            // Given
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> started = start(review(), new ReviewStarted(REVIEW_ID));
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> afterFirst =
                    review().step(started.state(), SagaInput.event(new Approved(REVIEW_ID)));

            // When
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> afterSecond =
                    review().step(afterFirst.state(), SagaInput.event(new Approved(REVIEW_ID)));

            // Then
            assertAll(
                    () -> assertThat(afterSecond.state().completed()).isTrue(),
                    () -> assertThat(afterSecond.effects()).containsExactly(SagaEffect.issue(new Publish(REVIEW_ID)))
            );
        }

        @Test
        void a_single_rejection_discards_and_completes_the_saga_immediately() {
            // Given
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> started = start(review(), new ReviewStarted(REVIEW_ID));

            // When
            Saga.Step<FlowState<ReviewEvent>, ReviewCommand> step = review().step(started.state(), SagaInput.event(new Rejected(REVIEW_ID)));

            // Then
            assertAll(
                    () -> assertThat(step.state().completed()).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new Discard(REVIEW_ID)))
            );
        }
    }

    @Nested
    @DisplayName("when a step needs two packed items plus either a courier or a pickup slot")
    class When_a_step_needs_two_packed_items_plus_either_a_courier_or_a_pickup_slot {

        private static final String SHIPMENT_ID = "shipment-1";

        @Test
        void two_packed_items_alone_do_not_fulfil_the_condition() {
            // Given
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> started = start(shipment(), new ShipmentStarted(SHIPMENT_ID));
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> afterFirstItem =
                    shipment().step(started.state(), SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-1")));

            // When
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> afterSecondItem =
                    shipment().step(afterFirstItem.state(), SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-2")));

            // Then
            assertThat(afterSecondItem.state().completed()).isFalse();
        }

        @Test
        void two_packed_items_and_a_courier_assignment_dispatch_and_complete_the_saga() {
            // Given
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> started = start(shipment(), new ShipmentStarted(SHIPMENT_ID));
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> afterItems = shipment().step(
                    shipment().step(started.state(), SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-1"))).state(),
                    SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-2")));

            // When
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> step =
                    shipment().step(afterItems.state(), SagaInput.event(new CourierAssigned(SHIPMENT_ID)));

            // Then
            assertAll(
                    () -> assertThat(step.state().completed()).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new DispatchShipment(SHIPMENT_ID)))
            );
        }

        @Test
        void two_packed_items_and_a_pickup_slot_dispatch_and_complete_the_saga_too() {
            // Given
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> started = start(shipment(), new ShipmentStarted(SHIPMENT_ID));
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> afterItems = shipment().step(
                    shipment().step(started.state(), SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-1"))).state(),
                    SagaInput.event(new ItemPacked(SHIPMENT_ID, "sku-2")));

            // When
            Saga.Step<FlowState<ShipmentEvent>, DispatchShipment> step =
                    shipment().step(afterItems.state(), SagaInput.event(new PickupScheduled(SHIPMENT_ID)));

            // Then
            assertAll(
                    () -> assertThat(step.state().completed()).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new DispatchShipment(SHIPMENT_ID)))
            );
        }
    }

    @Nested
    @DisplayName("when a step waits for a reading above a threshold")
    class When_a_step_waits_for_a_reading_above_a_threshold {

        private static final String SENSOR_ID = "sensor-1";

        @Test
        void a_reading_at_the_threshold_does_not_fulfil_the_condition() {
            // Given
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> started = start(sensor(), new SensorArmed(SENSOR_ID));

            // When
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> step =
                    sensor().step(started.state(), SagaInput.event(new ReadingTaken(SENSOR_ID, 40)));

            // Then
            assertThat(step.state().completed()).isFalse();
        }

        @Test
        void a_reading_above_the_threshold_raises_the_alarm_and_completes_the_saga() {
            // Given
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> started = start(sensor(), new SensorArmed(SENSOR_ID));

            // When
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> step =
                    sensor().step(started.state(), SagaInput.event(new ReadingTaken(SENSOR_ID, 41)));

            // Then
            assertAll(
                    () -> assertThat(step.state().completed()).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new RaiseAlarm(SENSOR_ID)))
            );
        }

        @Test
        void a_low_reading_followed_by_a_high_one_still_raises_the_alarm() {
            // Given
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> started = start(sensor(), new SensorArmed(SENSOR_ID));
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> afterLow =
                    sensor().step(started.state(), SagaInput.event(new ReadingTaken(SENSOR_ID, 10)));

            // When
            Saga.Step<FlowState<SensorEvent>, RaiseAlarm> afterHigh =
                    sensor().step(afterLow.state(), SagaInput.event(new ReadingTaken(SENSOR_ID, 45)));

            // Then
            assertAll(
                    () -> assertThat(afterLow.state().completed()).isFalse(),
                    () -> assertThat(afterHigh.state().completed()).isTrue(),
                    () -> assertThat(afterHigh.effects()).containsExactly(SagaEffect.issue(new RaiseAlarm(SENSOR_ID)))
            );
        }
    }

    @Nested
    @DisplayName("when a step mixes a classic branch with a window condition")
    class When_a_step_mixes_a_classic_branch_with_a_window_condition {

        private static final String PURCHASE_ID = "purchase-1";

        @Test
        void a_single_full_payment_releases_the_goods_through_the_classic_branch() {
            // Given
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> started = start(purchase(), new PurchaseStarted(PURCHASE_ID, 80));

            // When
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> step =
                    purchase().step(started.state(), SagaInput.event(new PaymentReceived(PURCHASE_ID, 80)));

            // Then
            assertAll(
                    () -> assertThat(step.state().completed()).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new ReleaseGoods(PURCHASE_ID)))
            );
        }

        @Test
        void two_partial_payments_release_the_goods_through_the_window_condition() {
            // Given
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> started = start(purchase(), new PurchaseStarted(PURCHASE_ID, 80));
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> afterFirst =
                    purchase().step(started.state(), SagaInput.event(new PaymentReceived(PURCHASE_ID, 30)));

            // When
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> afterSecond =
                    purchase().step(afterFirst.state(), SagaInput.event(new PaymentReceived(PURCHASE_ID, 30)));

            // Then
            assertAll(
                    () -> assertThat(afterFirst.state().completed()).isFalse(),
                    () -> assertThat(afterSecond.state().completed()).isTrue(),
                    () -> assertThat(afterSecond.effects()).containsExactly(
                            SagaEffect.issue(new ReleaseGoods(PURCHASE_ID)),
                            SagaEffect.issue(new NotifyLayawayComplete(PURCHASE_ID)))
            );
        }

        @Test
        void a_full_payment_arriving_second_satisfies_both_branches_but_the_declared_first_classic_branch_wins() {
            // Given
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> started = start(purchase(), new PurchaseStarted(PURCHASE_ID, 80));
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> afterFirst =
                    purchase().step(started.state(), SagaInput.event(new PaymentReceived(PURCHASE_ID, 30)));

            // When
            Saga.Step<FlowState<PurchaseEvent>, PurchaseCommand> afterSecond =
                    purchase().step(afterFirst.state(), SagaInput.event(new PaymentReceived(PURCHASE_ID, 80)));

            // Then
            assertAll(
                    () -> assertThat(afterSecond.state().completed()).isTrue(),
                    () -> assertThat(afterSecond.effects())
                            .as("the window condition is also satisfied here (two PaymentReceived events), but the classic branch is declared first")
                            .containsExactly(SagaEffect.issue(new ReleaseGoods(PURCHASE_ID)))
            );
        }
    }

    private static Saga<GameEvent, FlowState<GameEvent>, CloseGame> lobby() {
        return FlowSaga.<GameEvent, CloseGame>builder()
                .startsOn(GameCreated.class)
                .correlateAll(GameEvent::gameId)
                .step("awaiting-players", step -> step
                        .on(PlayerJoined.class, Continuation.next())
                        .timeout(Duration.ofMinutes(10), Continuation.end(),
                                received -> List.of(new CloseGame(received.initiating(GameCreated.class).gameId()))))
                .step("waiting-for-both-players", step -> step
                        .join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end()))
                .build();
    }

    private static Saga<AuctionEvent, FlowState<AuctionEvent>, CloseAuction> auction() {
        return FlowSaga.<AuctionEvent, CloseAuction>builder()
                .startsOn(AuctionStarted.class)
                .correlate(AuctionStarted.class, AuctionStarted::auctionId)
                .correlate(BidPlaced.class, BidPlaced::auctionId)
                .step("bidding", step -> step
                        .on(BidPlaced.class, Continuation.transitionTo("bidding"))
                        .timeout(received -> received.initiating(AuctionStarted.class).endsAt(), Continuation.end(),
                                received -> List.of(new CloseAuction(received.initiating(AuctionStarted.class).auctionId()))))
                .build();
    }

    private static Saga<ReviewEvent, FlowState<ReviewEvent>, ReviewCommand> review() {
        return FlowSaga.<ReviewEvent, ReviewCommand>builder()
                .startsOn(ReviewStarted.class)
                .correlateAll(ReviewEvent::reviewId)
                // One branch per outcome, in declaration order, first satisfied one winning. A single anyOf(...) branch that
                // works out afterwards which alternative fired would have to guess from the events, and the ordering that
                // decides a tie would sit inside the tree instead of in the declaration.
                .step("awaiting-decision", step -> step
                        .on(StepCondition.event(Rejected.class), Continuation.end(),
                                received -> List.of(new Discard(received.initiating(ReviewStarted.class).reviewId())))
                        .on(StepCondition.event(Approved.class, 2), Continuation.end(),
                                received -> List.of(new Publish(received.initiating(ReviewStarted.class).reviewId()))))
                .build();
    }

    private static Saga<ShipmentEvent, FlowState<ShipmentEvent>, DispatchShipment> shipment() {
        return FlowSaga.<ShipmentEvent, DispatchShipment>builder()
                .startsOn(ShipmentStarted.class)
                .correlateAll(ShipmentEvent::shipmentId)
                .step("packing", step -> step
                        .on(StepCondition.allOf(
                                        StepCondition.event(ItemPacked.class, 2),
                                        StepCondition.anyOf(StepCondition.event(CourierAssigned.class), StepCondition.event(PickupScheduled.class))),
                                Continuation.end(),
                                received -> List.of(new DispatchShipment(received.initiating(ShipmentStarted.class).shipmentId()))))
                .build();
    }

    private static Saga<SensorEvent, FlowState<SensorEvent>, RaiseAlarm> sensor() {
        return FlowSaga.<SensorEvent, RaiseAlarm>builder()
                .startsOn(SensorArmed.class)
                .correlateAll(SensorEvent::sensorId)
                .step("monitoring", step -> step
                        .on(StepCondition.event(ReadingTaken.class, (ReadingTaken reading) -> reading.celsius() > 40),
                                Continuation.end(),
                                received -> List.of(new RaiseAlarm(received.initiating(SensorArmed.class).sensorId()))))
                .build();
    }

    private static Saga<PurchaseEvent, FlowState<PurchaseEvent>, PurchaseCommand> purchase() {
        return FlowSaga.<PurchaseEvent, PurchaseCommand>builder()
                .startsOn(PurchaseStarted.class)
                .correlateAll(PurchaseEvent::purchaseId)
                // A single payment covering the total releases immediately, otherwise two installments, of any amount, do.
                .step("collecting-payment", step -> step
                        .on(PaymentReceived.class,
                                (payment, received) -> payment.amount() >= received.initiating(PurchaseStarted.class).total(),
                                Continuation.end(),
                                payment -> List.of(new ReleaseGoods(payment.purchaseId())))
                        .on(StepCondition.event(PaymentReceived.class, 2),
                                Continuation.end(),
                                received -> List.of(
                                        new ReleaseGoods(received.initiating(PurchaseStarted.class).purchaseId()),
                                        new NotifyLayawayComplete(received.initiating(PurchaseStarted.class).purchaseId()))))
                .build();
    }

    /**
     * Applies a start event the way an executor would. {@link Saga#step} deliberately leaves out {@link Saga#onStart},
     * so a start event's effects are onStart's followed by react's.
     */
    private static <E, C> Saga.Step<FlowState<E>, C> start(Saga<E, FlowState<E>, C> saga, E event) {
        FlowState<E> state = saga.evolve(saga.initialState(), SagaInput.event(event));
        List<SagaEffect<C>> effects = new ArrayList<>(saga.onStart(state, event));
        effects.addAll(saga.react(state, SagaInput.event(event)));
        return new Saga.Step<>(state, effects);
    }
}
