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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Positions are not commit order. A transaction that commits late can be handed to the saga with a lower global position
 * than an event it has already applied, so a position says nothing about whether an event was seen before. Up to 0.34.0 a
 * saga kept the highest position it had applied and skipped everything at or below it, which lost such an event.
 */
@DisplayName("A global position as a saga's redelivery key")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaPositionIsNotARedeliveryKeyTest {

    private static final String ORDER = "order-1";
    private static final SagaRunnerConfig FAST_POLL_CONFIG = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));
    private static final SagaRunnerConfig BEST_EFFORT = FAST_POLL_CONFIG.withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ReserveStock, ShipOrder {
    }

    record ReserveStock(String orderId) implements OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Paid {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Paid(String orderId) implements OrderState {
    }

    /** Both events issue a command, so a skipped event shows up as a missing command. */
    private static Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ReserveStock(e.orderId()))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .build();
    }

    private CloudEventConverter<OrderEvent> converter;
    private PushSubscriptionModel pushModel;
    private final List<OrderCommand> issued = new CopyOnWriteArrayList<>();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        pushModel = new PushSubscriptionModel();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
    }

    private SagaSubscription run(SagaRunnerConfig config) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(pushModel, converter)
                .run("orders", orderFulfillment(), SagaStateStore.inMemory(), issued::add, null, config);
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    @Nested
    @DisplayName("when events carry only a position")
    class WhenEventsCarryOnlyAPosition {

        @Test
        void a_position_only_event_with_a_lower_position_than_an_earlier_one_is_still_applied_under_best_effort() {
            // Given
            run(BEST_EFFORT);

            // When the second event commits late, so it arrives with a lower position than the first
            pushModel.accept(positionOnly(42, new OrderPlaced("placed", ORDER)));
            pushModel.accept(positionOnly(41, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER), new ShipOrder(ORDER));
        }

        @Test
        void a_position_only_event_with_the_position_of_an_earlier_one_is_still_applied_under_best_effort() {
            // Given
            run(BEST_EFFORT);

            // When
            pushModel.accept(positionOnly(42, new OrderPlaced("placed", ORDER)));
            pushModel.accept(positionOnly(42, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER), new ShipOrder(ORDER));
        }

        @Test
        void both_events_are_refused_before_their_reactions_run_under_the_default_REQUIRED() {
            // Given
            SagaSubscription subscription = run(FAST_POLL_CONFIG);

            // When
            Throwable first = catchThrowable(() -> pushModel.accept(positionOnly(42, new OrderPlaced("placed", ORDER))));
            Throwable second = catchThrowable(() -> pushModel.accept(positionOnly(41, new PaymentReserved("paid", ORDER))));

            // Then
            assertAll(
                    () -> assertThat(first).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(second).isInstanceOf(SagaRedeliveryDetectionException.class),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(subscription.instances().find(ORDER)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when events carry a stream key")
    class WhenEventsCarryAStreamKey {

        @Test
        void an_event_with_a_lower_position_than_an_earlier_one_is_applied_when_its_stream_version_is_higher() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            pushModel.accept(streamKeyed(1, 42, new OrderPlaced("placed", ORDER)));
            pushModel.accept(streamKeyed(2, 41, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER), new ShipOrder(ORDER));
        }

        @Test
        void a_redelivered_event_is_skipped_even_when_it_comes_back_with_a_higher_position() {
            // Given
            run(FAST_POLL_CONFIG);
            pushModel.accept(streamKeyed(1, 41, new OrderPlaced("placed", ORDER)));

            // When
            pushModel.accept(streamKeyed(1, 99, new OrderPlaced("placed", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER));
        }
    }

    @Nested
    @DisplayName("when a stream-keyed event and a position-only event meet")
    class WhenAStreamKeyedEventAndAPositionOnlyEventMeet {

        @Test
        void a_position_only_event_with_a_lower_position_after_a_stream_keyed_one_is_applied() {
            // Given
            run(BEST_EFFORT);

            // When
            pushModel.accept(streamKeyed(1, 42, new OrderPlaced("placed", ORDER)));
            pushModel.accept(positionOnly(41, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER), new ShipOrder(ORDER));
        }

        @Test
        void a_stream_keyed_event_with_a_lower_position_after_a_position_only_one_is_applied() {
            // Given
            run(BEST_EFFORT);

            // When
            pushModel.accept(positionOnly(42, new OrderPlaced("placed", ORDER)));
            pushModel.accept(streamKeyed(1, 41, new PaymentReserved("paid", ORDER)));

            // Then
            assertThat(issued).containsExactly(new ReserveStock(ORDER), new ShipOrder(ORDER));
        }
    }

    // What a feed that drops the stream extensions but keeps the position delivers.
    private CloudEvent positionOnly(long position, OrderEvent event) {
        return OccurrentCloudEventExtension.withPosition(converter.toCloudEvent(event), position);
    }

    // The stream key and the position an event store writes.
    private CloudEvent streamKeyed(long streamVersion, long position, OrderEvent event) {
        return OccurrentCloudEventExtension.withPosition(
                CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(ORDER, streamVersion)).build(),
                position);
    }
}
