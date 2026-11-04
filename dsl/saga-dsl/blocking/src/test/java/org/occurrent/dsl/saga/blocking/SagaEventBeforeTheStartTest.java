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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An event that is not a start event and reaches an instance that has not started used to be skipped and acknowledged,
 * so the subscription never offered it again and it was lost (issue 1317). The instance now keeps it, and the save that
 * applies the start applies it too.
 */
@DisplayName("An event that arrives before the start of its instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaEventBeforeTheStartTest {

    private static final String ORDER = "order-1";
    private static final String ORDERS = "orders-stream";
    private static final String PAYMENTS = "payments-stream";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final int THE_CAP = 10_000;

    sealed interface OrderEvent permits OrderPlaced, ItemAdded, OrderClosed {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record OrderClosed(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits OpenOrder, Reserve, CloseOrder {
    }

    record OpenOrder(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    record CloseOrder(String orderId) implements OrderCommand {
    }

    record Order(List<String> items, boolean closed) {
    }

    /** Every event issues a command and the state records the order it was applied in, so a lost or misplaced event shows in both. */
    private static Saga<OrderEvent, Order, OrderCommand> orderSaga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(new Order(List.of(), false))
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> state)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenOrder(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> {
                    List<String> items = new ArrayList<>(state.items());
                    items.add(e.item());
                    return new Order(items, state.closed());
                })
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .evolve(OrderClosed.class, (state, e) -> new Order(state.items(), true))
                .react(OrderClosed.class, (state, e) -> List.of(SagaEffect.issue(new CloseOrder(e.orderId()))))
                .isTerminal(Order::closed)
                .build();
    }

    private CloudEventConverter<OrderEvent> converter;
    private SagaStateStore<Order> store;
    private SagaExecution<OrderEvent, Order, OrderCommand> execution;
    private final List<OrderCommand> issued = new ArrayList<>();
    private final AtomicBoolean openingFails = new AtomicBoolean(false);

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        store = SagaStateStore.inMemory();
        execution = new SagaExecution<>("orders", orderSaga(), store, command -> {
            if (openingFails.get() && command instanceof OpenOrder) {
                throw new IllegalStateException("cannot open the order");
            }
            issued.add(command);
        }, converter, SagaRunnerConfig.defaults(), event -> true, () -> NOW);
    }

    private void deliver(String stream, long streamVersion, OrderEvent event) {
        execution.onCloudEvent(streamKeyed(stream, streamVersion, event));
    }

    private SagaEnvelope<Order> stored() {
        return store.find(ORDER).orElseThrow();
    }

    private CloudEvent streamKeyed(String stream, long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(stream, streamVersion)).build();
    }

    private static OrderPlaced placed() {
        return new OrderPlaced("placed", ORDER);
    }

    private static ItemAdded added(String item) {
        return new ItemAdded("added-" + item, ORDER, item);
    }

    private static OrderClosed closed() {
        return new OrderClosed("closed", ORDER);
    }

    private static Reserve reserve(String item) {
        return new Reserve(ORDER, item);
    }

    @Nested
    @DisplayName("when the start has not arrived")
    class WhenTheStartHasNotArrived {

        @Test
        void the_event_is_kept_on_an_instance_that_has_not_started_and_dispatches_nothing() {
            // Given

            // When
            deliver(ORDERS, 2, added("apple"));

            // Then
            SagaEnvelope<Order> instance = stored();
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(instance.started()).isFalse(),
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.version()).isEqualTo(1),
                    () -> assertThat(instance.early()).extracting(CloudEvent::getId).containsExactly("added-apple")
            );
        }
    }

    @Nested
    @DisplayName("when the start arrives after it")
    class WhenTheStartArrivesAfterIt {

        @Test
        void the_event_is_applied_and_its_command_dispatched_with_the_commands_of_the_start() {
            // Given
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple")),
                    () -> assertThat(stored().state().items()).containsExactly("apple")
            );
        }

        @Test
        void the_instance_keeps_no_event_once_the_start_has_applied_them() {
            // Given
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(stored().started()).isTrue(),
                    () -> assertThat(stored().early()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when it is delivered again before the start")
    class WhenItIsDeliveredAgainBeforeTheStart {

        @Test
        void it_is_applied_once() {
            // Given
            deliver(ORDERS, 2, added("apple"));
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple")),
                    () -> assertThat(stored().state().items()).containsExactly("apple")
            );
        }

        @Test
        void nothing_is_written() {
            // Given
            deliver(ORDERS, 2, added("apple"));
            SagaEnvelope<Order> afterTheFirstDelivery = stored();

            // When
            deliver(ORDERS, 2, added("apple"));

            // Then
            SagaEnvelope<Order> afterTheSecond = stored();
            assertAll(
                    () -> assertThat(afterTheSecond.version()).isEqualTo(afterTheFirstDelivery.version()),
                    () -> assertThat(afterTheSecond.early()).hasSize(1)
            );
        }

        @Test
        void another_event_with_the_same_content_but_its_own_id_is_kept_as_well() {
            // Given
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 3, new ItemAdded("added-apple-again", ORDER, "apple"));
            deliver(ORDERS, 1, placed());

            // Then
            assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple"), reserve("apple"));
        }
    }

    @Nested
    @DisplayName("when several arrived before the start")
    class WhenSeveralArrivedBeforeTheStart {

        @Test
        void events_of_different_streams_are_applied_in_the_order_they_arrived() {
            // Given
            deliver(PAYMENTS, 1, added("paid"));
            deliver(ORDERS, 2, added("ordered"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertThat(stored().state().items()).containsExactly("paid", "ordered");
        }

        @Test
        void versions_of_one_stream_that_arrived_out_of_order_are_applied_in_version_order() {
            // Given
            deliver(ORDERS, 3, added("third"));
            deliver(ORDERS, 2, added("second"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(stored().state().items()).containsExactly("second", "third"),
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("second"), reserve("third"))
            );
        }

        @Test
        void two_interleaved_streams_keep_their_arrival_slots_while_each_stream_runs_in_version_order() {
            // Given
            deliver(ORDERS, 3, added("orders-3"));
            deliver(PAYMENTS, 1, added("payments-1"));
            deliver(ORDERS, 2, added("orders-2"));
            deliver(PAYMENTS, 2, added("payments-2"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertThat(stored().state().items()).containsExactly("orders-2", "payments-1", "orders-3", "payments-2");
        }
    }

    @Nested
    @DisplayName("when one of them completes the instance")
    class WhenOneOfThemCompletesTheInstance {

        @Test
        void it_is_applied_after_the_events_that_arrived_after_it_on_other_streams_so_their_reactions_still_run() {
            // Given
            deliver(PAYMENTS, 1, closed());
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple"), new CloseOrder(ORDER)),
                    () -> assertThat(stored().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(stored().state().items()).containsExactly("apple")
            );
        }

        @Test
        void a_later_version_of_its_stream_goes_after_it_and_finds_the_instance_completed() {
            // Given
            deliver(PAYMENTS, 1, closed());
            deliver(PAYMENTS, 2, added("late"));
            deliver(ORDERS, 2, added("apple"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple"), new CloseOrder(ORDER)),
                    () -> assertThat(stored().state().items()).containsExactly("apple")
            );
        }

        @Test
        void an_event_that_does_not_complete_it_is_applied_in_its_place_even_when_a_completing_one_arrived_before_it() {
            // Given
            deliver(PAYMENTS, 1, closed());
            deliver(ORDERS, 2, added("first"));
            deliver(ORDERS, 3, added("second"));

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("first"), reserve("second"), new CloseOrder(ORDER));
        }
    }

    @Nested
    @DisplayName("when the start fails")
    class WhenTheStartFails {

        @Test
        void the_events_it_would_have_applied_stay_kept() {
            // Given
            deliver(ORDERS, 2, added("apple"));
            openingFails.set(true);

            // When
            Throwable thrown = catchThrowable(() -> deliver(ORDERS, 1, placed()));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("cannot open the order"),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stored().started()).isFalse(),
                    () -> assertThat(stored().early()).extracting(CloudEvent::getId).containsExactly("added-apple")
            );
        }

        @Test
        void the_redelivered_start_applies_them() {
            // Given
            deliver(ORDERS, 2, added("apple"));
            openingFails.set(true);
            catchThrowable(() -> deliver(ORDERS, 1, placed()));
            openingFails.set(false);

            // When
            deliver(ORDERS, 1, placed());

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenOrder(ORDER), reserve("apple")),
                    () -> assertThat(stored().state().items()).containsExactly("apple"),
                    () -> assertThat(stored().early()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the instance keeps the most it will")
    class WhenTheInstanceKeepsTheMostItWill {

        @Test
        void the_next_event_is_refused_and_everything_kept_stays_kept() {
            // Given
            fillTheInstanceWithKeptEvents(THE_CAP);

            // When
            Throwable thrown = catchThrowable(() -> deliver(ORDERS, THE_CAP + 2L, added("one-too-many")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("10000").hasMessageContaining("added-one-too-many"),
                    () -> assertThat(stored().early()).hasSize(THE_CAP),
                    () -> assertThat(stored().early()).extracting(CloudEvent::getId).doesNotContain("added-one-too-many")
            );
        }

        @Test
        void the_last_event_that_fits_is_kept() {
            // Given
            fillTheInstanceWithKeptEvents(THE_CAP - 1);

            // When
            deliver(ORDERS, THE_CAP + 1L, added("the-last-that-fits"));

            // Then
            assertThat(stored().early()).hasSize(THE_CAP).extracting(CloudEvent::getId).last().isEqualTo("added-the-last-that-fits");
        }

        @Test
        void a_redelivery_of_a_kept_event_is_not_refused() {
            // Given
            fillTheInstanceWithKeptEvents(THE_CAP);

            // When
            Throwable thrown = catchThrowable(() -> deliver(ORDERS, 2, added("kept-0")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(stored().early()).hasSize(THE_CAP)
            );
        }

        private void fillTheInstanceWithKeptEvents(int count) {
            IntStream.range(0, count).forEach(i -> deliver(ORDERS, i + 2L, added("kept-" + i)));
        }
    }
}
