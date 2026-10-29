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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A feed that does not deliver in commit order can bring an event to a saga before the event that starts its instance.
 * Acknowledging it would lose its reaction, because an instance that has not started cannot apply it, so the executor
 * holds the event in the state store and applies it once the start has been applied. These tests drive the executor
 * and an in-memory store together, the way a delivery does, and look at what the instance, the issued commands and the
 * held events are afterwards.
 */
@DisplayName("A saga event that arrives before its instance starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaHeldEventTest {

    private static final String ORDER = "order-1";
    private static final String ORDER_STREAM = "order-stream";
    private static final String PAYMENT_STREAM = "payment-stream";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();

    sealed interface OrderEvent permits OrderPlaced, ItemAdded, PaymentReceived {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record PaymentReceived(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits OpenLedger, Reserve, Ship {
    }

    record OpenLedger(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    record Ship(String orderId) implements OrderCommand {
    }

    record Order(List<String> items, boolean paid) {
        Order withItem(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(added, paid);
        }
    }

    /**
     * The state is {@code null} until the start, so applying any other event before it fails with a NullPointerException.
     * Every event issues a command, so an event that is skipped shows up as a missing one, and the items are kept in the
     * order they were applied.
     */
    private Saga<OrderEvent, Order, OrderCommand> orderSaga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(List.of(), false))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> {
                    if (unevolvableItems.contains(e.item())) {
                        throw evolveFailure;
                    }
                    return state.withItem(e.item());
                })
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .evolve(PaymentReceived.class, (state, e) -> new Order(state.items(), true))
                .react(PaymentReceived.class, (state, e) -> List.of(SagaEffect.issue(new Ship(e.orderId()))))
                .isTerminal(Order::paid)
                .build();
    }

    private final IllegalStateException reservationFailure = new IllegalStateException("cannot reserve");
    private final IllegalStateException evolveFailure = new IllegalStateException("cannot evolve");
    private final Set<String> failingItems = ConcurrentHashMap.newKeySet();
    private final Set<String> unevolvableItems = ConcurrentHashMap.newKeySet();
    private final List<OrderCommand> issued = new CopyOnWriteArrayList<>();
    private final AtomicReference<Instant> now = new AtomicReference<>(NOW);
    private final AtomicBoolean holds = new AtomicBoolean(true);
    private CloudEventConverter<OrderEvent> converter;
    private InMemorySagaStateStore<Order> store;

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        store = new InMemorySagaStateStore<>();
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> execution() {
        return execution(store, SagaRunnerConfig.defaults());
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> execution(SagaStateStore<Order> stateStore, SagaRunnerConfig config) {
        return new SagaExecution<>("orders", orderSaga(), stateStore, this::dispatch, converter, config, event -> true, now::get, holds::get);
    }

    private void dispatch(OrderCommand command) {
        if (command instanceof Reserve reserve && failingItems.contains(reserve.item())) {
            throw reservationFailure;
        }
        issued.add(command);
    }

    /** Delivers the event and moves the clock on a second, so that two events are never held at the same instant. */
    private void deliver(SagaExecution<OrderEvent, Order, OrderCommand> execution, CloudEvent event) {
        execution.onCloudEvent(event);
        now.updateAndGet(instant -> instant.plusSeconds(1));
    }

    private Throwable deliverRefused(SagaExecution<OrderEvent, Order, OrderCommand> execution, CloudEvent event) {
        Throwable refused = catchThrowable(() -> execution.onCloudEvent(event));
        now.updateAndGet(instant -> instant.plusSeconds(1));
        assertThat(refused).as("the delivery is refused").isNotNull();
        return refused;
    }

    private CloudEvent placedAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new OrderPlaced("placed-" + streamVersion, ORDER));
    }

    private CloudEvent itemAt(String streamId, long streamVersion, String item) {
        return streamKeyed(streamId, streamVersion, new ItemAdded("item-" + item, ORDER, item));
    }

    private CloudEvent itemAt(long streamVersion, String item) {
        return itemAt(ORDER_STREAM, streamVersion, item);
    }

    private CloudEvent paymentAt(String streamId, long streamVersion) {
        return streamKeyed(streamId, streamVersion, new PaymentReceived("payment-" + streamVersion, ORDER));
    }

    private CloudEvent unkeyedItem(String item) {
        return converter.toCloudEvent(new ItemAdded("item-" + item, ORDER, item));
    }

    // The stream key an event store writes, with the stream id and the version chosen by the test.
    private CloudEvent streamKeyed(String streamId, long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion)).build();
    }

    private List<String> heldKeys() {
        return store.heldIndex(ORDER).stream().map(HeldEventRef::key).sorted().toList();
    }

    private Order stateOfTheInstance() {
        return store.find(ORDER).orElseThrow().state();
    }

    private List<Long> pendingItemVersions() {
        StreamMarks marks = store.find(ORDER).map(instance -> instance.streamMarks().get(ORDER_STREAM)).orElse(null);
        return marks == null ? List.of() : marks.pending().getOrDefault(ITEM_ADDED_TYPE, List.of());
    }

    @Nested
    @DisplayName("when the delivery holds")
    class WhenTheDeliveryHolds {

        @Test
        void the_delivery_is_acknowledged_and_the_event_is_held_without_its_reaction_running() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAt(2, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(store.find(ORDER)).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@2"));
        }

        @Test
        void the_event_is_applied_exactly_once_when_the_start_arrives() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE));
        }

        @Test
        void the_instance_records_that_no_drain_is_due_once_the_held_event_is_applied() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(store.find(ORDER).orElseThrow().heldEventsChecked()).isTrue(),
                    () -> assertThat(store.find(ORDER).orElseThrow().drainedHeldKeys()).isEmpty());
        }

        @Test
        void the_event_is_not_applied_again_when_the_feed_delivers_it_again_after_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));
            deliver(execution, placedAt(1));

            // When
            deliver(execution, itemAt(2, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_start_is_not_run_again_when_the_feed_delivers_it_again_after_it_applied_the_held_event() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));
            deliver(execution, placedAt(1));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book"));
        }

        @Test
        void the_event_is_held_once_when_the_feed_delivers_it_twice_before_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();

            // When
            deliver(execution, itemAt(2, "book"));
            deliver(execution, itemAt(2, "book"));

            // Then
            assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@2");
        }

        @Test
        void the_reaction_runs_once_when_the_feed_delivered_the_event_twice_before_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));
            deliver(execution, itemAt(2, "book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book"));
        }

        @Test
        void an_event_without_a_stream_key_is_held_and_applied_exactly_once_when_the_start_arrives() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
            deliver(execution, unkeyedItem("book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void a_later_event_of_the_stream_that_started_the_instance_is_applied_when_it_was_held_before_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(6, "book"));

            // When
            deliver(execution, placedAt(5));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book"));
        }

        @Test
        void an_earlier_event_of_the_stream_that_started_the_instance_is_released_and_not_applied_because_it_committed_before_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(3, "book"));

            // When
            deliver(execution, placedAt(5));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void an_earlier_event_of_another_stream_than_the_one_that_started_the_instance_is_applied() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(PAYMENT_STREAM, 3, "book"));

            // When
            deliver(execution, placedAt(5));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book"));
        }

        @Test
        void the_events_deleted_with_the_instance_are_not_applied_when_a_start_arrives_afterwards() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));

            // When
            store.delete(ORDER);
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when several events are held")
    class WhenSeveralEventsAreHeld {

        @Test
        void two_events_of_one_stream_delivered_out_of_order_are_applied_in_version_order() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(10, "second"));
            deliver(execution, itemAt(9, "first"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "first"), new Reserve(ORDER, "second")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("first", "second"));
        }

        @Test
        void events_of_different_streams_are_applied_in_the_order_they_were_held() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(PAYMENT_STREAM, 1, "held-first"));
            deliver(execution, itemAt("shipping-stream", 1, "held-second"));
            deliver(execution, itemAt("billing-stream", 1, "held-third"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertThat(stateOfTheInstance().items()).containsExactly("held-first", "held-second", "held-third");
        }

        @Test
        void a_held_event_that_would_complete_the_instance_is_applied_after_the_events_held_for_other_streams() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, paymentAt(PAYMENT_STREAM, 1));
            deliver(execution, itemAt(2, "book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book"), new Ship(ORDER)),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_events_held_for_the_stream_of_the_completing_event_after_it_are_released_unapplied_because_the_instance_completed() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, paymentAt(PAYMENT_STREAM, 1));
            deliver(execution, itemAt(PAYMENT_STREAM, 2, "late"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Ship(ORDER)),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when the hold races the start")
    class WhenTheHoldRacesTheStart {

        @Test
        void an_event_that_finds_the_instance_started_after_it_was_held_is_applied_by_its_own_delivery_exactly_once() {
            // Given
            ReadsAnEmptyInstanceOnce racing = new ReadsAnEmptyInstanceOnce(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(racing, SagaRunnerConfig.defaults());
            deliver(execution, placedAt(1));
            racing.nextFindMissesTheInstance();

            // When
            deliver(execution, itemAt(2, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty(),
                    () -> assertThat(store.find(ORDER).orElseThrow().heldEventsChecked()).isTrue());
        }

        @Test
        void an_event_that_finds_the_instance_completed_after_it_was_held_is_released_and_its_reaction_does_not_run() {
            // Given
            ReadsAnEmptyInstanceOnce racing = new ReadsAnEmptyInstanceOnce(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(racing, SagaRunnerConfig.defaults());
            deliver(execution, placedAt(1));
            deliver(execution, paymentAt(PAYMENT_STREAM, 1));
            racing.nextFindMissesTheInstance();

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAt(2, "late")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Ship(ORDER)),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when a held copy is left behind the reorder window")
    class WhenAHeldCopyIsLeftBehindTheReorderWindow {

        @Test
        void the_next_delivery_of_the_event_applies_its_held_copy_and_releases_it() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionWithACopyLeftBehind();

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "two")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void a_second_delivery_of_the_event_does_not_apply_it_again() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionWithACopyLeftBehind();
            deliver(execution, itemAt(2, "two"));
            issued.clear();

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        private SagaExecution<OrderEvent, Order, OrderCommand> executionWithACopyLeftBehind() {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withReorderWindow(1));
            deliver(execution, placedAt(1));
            deliver(execution, itemAt(3, "three"));
            deliver(execution, itemAt(4, "four"));
            deliver(execution, itemAt(5, "five"));
            holdALateCopyOf(itemAt(2, "two"));
            deliver(execution, itemAt(6, "six"));
            assertThat(heldKeys()).as("the drain leaves the copy held").containsExactly(ORDER_STREAM + "@2");
            issued.clear();
            return execution;
        }

        // What a hold that lands after the start's drain leaves behind, with the drain marked as due the way the hold marks it.
        private void holdALateCopyOf(CloudEvent event) {
            HeldEventRef ref = new HeldEventRef(ORDER_STREAM + "@2", ORDER_STREAM, 2L, null, event.getType(), now.get(), false, false);
            store.hold(ORDER, new HeldEvent(ORDER, ref, event));
            store.markHeldEvents(ORDER, null);
        }
    }

    @Nested
    @DisplayName("when a held event fails while it is applied")
    class WhenAHeldEventFailsWhileItIsApplied {

        @Test
        void the_delivery_that_drove_the_drain_is_refused_with_the_failure_of_the_held_event_as_its_cause() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");

            // When
            Throwable refused = deliverRefused(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(refused).hasMessageContaining("could not apply the held event '" + ORDER_STREAM + "@2'"),
                    () -> assertThat(refused).hasCause(reservationFailure));
        }

        @Test
        void the_held_event_is_listed_pending_and_stays_held_while_quarantine_is_off() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");

            // When
            deliverRefused(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(pendingItemVersions()).containsExactly(2L),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@2"),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE));
        }

        @Test
        void the_start_is_not_run_again_and_the_held_event_is_applied_once_when_the_start_is_delivered_again_after_the_cause_is_fixed() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");
            deliverRefused(execution, placedAt(1));
            failingItems.clear();

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "bad")),
                    () -> assertThat(heldKeys()).isEmpty(),
                    () -> assertThat(pendingItemVersions()).isEmpty(),
                    () -> assertThat(store.find(ORDER).orElseThrow().heldEventsChecked()).isTrue());
        }

        @Test
        void the_failure_is_counted_on_the_clock_of_the_held_event_and_not_on_the_start_while_quarantine_is_on() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");
            Instant firstFailure = now.get();

            // When
            Throwable refused = deliverRefused(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(refused).hasCause(reservationFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().firstFailedAt()).isEqualTo(firstFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(pendingItemVersions()).containsExactly(2L));
        }

        @Test
        void a_held_event_whose_evolve_throws_while_the_drain_checks_whether_it_ends_the_instance_fails_as_itself_and_not_as_the_start() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
            deliver(execution, itemAt(2, "unevolvable"));
            deliver(execution, paymentAt(PAYMENT_STREAM, 1));
            unevolvableItems.add("unevolvable");
            Instant firstFailure = now.get();

            // When
            Throwable refused = deliverRefused(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(refused).hasMessageContaining("could not apply the held event '" + ORDER_STREAM + "@2'"),
                    () -> assertThat(refused).hasCause(evolveFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().firstFailedAt()).isEqualTo(firstFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(pendingItemVersions()).containsExactly(2L));
        }

        @Test
        void the_instance_is_quarantined_at_the_held_event_once_the_budget_of_that_event_has_elapsed() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");
            Instant firstFailure = now.get();
            deliverRefused(execution, placedAt(1));
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(placedAt(1)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().firstFailedAt()).isEqualTo(firstFailure),
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)));
        }
    }

    @Nested
    @DisplayName("when the delivery does not hold")
    class WhenTheDeliveryDoesNotHold {

        @Test
        void the_event_is_skipped_and_acknowledged_and_nothing_is_held() {
            // Given
            holds.set(false);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAt(2, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(store.find(ORDER)).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_event_is_not_applied_when_the_start_arrives_afterwards() {
            // Given
            holds.set(false);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "book"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty());
        }

        @Test
        void a_start_is_applied_as_it_always_was() {
            // Given
            holds.set(false);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();

            // When
            assertThatCode(() -> deliver(execution, placedAt(1))).doesNotThrowAnyException();

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER));
        }
    }

    /**
     * An in-memory store whose next read of the instance by {@code find} answers that there is none, as a read that
     * happened before another delivery wrote it would. Every other read, and every write, sees the instance.
     */
    private static final class ReadsAnEmptyInstanceOnce implements SagaStateStore<Order>, SagaHeldEvents<Order> {

        private final InMemorySagaStateStore<Order> delegate;
        private final AtomicBoolean missNextFind = new AtomicBoolean();

        ReadsAnEmptyInstanceOnce(InMemorySagaStateStore<Order> delegate) {
            this.delegate = delegate;
        }

        void nextFindMissesTheInstance() {
            missNextFind.set(true);
        }

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
            return missNextFind.compareAndSet(true, false) ? Optional.empty() : delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<Order>> findWithoutState(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<Order>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }

        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            return delegate.hold(sagaId, event);
        }

        @Override
        public boolean markOutstanding(String sagaId, String key) {
            return delegate.markOutstanding(sagaId, key);
        }

        @Override
        public Optional<SagaEnvelope<Order>> markHeldEvents(String sagaId, @Nullable String selfHeldKey) {
            return delegate.markHeldEvents(sagaId, selfHeldKey);
        }

        @Override
        public void unmarkSelfHeld(String sagaId, String selfHeldKey) {
            delegate.unmarkSelfHeld(sagaId, selfHeldKey);
        }

        @Override
        public List<HeldEventRef> heldIndex(String sagaId) {
            return delegate.heldIndex(sagaId);
        }

        @Override
        public List<HeldEvent> held(String sagaId, Collection<String> keys) {
            return delegate.held(sagaId, keys);
        }

        @Override
        public void release(String sagaId, String key) {
            delegate.release(sagaId, key);
        }
    }
}
