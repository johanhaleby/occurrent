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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
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
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.filter.Filter;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;
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
    private static final String EXPIRY_TIMER = "expiry";
    private static final Duration EXPIRY = Duration.ofMinutes(10);
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();

    sealed interface OrderEvent permits Opening, ItemAdded, Intake, Booking, PlainBooking {
        String eventId();

        String orderId();
    }

    /** The events that can open an order, so that a saga can declare the one type for both. */
    sealed interface Opening extends OrderEvent permits OrderPlaced, PaymentReceived {
    }

    record OrderPlaced(String eventId, String orderId) implements Opening {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record PaymentReceived(String eventId, String orderId) implements Opening {
    }

    /**
     * The intakes that can open an order, as an abstract class rather than an interface. It is a class and not a record so
     * that a saga can start on it, and sealed because a saga cannot derive its event types from an open hierarchy.
     */
    abstract static sealed class Intake implements OrderEvent permits PhoneIntake, WebIntake {
        private final String eventId;
        private final String orderId;

        Intake(String eventId, String orderId) {
            this.eventId = eventId;
            this.orderId = orderId;
        }

        @Override
        @JsonProperty("eventId")
        public String eventId() {
            return eventId;
        }

        @Override
        @JsonProperty("orderId")
        public String orderId() {
            return orderId;
        }
    }

    static final class PhoneIntake extends Intake {
        @JsonCreator
        PhoneIntake(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            super(eventId, orderId);
        }
    }

    static final class WebIntake extends Intake {
        @JsonCreator
        WebIntake(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            super(eventId, orderId);
        }
    }

    /** A concrete class that has a subclass, so that a saga can start on a class that other events on the stream extend. Sealed for the same reason as {@link Intake}. */
    static sealed class Booking implements OrderEvent permits Rebooking {
        private final String eventId;
        private final String orderId;

        @JsonCreator
        Booking(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            this.eventId = eventId;
            this.orderId = orderId;
        }

        @Override
        @JsonProperty("eventId")
        public String eventId() {
            return eventId;
        }

        @Override
        @JsonProperty("orderId")
        public String orderId() {
            return orderId;
        }
    }

    static final class Rebooking extends Booking {
        @JsonCreator
        Rebooking(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            super(eventId, orderId);
        }
    }

    /**
     * A concrete class that has a subclass and is neither sealed nor final, so a saga can start on it only under a
     * {@code replacementFilter}, because the subclasses it has cannot be enumerated.
     */
    static non-sealed class PlainBooking implements OrderEvent {
        private final String eventId;
        private final String orderId;

        @JsonCreator
        PlainBooking(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            this.eventId = eventId;
            this.orderId = orderId;
        }

        @Override
        @JsonProperty("eventId")
        public String eventId() {
            return eventId;
        }

        @Override
        @JsonProperty("orderId")
        public String orderId() {
            return orderId;
        }
    }

    static final class PlainRebooking extends PlainBooking {
        @JsonCreator
        PlainRebooking(@JsonProperty("eventId") String eventId, @JsonProperty("orderId") String orderId) {
            super(eventId, orderId);
        }
    }

    sealed interface OrderCommand permits OpenLedger, Reserve, Ship, Expire {
    }

    record OpenLedger(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    record Ship(String orderId) implements OrderCommand {
    }

    record Expire(String orderId) implements OrderCommand {
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
                .react(OrderPlaced.class, (state, e) -> startsAnExpiryTimer
                        ? List.of(SagaEffect.issue(new OpenLedger(e.orderId())), SagaEffect.startTimeout(EXPIRY_TIMER, EXPIRY))
                        : List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolveOnTimeout(EXPIRY_TIMER, (state, timeout) -> state)
                .reactOnTimeout(EXPIRY_TIMER, (state, timeout) -> List.of(SagaEffect.issue(new Expire(timeout.sagaId()))))
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
    private volatile boolean startsAnExpiryTimer;
    private final List<OrderCommand> issued = new CopyOnWriteArrayList<>();
    private final AtomicReference<Instant> now = new AtomicReference<>(NOW);
    private boolean holds = true;
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
        return execution(stateStore, config, event -> true);
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> execution(SagaStateStore<Order> stateStore, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable) {
        return new SagaExecution<>("orders", orderSaga(), stateStore, this::dispatch, converter, config, stillObtainable, now::get, holds);
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> executionOf(Saga<OrderEvent, Order, OrderCommand> saga) {
        return executionOf(saga, store);
    }

    private <S> SagaExecution<OrderEvent, S, OrderCommand> executionOf(Saga<OrderEvent, S, OrderCommand> saga, SagaStateStore<S> stateStore) {
        return executionOf(saga, stateStore, SagaRunnerConfig.defaults());
    }

    private <S> SagaExecution<OrderEvent, S, OrderCommand> executionOf(Saga<OrderEvent, S, OrderCommand> saga, SagaStateStore<S> stateStore, SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga, stateStore, this::dispatch, converter, config, event -> true, now::get, holds);
    }

    /** The reaction of every event names it, so that an event whose reaction ran can be told from one whose reaction did not, whatever its type. */
    private Saga<OrderEvent, Order, OrderCommand> sagaWhoseReactionsNameTheEvent() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(state == null ? List.of() : state.items(), false))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .build();
    }

    /** A saga whose payment starts an instance as well as the placement does, so that either can come first. */
    private Saga<OrderEvent, Order, OrderCommand> sagaStartingOnPlacedAndPayment() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .startsOn(PaymentReceived.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(List.of(), false))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(PaymentReceived.class, (state, e) -> new Order(state == null ? List.of() : state.items(), true))
                .react(PaymentReceived.class, (state, e) -> List.of(SagaEffect.issue(new Ship(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .build();
    }

    private void dispatch(OrderCommand command) {
        if (command instanceof Reserve reserve && failingItems.contains(reserve.item())) {
            throw reservationFailure;
        }
        issued.add(command);
    }

    /** Delivers the event and moves the clock on a second, so that two events are never held at the same instant. */
    private void deliver(SagaExecution<OrderEvent, ?, OrderCommand> execution, CloudEvent event) {
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

    private CloudEvent phoneIntakeAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new PhoneIntake("phone-" + streamVersion, ORDER));
    }

    private CloudEvent webIntakeAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new WebIntake("web-" + streamVersion, ORDER));
    }

    private CloudEvent bookingAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new Booking("booking-" + streamVersion, ORDER));
    }

    private CloudEvent rebookingAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new Rebooking("rebooking-" + streamVersion, ORDER));
    }

    private CloudEvent plainBookingAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new PlainBooking("plain-booking-" + streamVersion, ORDER));
    }

    private CloudEvent plainRebookingAt(long streamVersion) {
        return streamKeyed(ORDER_STREAM, streamVersion, new PlainRebooking("plain-rebooking-" + streamVersion, ORDER));
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

    private List<Long> pendingVersionsOf(Class<?> type) {
        StreamMarks marks = store.find(ORDER).map(instance -> instance.streamMarks().get(ORDER_STREAM)).orElse(null);
        return marks == null ? List.of() : marks.pending().getOrDefault(type.getName(), List.of());
    }

    private Set<Long> versionsWithAPendingInstant() {
        StreamMarks marks = store.find(ORDER).map(instance -> instance.streamMarks().get(ORDER_STREAM)).orElse(null);
        return marks == null ? Set.of() : marks.pendingSince().keySet();
    }

    private @Nullable String failureInput() {
        SagaFailure failure = store.find(ORDER).orElseThrow().failure();
        return failure == null ? null : failure.input();
    }

    private @Nullable Instant failingSinceOfTheCopy() {
        return store.heldIndex(ORDER).stream().filter(ref -> ref.key().equals(ORDER_STREAM + "@2")).findFirst().orElseThrow().failingSince();
    }

    private SagaStatus statusOfTheInstance() {
        return store.find(ORDER).orElseThrow().status();
    }

    private List<OrderCommand> issuedReservationsOf(String item) {
        return issued.stream().filter(command -> command.equals(new Reserve(ORDER, item))).toList();
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
        void an_earlier_event_of_the_stream_that_started_the_instance_waits_held_because_it_is_not_a_start_event() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(3, "book"));

            // When
            deliver(execution, placedAt(5));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@3"));
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
            ScriptedStore racing = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(racing, SagaRunnerConfig.defaults());
            deliver(execution, placedAt(1));
            racing.nextFindMissesTheInstance();

            // When
            deliver(execution, itemAt(2, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void an_event_that_finds_the_instance_completed_after_it_was_held_is_released_and_its_reaction_does_not_run() {
            // Given
            ScriptedStore racing = new ScriptedStore(store);
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
    @DisplayName("when other deliveries start the instance and move it past the event's reorder window before the hold")
    class WhenOtherDeliveriesMoveTheInstancePastTheEventBeforeTheHold {

        private ScriptedStore racing;
        private SagaExecution<OrderEvent, Order, OrderCommand> execution;

        @BeforeEach
        void readAnAbsentInstanceThenLetOtherDeliveriesStartAndAdvanceIt() {
            racing = new ScriptedStore(store);
            execution = execution(racing, SagaRunnerConfig.defaults().withReorderWindow(1));
            racing.nextFindMissesTheInstance();
            racing.beforeNextHold(() -> {
                deliver(execution, placedAt(1));
                deliver(execution, itemAt(3, "three"));
                deliver(execution, itemAt(4, "four"));
            });
        }

        @Test
        void the_reaction_of_the_event_runs_exactly_once_in_its_own_delivery() {
            // When
            Throwable thrown = catchThrowable(() -> deliver(execution, itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).containsExactlyInAnyOrder(new OpenLedger(ORDER), new Reserve(ORDER, "three"), new Reserve(ORDER, "four"), new Reserve(ORDER, "two")));
        }

        @Test
        void the_held_copy_of_the_event_is_released() {
            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertThat(heldKeys()).isEmpty();
        }

        @Test
        void a_second_delivery_of_the_event_does_not_apply_it_again() {
            // Given
            deliver(execution, itemAt(2, "two"));
            issued.clear();

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertThat(issued).isEmpty();
        }
    }

    @Nested
    @DisplayName("when the start of the event's own stream is delivered between the read of an absent instance and the hold of an event that committed before it")
    class WhenTheStartOfTheEventsOwnStreamIsDeliveredBeforeTheHold {

        private ScriptedStore racing;
        private SagaExecution<OrderEvent, Order, OrderCommand> execution;

        @BeforeEach
        void readAnAbsentInstanceThenLetTheStartBeDelivered() {
            racing = new ScriptedStore(store);
            execution = execution(racing, SagaRunnerConfig.defaults());
            racing.nextFindMissesTheInstance();
            racing.beforeNextHold(() -> deliver(execution, placedAt(5)));
        }

        @Test
        void the_event_waits_held_and_is_not_applied_because_it_is_not_a_start_event() {
            // When
            Throwable thrown = catchThrowable(() -> deliver(execution, itemAt(3, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@3"));
        }
    }

    @Nested
    @DisplayName("when an event without a stream key finds its instance started after it was held")
    class WhenAnEventWithoutAStreamKeyFindsTheInstanceStartedAfterItWasHeld {

        @Test
        void it_is_applied_exactly_once_because_its_held_copy_stands_for_it() {
            // Given
            ScriptedStore racing = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(racing, SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
            racing.nextFindMissesTheInstance();
            racing.beforeNextHold(() -> deliver(execution, placedAt(1)));

            // When
            deliver(execution, unkeyedItem("book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "book")),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when two events held for one instance could be taken for one event")
    class WhenTwoHeldEventsCouldBeTakenForOneEvent {

        private SagaExecution<OrderEvent, Order, OrderCommand> bestEffortExecution() {
            return execution(store, SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
        }

        private CloudEvent unkeyedItemFrom(String eventId, String source, String item) {
            return CloudEventBuilder.v1(converter.toCloudEvent(new ItemAdded(eventId, ORDER, item))).withSource(URI.create(source)).build();
        }

        @Test
        void a_keyed_event_and_an_event_without_a_stream_key_whose_id_and_source_spell_the_same_text_are_both_applied_when_the_start_arrives() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = bestEffortExecution();
            deliver(execution, itemAt("e from urn:test", 2, "keyed"));
            deliver(execution, unkeyedItemFrom("e", "urn:test@2", "unkeyed"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactlyInAnyOrder(new OpenLedger(ORDER), new Reserve(ORDER, "keyed"), new Reserve(ORDER, "unkeyed")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_same_event_without_a_stream_key_delivered_twice_is_still_held_once_and_applied_once() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = bestEffortExecution();
            deliver(execution, unkeyedItemFrom("e", "urn:test@2", "unkeyed"));
            deliver(execution, unkeyedItemFrom("e", "urn:test@2", "unkeyed"));

            // When
            deliver(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, "unkeyed")),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when a held copy is left behind the reorder window")
    class WhenAHeldCopyIsLeftBehindTheReorderWindow {

        @Test
        void the_next_delivery_of_any_event_applies_the_held_copy_before_itself() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionWithACopyLeftBehind();

            // When
            deliver(execution, itemAt(6, "six"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "two"), new Reserve(ORDER, "six")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

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

        @Test
        void a_later_delivery_of_the_event_does_not_apply_it_twice_and_the_copy_ends_up_released_when_the_release_after_its_own_save_failed() {
            // Given
            ScriptedStore failing = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionWithACopyLeftBehind(failing);
            failing.failNextRelease(new IllegalStateException("cannot release"));
            catchThrowable(() -> deliver(execution, itemAt(2, "two")));

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "two")),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        private SagaExecution<OrderEvent, Order, OrderCommand> executionWithACopyLeftBehind() {
            return executionWithACopyLeftBehind(store);
        }

        private SagaExecution<OrderEvent, Order, OrderCommand> executionWithACopyLeftBehind(SagaStateStore<Order> stateStore) {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(stateStore, SagaRunnerConfig.defaults().withReorderWindow(1));
            deliver(execution, placedAt(1));
            deliver(execution, itemAt(3, "three"));
            deliver(execution, itemAt(4, "four"));
            deliver(execution, itemAt(5, "five"));
            holdALateCopyOf(itemAt(2, "two"));
            assertThat(heldKeys()).as("the copy is held behind the window").containsExactly(ORDER_STREAM + "@2");
            issued.clear();
            return execution;
        }
    }

    // A hold that arrives after the instance moved past the event.
    private void holdALateCopyOf(CloudEvent event) {
        HeldEventRef ref = new HeldEventRef(ORDER_STREAM + "@2", ORDER_STREAM, 2L, null, event.getType(), now.get(), false, null);
        store.hold(ORDER, new HeldEvent(ORDER, ref, event));
    }

    // A copy left by a hold, placed straight in the store.
    private void holdWithoutMarkingADrain(long version, CloudEvent event) {
        HeldEventRef ref = new HeldEventRef(ORDER_STREAM + "@" + version, ORDER_STREAM, version, null, event.getType(), now.get(), false, null);
        store.hold(ORDER, new HeldEvent(ORDER, ref, event));
    }

    // Rewrites the stored instance to list versions of the start's stream as pending, the way an earlier failure would have left them.
    @SuppressWarnings("deprecation")
    private void listOnTheInstance(String pendingType, List<Long> pendingVersions, @Nullable SagaFailure failure) {
        SagaEnvelope<Order> stored = store.find(ORDER).orElseThrow();
        StreamMarks marks = stored.streamMarks().get(ORDER_STREAM);
        Map<Long, Instant> pendingSince = new HashMap<>();
        pendingVersions.forEach(version -> pendingSince.put(version, NOW));
        StreamMarks listed = new StreamMarks(marks.floor(), marks.byType(), pendingVersions.isEmpty() ? Map.of() : Map.of(pendingType, pendingVersions), marks.legacyFailed(), pendingSince);
        Map<String, StreamMarks> streamMarks = new HashMap<>(stored.streamMarks());
        streamMarks.put(ORDER_STREAM, listed);
        SagaEnvelope<Order> rewritten = new SagaEnvelope<>(stored.sagaId(), stored.state(), stored.status(), stored.version() + 1, stored.timers(), stored.streamWatermarks(),
                stored.positionWatermark(), stored.createdAt(), stored.updatedAt(), stored.completedAt(), stored.currentStep(), stored.started(), failure, streamMarks,
                stored.startMark());
        assertThat(store.compareAndSave(ORDER, rewritten, stored.version())).as("the instance is rewritten").isTrue();
    }

    // Rewrites the stored instance so that the floor of the start's stream is raised, the way an instance stored before 0.35.0 counts versions as handled.
    @SuppressWarnings("deprecation")
    private void raiseTheFloorTo(long floor) {
        SagaEnvelope<Order> stored = store.find(ORDER).orElseThrow();
        StreamMarks marks = stored.streamMarks().get(ORDER_STREAM);
        StreamMarks raised = new StreamMarks(floor, marks.byType(), marks.pending(), marks.legacyFailed(), marks.pendingSince());
        Map<String, StreamMarks> streamMarks = new HashMap<>(stored.streamMarks());
        streamMarks.put(ORDER_STREAM, raised);
        SagaEnvelope<Order> rewritten = new SagaEnvelope<>(stored.sagaId(), stored.state(), stored.status(), stored.version() + 1, stored.timers(), stored.streamWatermarks(),
                stored.positionWatermark(), stored.createdAt(), stored.updatedAt(), stored.completedAt(), stored.currentStep(), stored.started(), stored.failure(), streamMarks,
                stored.startMark());
        assertThat(store.compareAndSave(ORDER, rewritten, stored.version())).as("the floor is raised").isTrue();
    }

    @Nested
    @DisplayName("when a held copy is left behind the reorder window and the saga no longer holds")
    class WhenAHeldCopyIsLeftBehindTheReorderWindowAndTheSagaNoLongerHolds {

        @Test
        void the_next_delivery_of_the_event_applies_its_held_copy_and_releases_it() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionThatNoLongerHoldsWithACopyLeftBehind();

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "two")),
                    () -> assertThat(stateOfTheInstance().items()).contains("two"),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void a_second_delivery_of_the_event_does_not_apply_it_again() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionThatNoLongerHoldsWithACopyLeftBehind();
            deliver(execution, itemAt(2, "two"));
            issued.clear();

            // When
            deliver(execution, itemAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        // The copy was held while the saga ran on a model that reorders, and the saga now runs on one that does not.
        private SagaExecution<OrderEvent, Order, OrderCommand> executionThatNoLongerHoldsWithACopyLeftBehind() {
            SagaRunnerConfig config = SagaRunnerConfig.defaults().withReorderWindow(1);
            SagaExecution<OrderEvent, Order, OrderCommand> whileItHeld = execution(store, config);
            deliver(whileItHeld, placedAt(1));
            deliver(whileItHeld, itemAt(3, "three"));
            deliver(whileItHeld, itemAt(4, "four"));
            deliver(whileItHeld, itemAt(5, "five"));
            holdALateCopyOf(itemAt(2, "two"));
            assertThat(heldKeys()).as("the copy is held behind the window").containsExactly(ORDER_STREAM + "@2");
            holds = false;
            issued.clear();
            return execution(store, config);
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
        void the_held_event_stays_held_and_is_not_listed_pending_while_quarantine_is_off() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");

            // When
            deliverRefused(execution, placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(pendingItemVersions()).isEmpty(),
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
                    () -> assertThat(pendingItemVersions()).isEmpty());
        }

        @Test
        void the_failure_is_counted_on_the_clock_of_the_held_copy_and_no_record_is_written_while_quarantine_is_on() {
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
                    () -> assertThat(failingSinceOfTheCopy()).isEqualTo(firstFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(pendingItemVersions()).isEmpty());
        }

        @Test
        void a_held_event_whose_evolve_throws_while_the_sweep_checks_whether_it_ends_the_instance_fails_as_itself_and_not_as_the_start() {
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
                    () -> assertThat(failingSinceOfTheCopy()).isEqualTo(firstFailure),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(pendingItemVersions()).isEmpty());
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
    @DisplayName("when a held event fails in the drain a timer's attempt runs")
    class WhenAHeldEventFailsInTheDrainATimersAttemptRuns {

        /** A started instance with its expiry timer due and a held copy of version 2 that a hold left behind. */
        private SagaExecution<OrderEvent, Order, OrderCommand> startedWithADueTimerAndAHeldCopyThatFails(SagaRunnerConfig config) {
            startsAnExpiryTimer = true;
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, config);
            deliver(execution, placedAt(1));
            CloudEvent late = itemAt(2, "bad");
            store.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(ORDER_STREAM + "@2", ORDER_STREAM, 2L, null, late.getType(), now.get(), false, null), late));
            failingItems.add("bad");
            now.updateAndGet(instant -> instant.plus(EXPIRY).plusSeconds(1));
            return execution;
        }

        private List<String> timerNames() {
            return store.find(ORDER).orElseThrow().timers().stream().map(TimerEntry::name).toList();
        }

        @Test
        void the_held_event_stays_held_and_is_not_listed_pending_and_the_timer_stays_due_while_quarantine_is_off() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopyThatFails(SagaRunnerConfig.defaults());

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(pendingItemVersions()).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@2"),
                    () -> assertThat(timerNames()).containsExactly(EXPIRY_TIMER),
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE));
        }

        @Test
        void the_instance_is_quarantined_at_the_held_event_once_the_budget_of_that_event_has_elapsed() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopyThatFails(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
            execution.pollTimers();
            Instant firstFailure = now.get();
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().firstFailedAt()).isEqualTo(firstFailure),
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER)));
        }
    }

    @Nested
    @DisplayName("when the failure of a held event is past its budget")
    class WhenTheFailureOfAHeldEventIsPastItsBudget {

        private final Predicate<CloudEvent> onlyTheDrivingDeliveryIsObtainable = event -> event.getId().equals("placed-1");
        private final Predicate<CloudEvent> onlyTheHeldEventIsObtainable = event -> event.getId().equals("item-bad");

        /** The start drives a drain that fails on version 2, and its delivery is offered again after the budget of version 2 has elapsed. */
        private Throwable theStartIsOfferedAgainAfterTheBudget(Predicate<CloudEvent> stillObtainable) {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET), stillObtainable);
            deliver(execution, itemAt(2, "bad"));
            failingItems.add("bad");
            deliverRefused(execution, placedAt(1));
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));
            return catchThrowable(() -> execution.onCloudEvent(placedAt(1)));
        }

        @Test
        void the_instance_is_quarantined_when_the_driving_delivery_is_the_one_that_is_still_obtainable() {
            // When
            Throwable thrown = theStartIsOfferedAgainAfterTheBudget(onlyTheDrivingDeliveryIsObtainable);

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"));
        }

        @Test
        void the_instance_is_quarantined_without_asking_whether_the_driving_delivery_is_obtainable_when_only_the_held_event_is() {
            // When
            Throwable thrown = theStartIsOfferedAgainAfterTheBudget(onlyTheHeldEventIsObtainable);

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure().input()).isEqualTo(ORDER_STREAM + "@2"));
        }
    }

    @Nested
    @DisplayName("when the saga does not hold")
    class WhenTheSagaDoesNotHold {

        @Test
        void the_event_is_skipped_and_acknowledged_and_nothing_is_held() {
            // Given
            holds = false;
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
            holds = false;
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
            holds = false;
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();

            // When
            assertThatCode(() -> deliver(execution, placedAt(1))).doesNotThrowAnyException();

            // Then
            assertThat(issued).containsExactly(new OpenLedger(ORDER));
        }
    }

    @Nested
    @DisplayName("when an event of the stream that started the instance has a version below the start")
    class WhenAnEventOfTheStreamThatStartedTheInstanceHasAVersionBelowTheStart {

        private ListAppender<ILoggingEvent> appender;
        private Logger supportLog;
        private Logger executionLog;
        private @Nullable Level originalSupportLevel;
        private @Nullable Level originalExecutionLevel;

        @BeforeEach
        void captureTheLogOfTheDecisionAndOfTheExecution() {
            appender = new ListAppender<>();
            appender.start();
            supportLog = (Logger) LoggerFactory.getLogger(SagaExecutionSupport.class);
            executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            // Restored in teardown, because the level is process-global and would otherwise leak into the next test.
            originalSupportLevel = supportLog.getLevel();
            originalExecutionLevel = executionLog.getLevel();
            supportLog.setLevel(Level.DEBUG);
            executionLog.setLevel(Level.WARN);
            supportLog.addAppender(appender);
            executionLog.addAppender(appender);
        }

        @AfterEach
        void restoreTheLogs() {
            supportLog.detachAppender(appender);
            executionLog.detachAppender(appender);
            supportLog.setLevel(originalSupportLevel);
            executionLog.setLevel(originalExecutionLevel);
            appender.stop();
        }

        @Test
        void the_reaction_of_an_event_of_another_type_does_not_run_and_the_state_is_left_alone_while_the_event_waits_held() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();
            Order before = stateOfTheInstance();

            // When
            Throwable thrown = catchThrowable(() -> deliver(execution, itemAt(3, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stateOfTheInstance()).isEqualTo(before),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@3"));
        }

        @Test
        void the_reaction_of_an_event_of_another_type_does_not_run_however_many_times_it_is_delivered() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();

            // When
            deliver(execution, itemAt(3, "book"));
            deliver(execution, itemAt(3, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@3"));
        }

        @Test
        void the_reaction_of_an_event_of_a_start_type_runs_because_the_instance_may_have_started_on_the_later_of_two_start_events() {
            // Given a saga with two start types, whose stream holds the payment at 3 and the placement at 5, delivered placement first
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionOf(sagaStartingOnPlacedAndPayment());
            deliver(execution, placedAt(5));
            issued.clear();

            // When
            deliver(execution, paymentAt(ORDER_STREAM, 3));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Ship(ORDER)),
                    () -> assertThat(stateOfTheInstance().paid()).isTrue());
        }

        @Test
        void the_reaction_of_an_event_of_another_type_above_the_start_runs() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();

            // When
            deliver(execution, itemAt(6, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"));
        }

        @Test
        void the_reaction_of_an_event_of_another_stream_with_a_version_below_the_start_runs() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();

            // When
            deliver(execution, itemAt(PAYMENT_STREAM, 3, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "book")),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("book"));
        }

        @Test
        void the_held_copy_of_the_event_that_the_lookup_finds_waits_and_no_reaction_runs() {
            // Given a copy held for the event after the start, so that only the lookup of this delivery can find it
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, itemAt(7, "seven"));
            deliver(execution, placedAt(5));
            issued.clear();
            holdACopyOf(3, itemAt(3, "book"));
            assertThat(heldKeys()).as("the copy is held").containsExactly(ORDER_STREAM + "@3");

            // When
            deliver(execution, itemAt(3, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stateOfTheInstance().items()).containsExactly("seven"),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@3"));
        }

        @Test
        void the_skip_on_a_feed_in_commit_order_is_logged_at_debug_level_with_the_key_of_the_event() {
            // Given
            holds = false;
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));

            // When
            deliver(execution, itemAt(3, "book"));

            // Then
            assertThat(appender.list).filteredOn(logged -> logged.getLevel() == Level.DEBUG)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .anyMatch(message -> message.contains(ORDER_STREAM + "@3"));
        }

        @Test
        void the_event_is_skipped_and_not_held_on_a_feed_in_commit_order() {
            // Given
            holds = false;
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();

            // When
            Throwable thrown = catchThrowable(() -> deliver(execution, itemAt(3, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void no_warning_says_that_the_event_is_behind_the_reorder_window_when_it_is() {
            // Given a window of 1, and the item type applied up to version 8 of the stream
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, SagaRunnerConfig.defaults().withReorderWindow(1));
            deliver(execution, placedAt(5));
            deliver(execution, itemAt(6, "six"));
            deliver(execution, itemAt(7, "seven"));
            deliver(execution, itemAt(8, "eight"));
            issued.clear();

            // When
            deliver(execution, itemAt(3, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(appender.list).filteredOn(logged -> logged.getLevel() == Level.WARN).isEmpty());
        }

        // A copy left by a hold, placed straight in the store so that nothing but the lookup of a delivery of the event finds it.
        private void holdACopyOf(long version, CloudEvent event) {
            HeldEventRef ref = new HeldEventRef(ORDER_STREAM + "@" + version, ORDER_STREAM, version, null, event.getType(), now.get(), false, null);
            store.hold(ORDER, new HeldEvent(ORDER, ref, event));
        }
    }

    @Nested
    @DisplayName("when the state of the instance cannot be read and the input is of the stream that started it, below the start")
    class WhenTheStateOfTheInstanceCannotBeReadAndTheInputIsBelowTheStart {

        private final IllegalStateException cannotDecode = new IllegalStateException("the state can no longer be decoded");

        @Test
        void the_delivery_of_an_event_of_another_type_is_refused_with_the_load_failure_on_a_saga_with_one_start_type() {
            // Given an instance started at version 5 whose state can no longer be read, although the instance can
            ScriptedStore unreadable = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(unreadable, SagaRunnerConfig.defaults());
            deliver(execution, placedAt(5));
            issued.clear();
            long versionBefore = store.find(ORDER).orElseThrow().version();
            unreadable.cannotDecodeTheState(cannotDecode);

            // When
            Throwable thrown = deliverRefused(execution, itemAt(3, "book"));

            // Then the version is refused and listed as pending, so that a replay is still applied
            assertAll(
                    () -> assertThat(thrown).isSameAs(cannotDecode),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(pendingItemVersions()).containsExactly(3L),
                    () -> assertThat(store.find(ORDER).orElseThrow().version()).isEqualTo(versionBefore + 1));
        }

        @Test
        void the_delivery_of_an_event_of_another_type_is_refused_with_the_load_failure_on_a_saga_with_two_start_types() {
            // Given
            ScriptedStore unreadable = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = new SagaExecution<>("orders", sagaStartingOnPlacedAndPayment(), unreadable, this::dispatchTo,
                    converter, SagaRunnerConfig.defaults(), event -> true, now::get, holds);
            deliver(execution, placedAt(5));
            issued.clear();
            unreadable.cannotDecodeTheState(cannotDecode);

            // When
            Throwable thrown = deliverRefused(execution, itemAt(3, "book"));

            // Then the version is refused and listed as pending, so that a replay is still applied
            assertAll(
                    () -> assertThat(thrown).isSameAs(cannotDecode),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).isNull(),
                    () -> assertThat(pendingItemVersions()).containsExactly(3L));
        }

        private void dispatchTo(OrderCommand command) {
            dispatch(command);
        }
    }

    /**
     * A copy that the lookup of its own delivery applies as a new event is applied once, even when the release after the
     * save fails and the event is replayed after later events have moved it behind the reorder window.
     */
    @Nested
    @DisplayName("when the lookup applies the held copy of a new event and the release after the save fails")
    class WhenTheLookupAppliesTheHeldCopyOfANewEventAndTheReleaseFails {

        @Test
        void the_reaction_runs_once_and_nothing_is_left_when_the_event_is_replayed_behind_the_reorder_window() {
            // Given
            ScriptedStore failing = new ScriptedStore(store);
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(failing, SagaRunnerConfig.defaults().withReorderWindow(5));
            deliver(execution, placedAt(1));
            deliver(execution, itemAt(2, "two"));
            holdWithoutMarkingADrain(9, itemAt(9, "nine"));
            failing.failNextRelease(new IllegalStateException("cannot release"));
            Throwable refused = catchThrowable(() -> execution.onCloudEvent(itemAt(9, "nine")));
            assertThat(refused).as("the release fails after the save").hasMessage("cannot release");
            assertThat(issuedReservationsOf("nine")).as("the first delivery applied the copy").hasSize(1);
            for (long version = 11; version <= 16; version++) {
                deliver(execution, itemAt(version, "later-" + version));
            }

            // When
            deliver(execution, itemAt(9, "nine"));

            // Then
            assertAll(
                    () -> assertThat(issuedReservationsOf("nine")).hasSize(1),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    /**
     * The held copy that the lookup of its own delivery applies, because the event is behind the reorder window or below
     * the floor of an instance stored before 0.35.0, and that fails in the saga. It is the input that failed, so it is
     * kept under its own key and measured on its own clock, which is what quarantines the instance.
     */
    @Nested
    @DisplayName("when the held copy that the lookup applies fails and the event is behind the reorder window or below the floor")
    class WhenTheHeldCopyTheLookupAppliesFailsBehindTheReorderWindowOrBelowTheFloor {

        private static final String KEY = ORDER_STREAM + "@2";

        private SagaExecution<OrderEvent, Order, OrderCommand> executionWithACopyBehindTheWindow(SagaRunnerConfig config) {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, config.withReorderWindow(1));
            deliver(execution, placedAt(1));
            deliver(execution, itemAt(3, "three"));
            deliver(execution, itemAt(4, "four"));
            deliver(execution, itemAt(5, "five"));
            holdALateCopyOf(itemAt(2, "bad"));
            assertThat(heldKeys()).as("the copy is held behind the window").containsExactly(KEY);
            issued.clear();
            failingItems.add("bad");
            return execution;
        }

        private SagaExecution<OrderEvent, Order, OrderCommand> executionWithACopyBelowTheFloor(SagaRunnerConfig config) {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, config);
            deliver(execution, placedAt(1));
            deliver(execution, itemAt(7, "seven"));
            raiseTheFloorTo(5);
            holdWithoutMarkingADrain(2, itemAt(2, "bad"));
            issued.clear();
            failingItems.add("bad");
            return execution;
        }

        private void quarantinesAtTheKeyOfTheCopyOncePastTheBudget(SagaExecution<OrderEvent, Order, OrderCommand> execution) {
            // When
            Throwable first = deliverRefused(execution, itemAt(2, "bad"));
            Instant theCopyStartedFailingAt = failingSinceOfTheCopy();
            Instant firstFailure = now.get().minusSeconds(1);
            SagaStatus statusAfterTheFirstFailure = statusOfTheInstance();
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));
            Throwable second = catchThrowable(() -> execution.onCloudEvent(itemAt(2, "bad")));

            // Then
            assertAll(
                    () -> assertThat(first).hasCause(reservationFailure),
                    () -> assertThat(theCopyStartedFailingAt).isEqualTo(firstFailure),
                    () -> assertThat(statusAfterTheFirstFailure).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(second).isNull(),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(failureInput()).isEqualTo(KEY),
                    () -> assertThat(heldKeys()).containsExactly(KEY));
        }

        @Test
        void the_failure_record_names_the_copy_and_the_instance_is_quarantined_past_the_budget_when_the_event_is_behind_the_reorder_window() {
            quarantinesAtTheKeyOfTheCopyOncePastTheBudget(executionWithACopyBehindTheWindow(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)));
        }

        @Test
        void the_failure_record_names_the_copy_and_the_instance_is_quarantined_past_the_budget_when_the_event_is_below_the_floor_of_an_instance_stored_before_0_35_0() {
            quarantinesAtTheKeyOfTheCopyOncePastTheBudget(executionWithACopyBelowTheFloor(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)));
        }

        @Test
        void the_copy_stays_held_and_a_later_drain_applies_it_when_quarantine_is_off() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionWithACopyBehindTheWindow(SagaRunnerConfig.defaults());
            deliverRefused(execution, itemAt(2, "bad"));
            failingItems.clear();
            assertAll(
                    () -> assertThat(heldKeys()).as("the copy stays held").containsExactly(KEY),
                    () -> assertThat(failingSinceOfTheCopy()).as("a copy has no clock without a budget").isNull(),
                    () -> assertThat(failureInput()).as("nothing is recorded").isNull());

            // When
            deliver(execution, itemAt(7, "seven"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new Reserve(ORDER, "bad"), new Reserve(ORDER, "seven")),
                    () -> assertThat(heldKeys()).isEmpty());
        }
    }

    /**
     * Under best-effort detection a held copy with no stream key has no clock of its own, so its failure is measured on the
     * delivery that drove the drain when that delivery has a key, and a driver with no key is refused as it always was.
     */
    @Nested
    @DisplayName("when a held copy without a stream key fails in a drain and redelivery detection is best effort")
    class WhenAHeldCopyWithoutAStreamKeyFailsInADrain {

        private final SagaRunnerConfig bestEffort = SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

        private String keyOfTheFailingKeylessCopy() {
            CloudEvent copy = unkeyedItem("bad");
            String key = copy.getId() + " from " + copy.getSource() + " (no stream key)";
            store.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(key, null, null, null, copy.getType(), now.get(), false, null), copy));
            failingItems.add("bad");
            return key;
        }

        @Test
        void the_failure_record_names_the_keyless_copy_and_the_instance_is_quarantined_past_the_budget() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, bestEffort.withQuarantineAfter(BUDGET));
            deliver(execution, placedAt(1));
            String key = keyOfTheFailingKeylessCopy();

            // When
            Throwable first = deliverRefused(execution, itemAt(2, "two"));
            String afterTheFirstFailure = failureInput();
            SagaStatus statusAfterTheFirstFailure = statusOfTheInstance();
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));
            Throwable second = catchThrowable(() -> execution.onCloudEvent(itemAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(first).hasCause(reservationFailure),
                    () -> assertThat(afterTheFirstFailure).as("no record is written on an active instance for a copy").isNull(),
                    () -> assertThat(statusAfterTheFirstFailure).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(second).isNull(),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(failureInput()).isEqualTo(key));
        }

        @Test
        void a_driver_without_a_stream_key_is_refused_inside_the_budget_and_acknowledged_once_the_copy_is_past_it() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(store, bestEffort.withQuarantineAfter(BUDGET));
            deliver(execution, placedAt(1));
            String key = keyOfTheFailingKeylessCopy();

            // When
            Throwable refused = deliverRefused(execution, unkeyedItem("two"));
            now.updateAndGet(instant -> instant.plus(BUDGET).plusSeconds(1));
            Throwable acknowledged = catchThrowable(() -> execution.onCloudEvent(unkeyedItem("two")));

            // Then
            assertAll(
                    () -> assertThat(refused).hasCause(reservationFailure),
                    () -> assertThat(acknowledged).isNull(),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(failureInput()).isEqualTo(key),
                    () -> assertThat(pendingItemVersions()).isEmpty());
        }
    }

    /**
     * The attempt of a due timer drains first, and a held event it applies can complete the instance, cancel the timer or
     * arm it for later, so whether the timer is still due is asked of the instance the drain left.
     */
    @Nested
    @DisplayName("when the drain a timer's attempt runs applies a held event that changes the timer")
    class WhenTheDrainATimersAttemptRunsAppliesAHeldEventThatChangesTheTimer {

        private static final String CANCEL = "cancel";
        private static final String REARM = "rearm";
        private static final Duration LATER = Duration.ofHours(1);

        /** An item named cancel cancels the expiry timer and one named rearm arms it for an hour from the moment it is applied. */
        private Saga<OrderEvent, Order, OrderCommand> sagaWhoseItemsChangeTheExpiryTimer() {
            return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new Order(List.of(), false))
                    .react(OrderPlaced.class, (state, e) -> List.<SagaEffect<OrderCommand>>of(SagaEffect.issue(new OpenLedger(e.orderId())), SagaEffect.startTimeout(EXPIRY_TIMER, EXPIRY)))
                    .evolveOnTimeout(EXPIRY_TIMER, (state, timeout) -> state)
                    .reactOnTimeout(EXPIRY_TIMER, (state, timeout) -> List.of(SagaEffect.issue(new Expire(timeout.sagaId()))))
                    .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                    .react(ItemAdded.class, (state, e) -> switch (e.item()) {
                        case CANCEL -> List.<SagaEffect<OrderCommand>>of(SagaEffect.issue(new Reserve(e.orderId(), e.item())), SagaEffect.cancelTimeout(EXPIRY_TIMER));
                        case REARM -> List.<SagaEffect<OrderCommand>>of(SagaEffect.issue(new Reserve(e.orderId(), e.item())), SagaEffect.startTimeout(EXPIRY_TIMER, LATER));
                        default -> List.<SagaEffect<OrderCommand>>of(SagaEffect.issue(new Reserve(e.orderId(), e.item())));
                    })
                    .build();
        }

        /** A started instance with its expiry timer due and a held copy of version 2 that a hold left behind. */
        private SagaExecution<OrderEvent, Order, OrderCommand> startedWithADueTimerAndAHeldCopy(Saga<OrderEvent, Order, OrderCommand> saga, CloudEvent copy) {
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionOf(saga);
            deliver(execution, placedAt(1));
            holdALateCopyOf(copy);
            now.updateAndGet(instant -> instant.plus(EXPIRY).plusSeconds(1));
            return execution;
        }

        private List<TimerEntry> timers() {
            return store.find(ORDER).orElseThrow().timers();
        }

        @Test
        void the_timer_does_not_fire_when_the_held_event_completes_the_instance() {
            // Given
            startsAnExpiryTimer = true;
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopy(orderSaga(), paymentAt(ORDER_STREAM, 2));

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Ship(ORDER)),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_timer_does_not_fire_when_the_held_event_cancels_it() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopy(sagaWhoseItemsChangeTheExpiryTimer(), itemAt(2, CANCEL));

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, CANCEL)),
                    () -> assertThat(timers()).isEmpty(),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_timer_does_not_fire_and_stays_armed_for_later_when_the_held_event_arms_it_again() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopy(sagaWhoseItemsChangeTheExpiryTimer(), itemAt(2, REARM));
            long armedFor = now.get().plus(LATER).toEpochMilli();

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, REARM)),
                    () -> assertThat(timers()).extracting(TimerEntry::name, TimerEntry::firesAtEpochMilli).containsExactly(tuple(EXPIRY_TIMER, armedFor)),
                    () -> assertThat(heldKeys()).isEmpty());
        }

        @Test
        void the_timer_that_the_held_event_armed_again_fires_once_it_is_due() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = startedWithADueTimerAndAHeldCopy(sagaWhoseItemsChangeTheExpiryTimer(), itemAt(2, REARM));
            execution.pollTimers();
            now.updateAndGet(instant -> instant.plus(LATER).plusSeconds(1));

            // When
            execution.pollTimers();

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(ORDER), new Reserve(ORDER, REARM), new Expire(ORDER)),
                    () -> assertThat(timers()).isEmpty());
        }
    }

    /**
     * An event whose hold failed while quarantine was on leaves a failure record and a pending entry on an instance that
     * has not started. When the start above it arrives and the event is delivered again, the event is kept as a held copy
     * that waits, because it is below the start and is not a start event, and the record and the entry stay as they were.
     */
    @Nested
    @DisplayName("when the hold of an event failed with quarantine on and the instance then started above it")
    class WhenTheHoldOfAnEventFailedWithQuarantineOnAndTheInstanceThenStartedAboveIt {

        private ScriptedStore holdFails() {
            ScriptedStore failingHold = new ScriptedStore(store);
            failingHold.beforeNextHold(() -> {
                throw new IllegalStateException("cannot hold");
            });
            return failingHold;
        }

        @Test
        void a_redelivery_of_the_event_is_held_and_waits_and_the_record_and_the_pending_entry_are_left_as_they_were() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(holdFails(), SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
            deliverRefused(execution, itemAt(4, "book"));
            deliver(execution, placedAt(5));
            issued.clear();
            assertAll(
                    () -> assertThat(failureInput()).as("the record names the event").isEqualTo(ORDER_STREAM + "@4"),
                    () -> assertThat(pendingItemVersions()).as("the instance lists it pending").containsExactly(4L),
                    () -> assertThat(versionsWithAPendingInstant()).as("with the instant it first failed").containsExactly(4L));

            // When
            Throwable thrown = catchThrowable(() -> deliver(execution, itemAt(4, "book")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(stateOfTheInstance().items()).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@4"),
                    () -> assertThat(failureInput()).isEqualTo(ORDER_STREAM + "@4"),
                    () -> assertThat(pendingItemVersions()).containsExactly(4L),
                    () -> assertThat(statusOfTheInstance()).isEqualTo(SagaStatus.ACTIVE));
        }

        @Test
        void a_failure_record_that_names_another_input_stays_when_the_event_is_held() {
            // Given
            SagaExecution<OrderEvent, Order, OrderCommand> execution = execution();
            deliver(execution, placedAt(5));
            issued.clear();
            String another = ORDER_STREAM + "@3";
            listOnTheInstance(ITEM_ADDED_TYPE, List.of(4L), new SagaFailure(another, null, NOW, IllegalStateException.class.getName(), "cannot reserve"));

            // When
            deliver(execution, itemAt(4, "book"));

            // Then
            assertAll(
                    () -> assertThat(issued).isEmpty(),
                    () -> assertThat(heldKeys()).containsExactly(ORDER_STREAM + "@4"),
                    () -> assertThat(failureInput()).isEqualTo(another));
        }
    }

    /**
     * An in-memory store whose next read of the instance by {@code find} answers that there is none, as a read that
     * happened before another delivery wrote it would, and that can run something just before the next hold, as a
     * delivery that paused between its read and its hold would let other deliveries do. Every other read, and every
     * write, sees the instance. It can make the next release throw.
     */
    private static final class ScriptedStore implements SagaStateStore<Order>, SagaHeldEvents<Order> {

        private final InMemorySagaStateStore<Order> delegate;
        private final AtomicBoolean missNextFind = new AtomicBoolean();
        private final AtomicReference<@Nullable Runnable> beforeNextHold = new AtomicReference<>();
        private final AtomicReference<@Nullable RuntimeException> failureOnNextRelease = new AtomicReference<>();
        private final AtomicReference<@Nullable RuntimeException> failureOnFind = new AtomicReference<>();
        private volatile boolean losingEveryWriteWithoutState;

        ScriptedStore(InMemorySagaStateStore<Order> delegate) {
            this.delegate = delegate;
        }

        /** Makes the next release throw, once, and leave the copy held. */
        void failNextRelease(RuntimeException failure) {
            failureOnNextRelease.set(failure);
        }

        /** Makes every read of the instance with its state throw, as a store does whose converter no longer understands what it wrote. {@code findWithoutState} keeps answering. */
        void cannotDecodeTheState(RuntimeException failure) {
            failureOnFind.set(failure);
        }

        /** Makes every write without the state lose, as a failure record does when the instance keeps changing under it. */
        void loseEveryWriteWithoutState(boolean losing) {
            losingEveryWriteWithoutState = losing;
        }

        void nextFindMissesTheInstance() {
            missNextFind.set(true);
        }

        /** Runs once, just before the next hold, which then goes through. */
        void beforeNextHold(Runnable action) {
            beforeNextHold.set(action);
        }

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
            RuntimeException failure = failureOnFind.get();
            if (failure != null) {
                throw failure;
            }
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
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            return !losingEveryWriteWithoutState && compareAndSave(sagaId, envelope, expectedVersion);
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
            Runnable before = beforeNextHold.getAndSet(null);
            if (before != null) {
                before.run();
            }
            return delegate.hold(sagaId, event);
        }

        @Override
        public void failing(String sagaId, String key, Instant at) {
            delegate.failing(sagaId, key, at);
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
            RuntimeException failure = failureOnNextRelease.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
            delegate.release(sagaId, key);
        }
    }
}
