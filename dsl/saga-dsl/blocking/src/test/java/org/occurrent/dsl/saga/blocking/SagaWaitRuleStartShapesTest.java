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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Booking;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Intake;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.ItemAdded;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Opening;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Order;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderCommand;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderEvent;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderPlaced;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.PaymentReceived;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.PhoneIntake;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.PlainBooking;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.PlainRebooking;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Rebooking;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Reserve;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.WebIntake;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An event below the start's version on the start's own stream waits for a start event below it, whichever way the saga
 * declares its start events. The declaration is one type, several, an interface, an abstract class, a sealed concrete class
 * with a subclass on the stream, or a non-sealed one. Every declaration gets the same deliveries, with a higher and a
 * lower start event and an item between them that is not one.
 */
@DisplayName("The wait of an event below the start, across the shapes of a saga's start events")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaWaitRuleStartShapesTest {

    private static final String ORDER = "order-1";
    private static final String STREAM = "order-stream";
    private static final String ITEM = "book";
    private static final String FAILING = "T1";
    private static final String OTHER_STREAM = "other-stream";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final CloudEventConverter<OrderEvent> CONVERTER =
            new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();

    /** The reaction of every event names it, so a start event is told from the item and the higher from the lower. */
    private static Saga.Builder<OrderEvent, Order, OrderCommand> reactingTo(Saga.Builder<OrderEvent, Order, OrderCommand> builder, Class<? extends OrderEvent>... types) {
        Saga.Builder<OrderEvent, Order, OrderCommand> result = builder;
        for (Class<? extends OrderEvent> type : types) {
            result = nameTheEventOf(result, type);
        }
        return result.evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))));
    }

    private static <E extends OrderEvent> Saga.Builder<OrderEvent, Order, OrderCommand> nameTheEventOf(Saga.Builder<OrderEvent, Order, OrderCommand> builder, Class<E> type) {
        return builder
                .evolve(type, (state, e) -> new Order(state == null ? List.of() : state.items(), false))
                .react(type, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))));
    }

    private static Saga.Builder<OrderEvent, Order, OrderCommand> builder() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null).correlateAll(OrderEvent::orderId);
    }

    @SafeVarargs
    private static Saga.Builder<OrderEvent, Order, OrderCommand> startingOn(Class<? extends OrderEvent> start, Class<? extends OrderEvent>... evolved) {
        return reactingTo(builder().startsOn(start), evolved);
    }

    private static CloudEvent keyed(long version, OrderEvent event) {
        return CloudEventBuilder.v1(CONVERTER.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(STREAM, version)).build();
    }

    private static CloudEvent itemAt(long version) {
        return keyed(version, new ItemAdded("item-" + ITEM + version, ORDER, ITEM));
    }

    private static CloudEvent itemOn(String streamId, long version, String item) {
        CloudEvent plain = CONVERTER.toCloudEvent(new ItemAdded("item-" + item, ORDER, item));
        return CloudEventBuilder.v1(plain).withExtension(OccurrentCloudEventExtension.occurrent(streamId, version)).build();
    }

    /** A saga, and the higher and the lower start event it starts on. The reaction of a start event reserves its event id. */
    record Shape(String name, Supplier<Saga<OrderEvent, Order, OrderCommand>> saga, LongFunction<OrderEvent> higher, LongFunction<OrderEvent> lower) {
        @Override
        public String toString() {
            return name;
        }

        String higherId(long version) {
            return higher.apply(version).eventId();
        }

        String lowerId(long version) {
            return lower.apply(version).eventId();
        }
    }

    @SuppressWarnings("unchecked")
    static Stream<Shape> shapes() {
        return Stream.of(
                new Shape("one concrete start type", () -> startingOn(OrderPlaced.class, OrderPlaced.class).build(),
                        v -> new OrderPlaced("higher-" + v, ORDER), v -> new OrderPlaced("lower-" + v, ORDER)),
                new Shape("several start types", () -> reactingTo(builder().startsOn(OrderPlaced.class).startsOn(PaymentReceived.class), OrderPlaced.class, PaymentReceived.class).build(),
                        v -> new OrderPlaced("higher-" + v, ORDER), v -> new PaymentReceived("lower-" + v, ORDER)),
                new Shape("an interface as the start type", () -> startingOn(Opening.class, OrderPlaced.class, PaymentReceived.class).build(),
                        v -> new OrderPlaced("higher-" + v, ORDER), v -> new PaymentReceived("lower-" + v, ORDER)),
                new Shape("an abstract sealed class as the start type", () -> startingOn(Intake.class, PhoneIntake.class, WebIntake.class).build(),
                        v -> new PhoneIntake("higher-" + v, ORDER), v -> new WebIntake("lower-" + v, ORDER)),
                new Shape("a sealed concrete class with a subclass on the stream", () -> startingOn(Booking.class, Booking.class, Rebooking.class).build(),
                        v -> new Booking("higher-" + v, ORDER), v -> new Rebooking("lower-" + v, ORDER)),
                new Shape("a non-sealed concrete class with a subclass on the stream", () -> startingOn(PlainBooking.class, PlainBooking.class, PlainRebooking.class).replacementFilter(Filter.all()).build(),
                        v -> new PlainBooking("higher-" + v, ORDER), v -> new PlainRebooking("lower-" + v, ORDER))
        );
    }

    private final InMemorySagaStateStore<Order> store = new InMemorySagaStateStore<>();
    private final List<OrderCommand> issued = new CopyOnWriteArrayList<>();
    private final List<Throwable> refused = new CopyOnWriteArrayList<>();
    private final Set<String> failingItems = ConcurrentHashMap.newKeySet();
    private SagaExecution<OrderEvent, Order, OrderCommand> execution;

    private void start(Shape shape) {
        execution = new SagaExecution<>("orders", shape.saga().get(), store, this::dispatch, CONVERTER, SagaRunnerConfig.defaults(), event -> true, () -> NOW, true);
    }

    private void dispatch(OrderCommand command) {
        if (command instanceof Reserve reserve && failingItems.contains(reserve.item())) {
            throw new IllegalStateException("cannot reserve " + reserve.item());
        }
        issued.add(command);
    }

    private void deliver(CloudEvent event) {
        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(event));
        if (thrown != null) {
            refused.add(thrown);
        }
    }

    private List<String> reactions() {
        return issued.stream().map(command -> ((Reserve) command).item()).toList();
    }

    private List<String> heldKeys() {
        return store.heldIndex(ORDER).stream().map(HeldEventRef::key).sorted().toList();
    }

    @ParameterizedTest
    @MethodSource("shapes")
    void an_item_between_a_higher_and_a_lower_start_event_runs_once_after_the_lower_whichever_start_came_first(Shape shape) {
        // Given
        start(shape);
        deliver(keyed(5, shape.higher().apply(5)));
        deliver(itemAt(4));
        deliver(keyed(3, shape.lower().apply(3)));

        // Then
        assertAll(
                () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                () -> assertThat(reactions()).as("the reactions").containsExactlyInAnyOrder(shape.higherId(5), shape.lowerId(3), ITEM),
                () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                () -> assertThat(store.find(ORDER).orElseThrow().startMark()).as("the start mark").isEqualTo(new StartMark(STREAM, 3, null))
        );
    }

    @ParameterizedTest
    @MethodSource("shapes")
    void an_item_that_came_before_the_instance_started_waits_for_the_lower_start_event_and_then_runs_once(Shape shape) {
        // Given
        start(shape);
        deliver(itemAt(4));
        deliver(keyed(5, shape.higher().apply(5)));
        List<String> reactionsAfterTheHigher = reactions();
        List<String> heldAfterTheHigher = heldKeys();

        // When
        deliver(keyed(3, shape.lower().apply(3)));

        // Then
        assertAll(
                () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                () -> assertThat(reactionsAfterTheHigher).as("the reactions while only the higher start event is applied").containsExactly(shape.higherId(5)),
                () -> assertThat(heldAfterTheHigher).as("the held index while only the higher start event is applied").containsExactly(STREAM + "@4"),
                () -> assertThat(reactions()).as("the reactions after the lower start event").containsExactlyInAnyOrder(shape.higherId(5), shape.lowerId(3), ITEM),
                () -> assertThat(heldKeys()).as("the held index after the lower start event").isEmpty()
        );
    }

    @ParameterizedTest
    @MethodSource("shapes")
    void an_item_below_the_higher_start_event_waits_with_no_reaction_while_no_lower_start_event_has_come(Shape shape) {
        // Given
        start(shape);
        deliver(keyed(5, shape.higher().apply(5)));

        // When
        deliver(itemAt(4));

        // Then
        assertAll(
                () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                () -> assertThat(reactions()).as("the reactions").containsExactly(shape.higherId(5)),
                () -> assertThat(heldKeys()).as("the held index").containsExactly(STREAM + "@4"),
                () -> assertThat(store.find(ORDER).orElseThrow().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @ParameterizedTest
    @MethodSource("shapes")
    void the_events_delivered_in_order_run_once_each_and_leave_nothing_held(Shape shape) {
        // Given
        start(shape);

        // When
        deliver(keyed(3, shape.lower().apply(3)));
        deliver(itemAt(4));
        deliver(keyed(5, shape.higher().apply(5)));

        // Then
        assertAll(
                () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                () -> assertThat(reactions()).as("the reactions").containsExactly(shape.lowerId(3), ITEM, shape.higherId(5)),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    @ParameterizedTest
    @MethodSource("shapes")
    void a_lower_start_event_kept_behind_a_failing_copy_is_flagged_as_a_start_event_and_applies_once_the_copy_heals(Shape shape) {
        // Given
        start(shape);
        failingItems.add(FAILING);
        deliver(itemOn(OTHER_STREAM, 1, FAILING));
        deliver(keyed(5, shape.higher().apply(5)));
        deliver(keyed(3, shape.lower().apply(3)));
        List<HeldEventRef> keptWhileTheCopyFails = store.heldIndex(ORDER).stream().filter(ref -> ref.key().equals(STREAM + "@3")).toList();
        failingItems.clear();

        // When
        deliver(itemAt(6));
        deliver(itemAt(4));

        // Then
        assertAll(
                () -> assertThat(keptWhileTheCopyFails).as("the lower start event is kept while the copy fails").singleElement().satisfies(ref -> assertThat(ref.startEvent()).as("flagged as a start event").isTrue()),
                () -> assertThat(reactions()).as("the reactions").contains(shape.lowerId(3), ITEM, FAILING),
                () -> assertThat(reactions().stream().filter(ITEM::equals).count()).as("the item at 4 and the item at 6 are the same item").isEqualTo(2),
                () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                () -> assertThat(store.find(ORDER).orElseThrow().startMark()).as("the start mark").isEqualTo(new StartMark(STREAM, 3, null))
        );
    }
}
