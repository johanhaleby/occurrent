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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.ItemAdded;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Order;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderCommand;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderEvent;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.OrderPlaced;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.PaymentReceived;
import org.occurrent.dsl.saga.blocking.SagaHeldEventTest.Reserve;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A held copy and the deliveries around it while the state store fails, loses a save, or another delivery slips in
 * between two of its calls. Each test says what must still hold at the end. An event is applied once, or refused so
 * that its redelivery applies it, and no copy is left that nothing will apply.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaHeldCopyFaultInjectionTest {
    static final String ORDER = "order-1", S = "s";
    static final Duration BUDGET = Duration.ofMinutes(5);
    final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
    final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    final Set<String> failing = new HashSet<>();
    final List<String> issued = new CopyOnWriteArrayList<>();
    final FaultInjectingStore<Order> store = new FaultInjectingStore<>();
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    final Logger sagaLog = (Logger) getLogger("org.occurrent.dsl.saga");

    @BeforeEach
    void captureLogs() {
        appender.start();
        sagaLog.addAppender(appender);
    }

    @AfterEach
    void releaseLogs() {
        sagaLog.detachAppender(appender);
        appender.stop();
    }

    List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    Saga<OrderEvent, Order, OrderCommand> saga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (st, e) -> (st == null ? new Order(List.of(), false) : st).withItem(e.eventId()))
                .react(OrderPlaced.class, (st, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                .evolve(ItemAdded.class, (st, e) -> st.withItem(e.eventId()))
                .react(ItemAdded.class, (st, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                .evolve(PaymentReceived.class, (st, e) -> new Order(st.withItem(e.eventId()).items(), true))
                .isTerminal(Order::paid)
                .build();
    }

    SagaExecution<OrderEvent, Order, OrderCommand> ex(SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga(), store, c -> {
            Reserve r = (Reserve) c;
            if (failing.contains(r.item())) throw new IllegalStateException("fail " + r.item());
            issued.add(r.item());
        }, converter, config, e -> true, now::get, true);
    }

    CloudEvent placed(long v) {
        return keyed(v, new OrderPlaced("P" + v, ORDER));
    }

    CloudEvent item(long v) {
        return keyed(v, new ItemAdded("I" + v, ORDER, "I" + v));
    }

    CloudEvent keyed(long v, OrderEvent e) {
        return CloudEventBuilder.v1(converter.toCloudEvent(e)).withExtension(OccurrentCloudEventExtension.occurrent(S, v)).build();
    }

    CloudEvent keyless(String id) {
        return converter.toCloudEvent(new ItemAdded(id, ORDER, id));
    }

    Throwable send(SagaExecution<OrderEvent, Order, OrderCommand> ex, CloudEvent e) {
        Throwable t = catchThrowable(() -> ex.onCloudEvent(e));
        now.updateAndGet(i -> i.plusSeconds(1));
        return t;
    }

    SagaEnvelope<Order> instance() {
        return store.d.find(ORDER).orElseThrow();
    }

    // A keyless copy that fails in the pass over the held copies after a driver the instance already applied

    @Test
    void a_keyless_copy_that_fails_in_the_sweep_of_a_keyed_start_the_instance_already_applied_reaches_its_budget_and_quarantines() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT).withQuarantineAfter(BUDGET));
        failing.add("K");
        assertThat(send(ex, keyless("K"))).as("the keyless event is held").isNull();
        assertThat(send(ex, placed(1))).as("the start is refused since its drain failed").isNotNull();
        assertThat(instance().state().items()).as("the start applied").containsExactly("P1");
        Throwable last = null;
        for (int i = 0; i < 5; i++) {
            now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
            last = send(ex, placed(1));
        }
        assertThat(instance().status()).as("the instance has been refusing the start for 25 minutes against a 5 minute budget").isEqualTo(SagaStatus.QUARANTINED);
    }

    @Test
    void a_keyless_copy_that_fails_in_the_sweep_of_a_driver_not_yet_applied_reaches_its_budget_and_quarantines() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT).withQuarantineAfter(BUDGET));
        failing.add("K");
        assertThat(send(ex, keyless("K"))).as("the keyless event is held").isNull();
        send(ex, placed(1));
        // The same copy, with a driver the instance has not applied
        assertThat(send(ex, item(2))).isNotNull();
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        assertThat(send(ex, item(2))).isNull();
        assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
    }

    // The state cannot be read while a copy sits behind the window

    SagaExecution<OrderEvent, Order, OrderCommand> withACopyBehindTheWindow() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withReorderWindow(1));
        send(ex, placed(1));
        send(ex, item(3));
        send(ex, item(4));
        send(ex, item(5));
        send(ex, item(6));
        // Applying the held copies applies a copy behind the window at once, so the copy goes in after the last delivery that would have applied it
        HeldEventRef ref = new HeldEventRef(S + "@2", S, 2L, null, item(2).getType(), now.get(), false, null);
        store.d.hold(ORDER, new HeldEvent(ORDER, ref, item(2)));
        assertThat(store.d.heldIndex(ORDER)).as("the copy is held behind the window").hasSize(1);
        return ex;
    }

    @Test
    void a_delivery_of_a_copy_held_behind_the_window_is_refused_or_applied_while_the_state_cannot_be_read() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = withACopyBehindTheWindow();
        store.cannotDecode = true;
        Throwable answer = send(ex, item(2));
        store.cannotDecode = false;
        send(ex, item(7));
        assertThat(answer != null || instance().state().items().contains("I2")).as("the event was refused, or its reaction ran once the state reads again").isTrue();
    }

    @Test
    void a_delivery_of_a_copy_held_behind_the_window_applies_it_when_the_state_can_be_read() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = withACopyBehindTheWindow();
        assertThat(send(ex, item(2))).isNull();
        assertThat(instance().state().items()).contains("I2");
    }

    // An earlier start that failed and is named nowhere

    @Test
    void an_item_above_an_earlier_start_that_failed_and_is_named_nowhere_waits_and_is_applied_with_the_start() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        send(ex, placed(5));
        failing.add("P3");
        store.loseWithoutState.set(100);
        store.failHold.set(1);
        assertThat(send(ex, placed(3))).as("the earlier start is refused").isNotNull();
        store.loseWithoutState.set(0);
        failing.clear();
        assertThat(store.d.heldIndex(ORDER)).as("nothing names the earlier start").isEmpty();
        assertThat(send(ex, item(4))).as("the item is acknowledged").isNull();
        assertThat(send(ex, placed(3))).isNull();
        assertThat(instance().state().items()).as("I4 committed after P3, and the instance knew about P3 when I4 arrived").contains("P3", "I4");
    }

    @Test
    void an_item_above_an_earlier_start_that_is_pending_is_applied() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        send(ex, placed(5));
        failing.add("P3");
        assertThat(send(ex, placed(3))).isNotNull();
        failing.clear();
        assertThat(send(ex, item(4))).isNull();
        assertThat(send(ex, placed(3))).isNull();
        assertThat(instance().state().items()).contains("P3", "I4");
    }

    // The failure record and the pending entry of a copy that waited below the start

    @Test
    void the_failure_record_of_a_copy_that_waited_below_the_start_is_cleared_when_a_lower_start_applies_it() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        store.failHold.set(1);
        assertThat(send(ex, item(2))).as("the hold fails").isNotNull();
        assertThat(instance().failure().input()).isEqualTo(S + "@2");
        assertThat(send(ex, item(2))).as("the redelivery is held and acknowledged").isNull();
        assertThat(store.d.heldIndex(ORDER)).hasSize(1);
        assertThat(send(ex, placed(5))).isNull();
        assertThat(store.d.heldIndex(ORDER)).as("the copy is below the start and is not a start event, so it waits").hasSize(1);
        // A start below the copy is what applies it
        assertThat(send(ex, placed(1))).isNull();
        assertThat(store.d.heldIndex(ORDER)).as("the lower start applied the copy").isEmpty();
        assertThat(instance().state().items()).contains("I2");
        send(ex, item(6));
        assertThat(instance().failure()).as("nothing delivers the event again, so nothing else can clear it").isNull();
    }

    // A delivery refused for the failure of a held copy

    SagaExecution<OrderEvent, Order, OrderCommand> started(SagaRunnerConfig config) {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(config);
        assertThat(send(ex, placed(1))).isNull();
        return ex;
    }

    CloudEvent otherStream(long v) {
        return CloudEventBuilder.v1(converter.toCloudEvent(new ItemAdded("T" + v, ORDER, "T" + v))).withExtension(OccurrentCloudEventExtension.occurrent("t", v)).build();
    }

    @Test
    void a_delivery_refused_for_the_failure_of_a_held_copy_is_applied_on_replay_past_the_window() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withReorderWindow(2));
        assertThat(send(ex, otherStream(1))).as("T1 is held").isNull();
        failing.add("T1");
        assertThat(send(ex, placed(1))).as("the start saves, then its drain fails on T1").isNotNull();
        Throwable refused = send(ex, item(2));
        assertThat(refused).as("the delivery is refused because of the held copy").isNotNull();
        failing.clear();
        // The source parked s@2 and moves on
        assertThat(send(ex, item(3))).isNull();
        assertThat(send(ex, item(4))).isNull();
        assertThat(send(ex, item(5))).isNull();
        // The exception tells the operator to replay it
        Throwable replay = send(ex, item(2));
        assertThat(instance().state().items()).as("the replay the exception asks for applies the event").contains("I2");
    }

    @Test
    void a_delivery_that_failed_itself_is_applied_on_replay_past_the_window() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("I2");
        assertThat(send(ex, item(2))).isNotNull();
        failing.clear();
        assertThat(send(ex, item(3))).isNull();
        assertThat(send(ex, item(4))).isNull();
        assertThat(send(ex, item(5))).isNull();
        assertThat(send(ex, item(2))).isNull();
        assertThat(instance().state().items()).contains("I2");
    }

    // An earlier start refused for the failure of a held copy

    @Test
    void an_item_above_an_earlier_start_refused_for_the_failure_of_a_held_copy_is_applied_with_the_start() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        assertThat(send(ex, otherStream(1))).as("T1 is held").isNull();
        failing.add("T1");
        assertThat(send(ex, placed(5))).as("the start saves, then its drain fails on T1").isNotNull();
        assertThat(send(ex, placed(3))).as("the earlier start is refused because of the held copy").isNotNull();
        failing.clear();
        assertThat(send(ex, item(4))).as("the item is acknowledged").isNull();
        assertThat(send(ex, placed(3))).isNull();
        assertThat(instance().state().items()).contains("P3", "I4");
    }

    @Test
    void an_item_above_an_earlier_start_that_failed_itself_is_applied() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        send(ex, placed(5));
        failing.add("P3");
        assertThat(send(ex, placed(3))).isNotNull();
        failing.clear();
        assertThat(send(ex, item(4))).isNull();
        assertThat(send(ex, placed(3))).isNull();
        assertThat(instance().state().items()).contains("P3", "I4");
    }

    // A start that fails while a later start commits past the window

    @Test
    void a_start_that_fails_while_a_later_start_commits_past_the_window_is_applied_on_redelivery() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("P5");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, placed(7))).isNull();
        Throwable refused = send(ex, placed(5));
        assertThat(refused).as("P5's reaction failed, so the delivery is refused").isNotNull();
        failing.clear();
        Throwable replay = send(ex, placed(5));
        assertThat(instance().state().items()).as("the delivery was refused, so its redelivery runs it").contains("P5");
    }

    @Test
    void an_item_that_fails_while_a_later_item_commits_past_the_window_is_applied_on_redelivery() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("I2");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, item(5))).isNull();
        assertThat(send(ex, item(2))).isNotNull();
        failing.clear();
        assertThat(send(ex, item(2))).isNull();
        assertThat(instance().state().items()).contains("I2");
    }

    @Test
    void a_start_that_fails_before_a_later_start_commits_is_applied_on_redelivery() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("P5");
        assertThat(send(ex, placed(5))).isNotNull();
        assertThat(send(ex, placed(7))).isNull();
        failing.clear();
        assertThat(send(ex, placed(5))).isNull();
        assertThat(instance().state().items()).contains("P5");
    }

    // A copy whose release failed on the save that completed the instance

    @Test
    void a_copy_whose_release_failed_on_the_save_that_completed_the_instance_is_released_by_the_redelivery() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        assertThat(send(ex, keyed(3, new PaymentReceived("Z3", ORDER)))).isNull();
        store.failRelease.set(1);
        assertThat(send(ex, placed(1))).as("the release failed, so the delivery is refused").isNotNull();
        assertThat(send(ex, placed(1))).isNull();
        assertThat(instance().status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(instance().state().items()).containsExactly("P1", "Z3");
        assertThat(store.d.heldIndex(ORDER)).as("the instance has ended, so nothing may keep the copy").isEmpty();
    }

    @Test
    void a_copy_whose_release_failed_on_a_save_that_did_not_complete_the_instance_is_released_by_the_next_sweep() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        assertThat(send(ex, item(3))).isNull();
        store.failRelease.set(1);
        assertThat(send(ex, placed(1))).isNotNull();
        assertThat(send(ex, placed(1))).isNull();
        assertThat(instance().state().items()).containsExactly("P1", "I3");
        assertThat(store.d.heldIndex(ORDER)).isEmpty();
    }

    // A delivery below the start that races the start

    @Test
    void a_copy_below_the_start_that_failed_reads_and_a_racing_start_left_behind_keeps_waiting_and_runs_nothing_twice() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults());
        assertThat(send(ex, item(3))).isNull();
        store.failFind.set(1);
        Throwable first = send(ex, item(1));
        store.failMark.set(1);
        Throwable second = send(ex, item(1));
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, placed(2))).isNull();
        Throwable third = send(ex, item(1));
        Throwable fourth = send(ex, item(1));
        assertThat(store.d.heldIndex(ORDER).stream().map(HeldEventRef::key)).as("the copy at 1 is below the start at 2 and waits for a start below it").containsExactly(S + "@1");
        assertThat(instance().state().items()).containsExactly("P2", "I3");
    }

    // Overtaken in flight, and the hold of the plain copy fails

    @Test
    void an_event_overtaken_in_flight_is_kept_as_a_plain_copy_and_applied_on_redelivery() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("I2");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, item(5))).isNull();
        assertThat(send(ex, item(2))).as("I2's reaction failed, so the delivery is refused").isNotNull();
        failing.clear();
        assertThat(send(ex, item(2))).isNull();
        assertThat(instance().state().items()).contains("I2");
        assertThat(store.d.heldIndex(ORDER)).isEmpty();
    }

    @Test
    void an_event_overtaken_in_flight_whose_plain_copy_cannot_be_held_is_skipped_on_redelivery_as_behind_the_reorder_window() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("I2");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            assertThat(send(ex, item(5))).isNull();
            store.failHold.set(1);
        };

        // When
        Throwable first = send(ex, item(2));
        failing.clear();
        Throwable replay = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("I2's reaction failed, so the delivery is refused").isNotNull(),
                () -> assertThat(replay).as("the redelivery is acknowledged behind the window").isNull(),
                () -> assertThat(issued).as("the reaction of I2 never ran").doesNotContain("I2"),
                () -> assertThat(store.d.heldIndex(ORDER)).as("nothing was held for I2").isEmpty(),
                () -> assertThat(errors()).as("the ERROR names the event and says its redelivery is skipped")
                        .anySatisfy(message -> assertThat(message).contains("'" + S + "@2'").contains("skipped as behind the reorder window"))
        );
    }

    @Test
    void an_event_overtaken_in_flight_whose_instance_read_fails_while_keeping_it_is_held_and_runs_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        failing.add("I2");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            assertThat(send(ex, item(5))).isNull();
            // The read that lists I2 as pending passes, and the read that decides on its plain copy fails.
            store.interleaveAt = "afterFindWithoutState";
            store.interleaveAction = () -> store.failFindWithoutState.set(1);
        };

        // When
        Throwable first = send(ex, item(2));
        List<String> heldAfterTheFirst = store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).toList();
        failing.clear();
        Throwable replay = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("I2's reaction failed, so the delivery is refused").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("the copy was held although the read failed").containsExactly(S + "@2"),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(issued.stream().filter("I2"::equals).count()).as("the reaction of I2").isEqualTo(1),
                () -> assertThat(instance().state().items().stream().filter("I2"::equals).count()).as("I2 in the saved state").isEqualTo(1),
                () -> assertThat(store.d.heldIndex(ORDER)).as("the copy is released").isEmpty()
        );
    }
}
