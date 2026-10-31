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
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

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
    final Set<String> poisoned = new HashSet<>();
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

    List<String> warnings() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
    }

    Saga<OrderEvent, Order, OrderCommand> saga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (st, e) -> (st == null ? new Order(List.of(), false) : st).withItem(evolvable(e.eventId())))
                .react(OrderPlaced.class, (st, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                .evolve(ItemAdded.class, (st, e) -> st.withItem(evolvable(e.eventId())))
                .react(ItemAdded.class, (st, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                .evolve(PaymentReceived.class, (st, e) -> new Order(st.withItem(e.eventId()).items(), true))
                .isTerminal(Order::paid)
                .build();
    }

    String evolvable(String eventId) {
        if (poisoned.contains(eventId)) throw new IllegalStateException("evolve fails on " + eventId);
        return eventId;
    }

    SagaExecution<OrderEvent, Order, OrderCommand> ex(SagaRunnerConfig config) {
        return ex(config, true);
    }

    // A feed that does not declare commit order is one whose deliveries hold, and one that does is not.
    SagaExecution<OrderEvent, Order, OrderCommand> ex(SagaRunnerConfig config, boolean holds) {
        return new SagaExecution<>("orders", saga(), store, c -> {
            Reserve r = (Reserve) c;
            if (failing.contains(r.item())) throw new IllegalStateException("fail " + r.item());
            issued.add(r.item());
        }, converter, config, e -> true, now::get, holds);
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
    void a_delivery_below_the_start_that_its_type_has_moved_past_is_held_when_it_is_refused_for_the_failure_of_a_held_copy() {
        // Given the item type applied at 7, 8 and 9 above a start at 4, so version 2 is behind the window of its type
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withReorderWindow(2));
        assertThat(send(ex, placed(4))).isNull();
        assertThat(send(ex, item(7))).isNull();
        assertThat(send(ex, item(8))).isNull();
        assertThat(send(ex, item(9))).isNull();
        CloudEvent other = otherStream(1);
        store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef("t@1", "t", 1L, null, other.getType(), now.get(), false, null), other));
        failing.add("T1");

        // When
        Throwable refused = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the failure of the copy").isNotNull(),
                () -> assertThat(store.d.heldIndex(ORDER).stream().map(HeldEventRef::key)).as("a copy of the delivery is held beside the copy that fails").containsExactlyInAnyOrder(S + "@2", "t@1"),
                () -> assertThat(issued).as("the reaction of the delivery has not run").doesNotContain("I2")
        );
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

    // A copy whose release fails for a delivery that does not own the copy

    private List<Long> pendingVersions() {
        return instance().streamMarks().values().stream().flatMap(marks -> marks.pending().values().stream()).flatMap(List::stream).toList();
    }

    private void holdACopyOfAnotherStream(long version) {
        CloudEvent other = otherStream(version);
        store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef("t@" + version, "t", version, null, other.getType(), now.get(), false, null), other));
    }

    @Test
    void a_release_that_fails_for_a_copy_the_delivery_applied_refuses_the_delivery_and_keeps_it_without_blaming_it() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        holdACopyOfAnotherStream(1);
        store.failRelease.set(1);

        // When
        Throwable refused = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the copy").isNotNull().hasMessageContaining("could not apply or release the held event 't@1'"),
                () -> assertThat(store.d.heldIndex(ORDER).stream().map(HeldEventRef::key)).as("the copy of the delivery is held beside the copy that applied").containsExactlyInAnyOrder(S + "@2", "t@1"),
                () -> assertThat(instance().failure()).as("the failure record").isNull(),
                () -> assertThat(pendingVersions()).as("the pending entries").isEmpty(),
                () -> assertThat(issued).as("the reaction of the copy that applied").contains("T1"),
                () -> assertThat(issued).as("the reaction of the delivery has not run").doesNotContain("I2")
        );
    }

    @Test
    void a_release_that_keeps_failing_past_the_budget_does_not_quarantine_the_instance_once_the_instance_shows_the_copy_applied() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        holdACopyOfAnotherStream(1);
        store.failRelease.set(1);
        send(ex, item(2));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        store.failRelease.set(1);

        // When
        Throwable pastTheBudget = send(ex, item(3));

        // Then
        assertAll(
                () -> assertThat(pastTheBudget).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().failure()).as("the failure record").isNull(),
                () -> assertThat(instance().state().items()).as("the marks show the copy applied").contains("T1")
        );
    }

    @Test
    void the_copy_is_released_and_each_delivery_refused_for_it_reacts_exactly_once_when_the_release_works_again() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        holdACopyOfAnotherStream(1);
        store.failRelease.set(1);
        send(ex, item(2));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        store.failRelease.set(1);
        send(ex, item(3));

        // When
        Throwable next = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                () -> assertThat(store.d.heldIndex(ORDER)).as("nothing is left held").isEmpty(),
                () -> assertThat(issued.stream().filter("T1"::equals).count()).as("the reaction of the copy").isEqualTo(1),
                () -> assertThat(issued.stream().filter("I2"::equals).count()).as("the reaction of the first refused delivery").isEqualTo(1),
                () -> assertThat(issued.stream().filter("I3"::equals).count()).as("the reaction of the second refused delivery").isEqualTo(1),
                () -> assertThat(issued.stream().filter("I4"::equals).count()).as("the reaction of the delivery that found the release working").isEqualTo(1)
        );
    }

    // A delivery with its own copy whose save is lost to a delivery that applies the copy

    @Test
    void a_delivery_with_its_own_copy_whose_save_loses_to_a_delivery_that_applies_the_copy_and_moves_its_type_past_the_window_runs_once_and_leaves_no_copy() {
        // Given a copy of another stream that the sweep applies before the copy of the delivery
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        holdACopyOfAnotherStream(1);
        now.updateAndGet(i -> i.plusSeconds(1));
        store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(S + "@4", S, 4L, null, item(4).getType(), now.get(), false, null), item(4)));
        AtomicBoolean raced = new AtomicBoolean();
        store.interleaveAt = "beforeCas";
        store.interleaveAction = () -> {
            raced.set(true);
            for (long v = 5; v <= 9; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
        };

        // When
        Throwable delivered = send(ex, item(4));
        send(ex, item(10));

        // Then
        assertAll(
                () -> assertThat(raced).as("the other deliveries ran before the save of the delivery").isTrue(),
                () -> assertThat(delivered).as("the retry acknowledges the delivery").isNull(),
                () -> assertThat(store.d.heldIndex(ORDER)).as("no copy of the delivery is held").isEmpty(),
                () -> assertThat(issued.stream().filter("I4"::equals).count()).as("the reaction of the delivery, after a further delivery swept the copies").isEqualTo(1)
        );
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

    // A delivery whose own sweep moves its type past it

    private SagaExecution<OrderEvent, Order, OrderCommand> startAtFiveWithCopiesAtTenAndElevenOfWhichTheCopyAtTenFailed() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        failing.add("T10");
        send(ex, otherStream(10));
        send(ex, otherStream(11));
        send(ex, placed(5));
        failing.remove("T10");
        assertThat(instance().streamMarks().get("t")).as("the instance has no mark on the stream of the copies yet").isNull();
        return ex;
    }

    @Test
    void a_delivery_that_read_the_instance_as_new_is_applied_before_the_copies_above_it_and_runs_once_with_no_copy() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startAtFiveWithCopiesAtTenAndElevenOfWhichTheCopyAtTenFailed();

        // When
        Throwable first = send(ex, otherStream(8));
        List<String> heldAfterTheFirst = store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).toList();
        Throwable replay = send(ex, otherStream(8));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldAfterTheFirst).as("no copy of the delivery is held").isEmpty(),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(issued.stream().filter("T8"::equals).count()).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(issued.stream().filter("T11"::equals).count()).as("the reaction of the copy the sweep applied").isEqualTo(1),
                () -> assertThat(store.d.heldIndex(ORDER)).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_delivery_applied_before_the_copies_above_it_is_refused_for_the_one_that_fails_and_is_not_held_and_runs_once() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startAtFiveWithCopiesAtTenAndElevenOfWhichTheCopyAtTenFailed();
        failing.add("T11");

        // When
        Throwable first = send(ex, otherStream(8));
        List<String> heldAfterTheFirst = store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).toList();
        failing.clear();
        Throwable next = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the copy that fails after it").isNotNull().hasMessageContaining("t@11"),
                () -> assertThat(heldAfterTheFirst).as("the delivery is applied, so no copy of it is held").doesNotContain("t@8"),
                () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                () -> assertThat(issued.stream().filter("T8"::equals).count()).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    // A replay of an event the instance applied long ago

    @Test
    void a_replay_of_an_event_applied_long_ago_whose_reads_fail_is_refused_and_leaves_no_copy_and_no_second_reaction() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        for (long v = 1; v <= 5; v++) {
            send(ex, otherStream(v));
        }
        store.failFind.set(1);
        store.failFindWithoutState.set(2);

        // When
        Throwable replay = send(ex, otherStream(1));
        Throwable next = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(replay).as("the replay is refused while the instance cannot be read").isNotNull(),
                () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                () -> assertThat(store.d.heldIndex(ORDER)).as("no copy of the replay is held").isEmpty(),
                () -> assertThat(issued.stream().filter("T1"::equals).count()).as("the reaction of the event applied long ago").isEqualTo(1)
        );
    }

    @Test
    void a_delivery_that_read_the_instance_as_new_is_held_when_its_pending_write_is_lost_and_the_instance_is_then_behind_the_window_and_runs_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        failing.add("I2");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            assertThat(send(ex, item(5))).isNull();
            store.loseWithoutState.set(100);
        };

        // When
        Throwable first = send(ex, item(2));
        store.loseWithoutState.set(0);
        List<String> heldAfterTheFirst = store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).toList();
        failing.clear();
        Throwable replay = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the reaction failed, so the delivery is refused").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("a copy of the delivery is held, since it read the event as new").containsExactly(S + "@2"),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(issued.stream().filter("I2"::equals).count()).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(store.d.heldIndex(ORDER)).as("the copy is released").isEmpty()
        );
    }

    // A copy that was applied while the sweep that charges it was failing on it

    @Test
    void a_copy_whose_save_took_effect_before_the_response_was_lost_does_not_quarantine_the_instance_past_its_budget() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        failing.add("T1");
        send(ex, otherStream(1));
        send(ex, placed(1));
        assertThat(store.d.heldIndex(ORDER).getFirst().failingSince()).as("the copy has a clock").isNotNull();
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        failing.clear();
        store.writeThenThrowCas.set(1);

        // When
        Throwable next = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(next).as("the delivery is refused since the response of its save was lost").isNotNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().failure()).as("the failure record").isNull()
        );
    }

    private long reactionsTo(String item) {
        return issued.stream().filter(item::equals).count();
    }

    private List<String> heldKeys() {
        return store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).sorted().toList();
    }

    // A delivery whose own sweep applies its own waiting copy

    @Test
    void a_redelivery_of_a_waiting_copy_whose_own_sweep_applies_it_and_the_copies_around_it_runs_each_once_and_leaves_no_copy() {
        // Given a start at 5 refused for a failing copy at 6, a waiting copy at 4 and a lower start kept at 3
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(3));
        failing.add("I6");
        send(ex, item(6));
        send(ex, item(7));
        send(ex, item(8));
        assertThat(send(ex, placed(5))).as("the start is refused for the copy at 6").isNotNull();
        holdDirect(S + "@4", S, 4, item(4), false);
        holdDirect(S + "@3", S, 3, placed(3), true);
        assertThat(heldKeys()).as("the waiting copies").contains(S + "@4", S + "@3");
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(4));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("P3")).as("the start at 3").isEqualTo(1),
                () -> assertThat(reactionsTo("I4")).as("the item at 4, which the sweep of its own redelivery applied").isEqualTo(1),
                () -> assertThat(reactionsTo("I6")).as("the item at 6").isEqualTo(1),
                () -> assertThat(reactionsTo("I7")).as("the item at 7").isEqualTo(1),
                () -> assertThat(reactionsTo("I8")).as("the item at 8").isEqualTo(1),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    // The save that follows the given number of releases loses once.
    private void theSaveAfterTheFourthReleaseLoses(int remaining) {
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> {
            if (remaining == 1) {
                store.loseCas.set(1);
            } else {
                theSaveAfterTheFourthReleaseLoses(remaining - 1);
            }
        };
    }

    @Test
    void a_redelivery_whose_own_copy_the_sweep_applied_before_a_later_copy_lost_its_save_runs_once_and_leaves_no_copy() {
        // Given copies at 2 to 6 held before the start, the one at 2 failing, and the redelivery of the one at 2
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        failing.add("I2");
        for (long v = 2; v <= 6; v++) {
            send(ex, item(v));
        }
        assertThat(send(ex, placed(1))).as("the start is refused for the copy at 2").isNotNull();
        failing.clear();
        theSaveAfterTheFourthReleaseLoses(4);

        // When
        Throwable redelivery = send(ex, item(2));
        send(ex, item(7));

        // Then
        assertAll(
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the item at 2").isEqualTo(1),
                () -> assertThat(reactionsTo("I3")).as("the item at 3").isEqualTo(1),
                () -> assertThat(reactionsTo("I4")).as("the item at 4").isEqualTo(1),
                () -> assertThat(reactionsTo("I5")).as("the item at 5").isEqualTo(1),
                () -> assertThat(reactionsTo("I6")).as("the item at 6, whose save was lost and so dispatched again").isGreaterThanOrEqualTo(1),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    @Test
    void a_delivery_that_saw_the_event_as_new_and_then_found_a_copy_of_it_is_not_overtaken_when_another_delivery_applies_the_copy_and_passes_the_window() {
        // Given a copy of another stream that the sweep applies first, which the first attempt loses its save on
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdACopyOfAnotherStream(1);
        now.updateAndGet(i -> i.plusSeconds(1));
        AtomicBoolean secondAttemptLost = new AtomicBoolean();
        store.interleaveAt = "beforeCas";
        store.interleaveAction = () -> {
            // Another delivery of the same event kept a copy of it, and the first attempt loses its save
            store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(S + "@4", S, 4L, null, item(4).getType(), now.get(), false, null), item(4)));
            store.loseCas.set(1);
            store.interleaveAt = "beforeCas";
            store.interleaveAction = () -> {
                // The second attempt finds the copy, and other deliveries apply both copies and pass the window before its save
                secondAttemptLost.set(true);
                for (long v = 5; v <= 9; v++) {
                    assertThat(send(ex, item(v))).isNull();
                }
            };
        };

        // When
        Throwable delivered = send(ex, item(4));
        send(ex, item(10));

        // Then
        assertAll(
                () -> assertThat(secondAttemptLost).as("the second attempt lost its save to the other deliveries").isTrue(),
                () -> assertThat(delivered).as("the third attempt acknowledges the delivery").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the delivery, after a further delivery swept the copies").isEqualTo(1),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    // A feed in commit order and the sweep of a copy held before the deploy

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_that_its_own_sweep_passes_is_skipped_without_an_error() {
        // Given copies at 6 to 8 held by a holding feed, and the same instance then driven by a feed in commit order
        SagaExecution<OrderEvent, Order, OrderCommand> holding = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        failing.add("I6");
        send(holding, item(6));
        send(holding, item(7));
        send(holding, item(8));
        send(holding, placed(5));
        failing.clear();
        SagaExecution<OrderEvent, Order, OrderCommand> commitOrder = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);

        // When
        Throwable delivery = send(commitOrder, item(4));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the event below the start is acknowledged").isNull(),
                () -> assertThat(errors()).as("nothing is logged as an error").isEmpty(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the event below the start").isZero()
        );
    }

    @Test
    void an_event_on_a_feed_in_commit_order_with_copies_held_before_the_deploy_above_it_is_applied_before_them_and_reacts_once_with_no_copy() {
        // Given copies at 10 and 11 of another stream held by a holding feed, the one at 10 failing, and a feed in commit order after the deploy
        SagaExecution<OrderEvent, Order, OrderCommand> holding = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        failing.add("T10");
        send(holding, otherStream(10));
        send(holding, otherStream(11));
        send(holding, placed(5));
        failing.clear();
        SagaExecution<OrderEvent, Order, OrderCommand> commitOrder = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);

        // When
        Throwable first = send(commitOrder, otherStream(8));
        List<String> heldAfterTheFirst = heldKeys();
        Throwable redelivery = send(commitOrder, otherStream(8));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldAfterTheFirst).as("no copy of the event is held").isEmpty(),
                () -> assertThat(errors()).as("nothing is logged as lost").isEmpty(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("T8")).as("the reaction of the event").isEqualTo(1),
                () -> assertThat(reactionsTo("T11")).as("the reaction of the copy applied after it").isEqualTo(1)
        );
    }

    @Test
    void a_start_below_the_start_on_a_feed_in_commit_order_that_meets_a_failing_copy_is_kept_and_applies_once_when_the_copy_is_fixed() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(3), false);
        send(ex, placed(5));
        holdACopyOfAnotherStream(1);
        failing.add("T1");

        // When
        Throwable refused = send(ex, placed(3));
        List<String> heldAfterTheRefusal = heldKeys();
        failing.clear();
        Throwable replay = send(ex, placed(3));

        // Then
        assertAll(
                () -> assertThat(refused).as("the start is refused for the copy").isNotNull(),
                () -> assertThat(heldAfterTheRefusal).as("the start is kept beside the copy").containsExactlyInAnyOrder("t@1", S + "@3"),
                () -> assertThat(replay).as("the replay is acknowledged").isNull(),
                () -> assertThat(reactionsTo("P3")).as("the start at 3").isEqualTo(1),
                () -> assertThat(instance().startMark().version()).as("the start of the instance").isEqualTo(3),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    // A first read that fails for a delivery behind the window

    @Test
    void a_delivery_behind_the_window_whose_first_reads_fail_is_refused_and_its_redelivery_is_skipped_as_for_any_arrival_behind_the_window() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        send(ex, placed(1));
        for (long v = 3; v <= 6; v++) {
            send(ex, item(v));
        }
        store.failFind.set(1);
        store.failFindWithoutState.set(1);

        // When
        Throwable first = send(ex, item(2));
        List<String> heldAfterTheFirst = heldKeys();
        Throwable replay = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused while the instance cannot be read").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("nothing is held for it").isEmpty(),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isZero()
        );
    }

    @Test
    void a_delivery_behind_the_window_whose_every_read_fails_is_held_and_runs_once() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        send(ex, placed(1));
        for (long v = 3; v <= 6; v++) {
            send(ex, item(v));
        }
        store.failFind.set(1);
        store.failFindWithoutState.set(3);

        // When
        Throwable first = send(ex, item(2));
        List<String> heldAfterTheFirst = heldKeys();
        send(ex, item(7));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("a copy of the delivery is held").containsExactly(S + "@2"),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    // The clock of a copy and the instance it can quarantine

    @Test
    void a_keyless_copy_past_its_budget_whose_save_took_effect_before_the_response_was_lost_quarantines_the_instance_since_nothing_proves_it_applied() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT).withQuarantineAfter(BUDGET));
        failing.add("K");
        send(ex, keyless("K"));
        send(ex, placed(1));
        assertThat(store.d.heldIndex(ORDER).getFirst().failingSince()).as("the copy has a clock").isNotNull();
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        failing.clear();
        store.writeThenThrowCas.set(1);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(instance().state().items()).as("the save applied the copy").contains("K"),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED)
        );
    }

    @Test
    void a_keyed_copy_flagged_as_a_start_event_below_the_start_that_fails_past_its_budget_quarantines_the_instance_at_its_key() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        send(ex, placed(5));
        CloudEvent p3 = placed(3);
        store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(S + "@3", S, 3L, null, p3.getType(), now.get(), true, null), p3));
        failing.add("P3");
        Throwable insideTheBudget = send(ex, item(6));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));

        // When
        Throwable pastTheBudget = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(insideTheBudget).as("the delivery inside the budget is refused").isNotNull(),
                () -> assertThat(pastTheBudget).as("the delivery past the budget is acknowledged").isNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(instance().failure().input()).as("the failure record").isEqualTo(S + "@3"),
                () -> assertThat(heldKeys()).as("the copy and the delivery stay held").containsExactlyInAnyOrder(S + "@3", S + "@6")
        );
    }

    @Test
    void a_copy_applied_by_a_delivery_between_the_reads_of_the_charge_and_its_write_does_not_quarantine_the_instance() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        failing.add("T1");
        send(ex, otherStream(1));
        send(ex, placed(1));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        store.interleaveAt = "beforeCasWithoutState";
        store.interleaveAction = () -> {
            failing.clear();
            assertThat(send(ex, item(3))).isNull();
        };

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is refused for the copy").isNotNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(reactionsTo("T1")).as("the reaction of the copy the other delivery applied").isEqualTo(1)
        );
    }

    @Test
    void a_store_outage_that_fails_the_read_of_a_copy_and_the_read_of_the_instance_past_the_budget_quarantines_nothing() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        send(ex, otherStream(1));
        store.failHeldRead.set(1);
        send(ex, placed(1));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        store.failHeldRead.set(1);
        store.failFindWithoutState.set(2);

        // When
        Throwable delivery = send(ex, item(2));
        SagaStatus statusAfterTheOutage = instance().status();
        Throwable next = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is refused").isNotNull(),
                () -> assertThat(statusAfterTheOutage).as("the instance status after the outage").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("T1")).as("the reaction of the copy").isEqualTo(1),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void a_healthy_copy_with_a_clock_from_an_earlier_failed_read_is_applied_and_clears_on_the_next_read_that_works() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        send(ex, otherStream(1));
        store.failHeldRead.set(1);
        send(ex, placed(1));
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(heldKeys()).as("the held index").isEmpty()
        );
    }

    // A delivery that read the instance as new and then fails on a read of the held events

    @Test
    void a_delivery_whose_read_of_the_held_events_fails_after_other_deliveries_passed_it_by_the_window_is_refused_and_reacts_once() {
        // Given s@2 reads the instance as new, and s@3 to s@5 move the instance past it before the read of the held events fails
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withReorderWindow(2));
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            for (long v = 3; v <= 5; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
            store.interleaveAt = "afterHeldIndex";
            store.interleaveAction = () -> {
                throw new IllegalStateException("cannot read the held index");
            };
        };

        // When
        Throwable first = send(ex, item(2));
        boolean theReadFailed = store.interleaveAt == null;
        Throwable redelivery = send(ex, item(2));
        send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(theReadFailed).as("the read of the held events failed").isTrue(),
                () -> assertThat(first).as("the delivery is refused").isNotNull().hasMessageContaining("cannot read the held index"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A delivery that cannot be held after a held copy failed

    private SagaExecution<OrderEvent, Order, OrderCommand> refusedForACopyThatCannotBeReleasedWhileTheDeliveryCannotBeHeld() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        holdACopyOfAnotherStream(1);
        store.failRelease.set(1);
        return ex;
    }

    @Test
    void a_delivery_refused_for_a_held_copy_that_fails_and_that_cannot_be_held_is_listed_as_pending_and_reacts_once_on_redelivery_past_the_window() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = refusedForACopyThatCannotBeReleasedWhileTheDeliveryCannotBeHeld();
        store.failHold.set(1);

        // When
        Throwable first = send(ex, item(6));
        List<String> heldAfterTheFirst = heldKeys();
        send(ex, item(7));
        send(ex, item(8));
        send(ex, item(9));
        Throwable redelivery = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the copy").isNotNull().hasMessageContaining("t@1"),
                () -> assertThat(heldAfterTheFirst).as("no copy of the delivery could be held").doesNotContain(S + "@6"),
                () -> assertThat(warnings()).as("the WARN says the event is listed as pending instead")
                        .anySatisfy(message -> assertThat(message).contains("'" + S + "@6'").contains("lists the event as pending")),
                () -> assertThat(errors()).as("the event was listed, so nothing is logged as lost").isEmpty(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I6")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(reactionsTo("T1")).as("the reaction of the copy").isEqualTo(1)
        );
    }

    @Test
    void a_delivery_refused_for_a_held_copy_that_fails_and_that_can_neither_be_held_nor_listed_is_logged_at_error_with_its_event() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = refusedForACopyThatCannotBeReleasedWhileTheDeliveryCannotBeHeld();
        store.failHold.set(2);
        store.interleaveAt = "beforeCasWithoutState";
        store.interleaveAction = () -> {
            throw new IllegalStateException("cannot list");
        };

        // When
        Throwable first = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the copy").isNotNull().hasMessageContaining("t@1"),
                () -> assertThat(heldKeys()).as("no copy of the delivery could be held").doesNotContain(S + "@6"),
                () -> assertThat(errors()).as("the ERROR names the event").singleElement()
                        .satisfies(message -> assertThat(message).contains("'" + S + "@6'").contains("could not keep it"))
        );
    }

    // A keyless copy whose release fails after its save, which nothing can prove applied

    private SagaExecution<OrderEvent, Order, OrderCommand> withAKeylessCopyHeldBeforeTheStart() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT).withQuarantineAfter(BUDGET));
        assertThat(send(ex, keyless("K"))).as("the keyless event is held").isNull();
        return ex;
    }

    @Test
    void a_keyless_copy_whose_save_took_effect_and_whose_release_failed_reacts_again_on_the_next_attempt_which_is_the_named_limit_of_a_copy_with_no_redelivery_key() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = withAKeylessCopyHeldBeforeTheStart();
        store.failRelease.set(1);

        // When
        Throwable start = send(ex, placed(1));
        Throwable next = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(start).as("the start is refused since the release failed").isNotNull(),
                () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("K")).as("the reaction of the copy, once for the save that stood and once for the next attempt").isEqualTo(2),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the next delivery").isEqualTo(1),
                () -> assertThat(heldKeys()).as("the copy is released by the next attempt").isEmpty()
        );
    }

    @Test
    void a_keyless_copy_whose_release_keeps_failing_past_the_budget_never_quarantines_the_instance_which_is_the_named_limit_of_a_copy_with_no_redelivery_key() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = withAKeylessCopyHeldBeforeTheStart();
        store.failRelease.set(1);
        send(ex, placed(1));
        List<Throwable> refusals = new ArrayList<>();

        // When
        for (int i = 0; i < 3; i++) {
            now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
            store.failRelease.set(1);
            refusals.add(send(ex, placed(1)));
        }

        // Then
        assertAll(
                () -> assertThat(refusals).as("every attempt is refused for the release").allSatisfy(refusal -> assertThat(refusal).isNotNull()),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().failure()).as("the failure record").isNull(),
                () -> assertThat(reactionsTo("K")).as("the reaction of the copy, once for each attempt").isEqualTo(4)
        );
    }

    // The probes of the sweep, the reads around it, and the delivery that a sweep passes

    private void holdDirect(String key, String stream, long version, CloudEvent event, boolean startEvent) {
        holdDirect(key, stream, version, event, startEvent, null);
    }

    private void holdDirect(String key, String stream, long version, CloudEvent event, boolean startEvent, Instant failingSince) {
        store.d.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(key, stream, version, null, event.getType(), now.get(), startEvent, failingSince), event));
        now.updateAndGet(i -> i.plusSeconds(1));
    }

    private void passedByTheNextDelivery(SagaExecution<OrderEvent, Order, OrderCommand> ex, long version) {
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, item(version))).isNull();
    }

    @Test
    void an_item_whose_evolve_throws_while_another_delivery_passes_it_by_the_window_is_refused_and_reacts_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        poisoned.add("I2");
        passedByTheNextDelivery(ex, 5);

        // When
        Throwable first = send(ex, item(2));
        poisoned.clear();
        Throwable replay = send(ex, item(2));
        send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the evolve failure refuses the delivery").isNotNull().hasMessageContaining("evolve fails on I2"),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void an_item_whose_evolve_throws_while_another_delivery_passes_it_by_the_window_and_quarantine_is_on_is_refused_and_reacts_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        poisoned.add("I2");
        passedByTheNextDelivery(ex, 5);

        // When
        Throwable first = send(ex, item(2));
        poisoned.clear();
        Throwable replay = send(ex, item(2));
        send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused").isNotNull(),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void a_start_whose_evolve_throws_while_a_later_start_passes_it_by_the_window_is_refused_and_reacts_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        poisoned.add("P5");
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> assertThat(send(ex, placed(8))).isNull();

        // When
        Throwable first = send(ex, placed(5));
        poisoned.clear();
        Throwable replay = send(ex, placed(5));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused").isNotNull(),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("P5")).as("the reaction of the start").isEqualTo(1)
        );
    }

    @Test
    void an_item_whose_evolve_throws_while_a_held_copy_is_listed_and_another_delivery_passes_it_by_the_window_is_refused_and_reacts_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect("t@1", "t", 1, otherStream(1), false);
        poisoned.add("I2");
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> assertThat(send(ex, item(5))).isNull();

        // When
        Throwable first = send(ex, item(2));
        poisoned.clear();
        Throwable replay = send(ex, item(2));
        send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused").isNotNull(),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> startedAtFiveWithCopiesAboveAndBelowIt(SagaRunnerConfig config, long startOfTheCopies) {
        SagaExecution<OrderEvent, Order, OrderCommand> holding = ex(config);
        failing.add("I6");
        send(holding, item(6));
        send(holding, item(7));
        send(holding, item(8));
        assertThat(send(holding, placed(5))).as("the start is refused for I6").isNotNull();
        holdDirect(S + "@" + startOfTheCopies, S, startOfTheCopies, placed(startOfTheCopies), true);
        failing.clear();
        return ex(config, false);
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_is_applied_after_the_held_start_that_lowers_the_start_below_it_and_before_the_copies_above_it() {
        // Given a start at 5 refused for I6, a start copy at 3 that the sweep applies, and a feed in commit order
        SagaRunnerConfig config = SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2);
        SagaExecution<OrderEvent, Order, OrderCommand> commitOrder = startedAtFiveWithCopiesAboveAndBelowIt(config, 3);

        // When
        Throwable first = send(commitOrder, item(4));
        List<String> heldAfterTheFirst = heldKeys();
        Throwable redelivery = send(commitOrder, item(4));

        // Then
        assertAll(
                () -> assertThat(instance().startMark().version()).as("the start copy lowered the start below the event").isEqualTo(3),
                () -> assertThat(first).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldAfterTheFirst).as("no copy of the event is held").isEmpty(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the event").isEqualTo(1),
                () -> assertThat(reactionsTo("I8")).as("the reaction of a copy above the event").isEqualTo(1),
                () -> assertThat(issued.stream().filter(i -> i.equals("P3") || i.equals("I4") || i.equals("I8")).toList()).as("the start copy, then the event, then the copy above it").containsExactly("P3", "I4", "I8"),
                () -> assertThat(errors()).as("nothing is logged as lost").isEmpty()
        );
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_is_applied_once_a_held_lower_start_has_lowered_the_start_below_it() {
        // Given a start at 5 refused for I6, then a lower start copy at 3 held beside it
        SagaRunnerConfig config = SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(3);
        SagaExecution<OrderEvent, Order, OrderCommand> holding = ex(config);
        failing.add("I6");
        send(holding, item(6));
        send(holding, item(7));
        send(holding, item(8));
        assertThat(send(holding, placed(5))).as("the start is refused for I6").isNotNull();
        holdDirect(S + "@3", S, 3, placed(3), true);
        assertThat(heldKeys()).contains(S + "@3");
        failing.clear();
        SagaExecution<OrderEvent, Order, OrderCommand> commitOrder = ex(config, false);

        // When
        Throwable first = send(commitOrder, item(4));
        List<String> heldAfterTheFirst = heldKeys();
        Throwable redelivery = send(commitOrder, item(4));

        // Then
        assertAll(
                () -> assertThat(instance().startMark().version()).as("the start copy lowered the start below the event").isEqualTo(3),
                () -> assertThat(first).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldAfterTheFirst).as("no copy of the event is held").isEmpty(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the event").isEqualTo(1),
                () -> assertThat(errors()).as("nothing is logged as lost").isEmpty()
        );
    }

    @Test
    void an_event_on_a_feed_in_commit_order_that_a_start_copy_lifts_above_the_start_is_applied_before_a_later_copy_quarantines_the_instance() {
        // Given
        SagaRunnerConfig config = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(3);
        SagaExecution<OrderEvent, Order, OrderCommand> holding = ex(config);
        failing.add("I6");
        send(holding, item(6));
        assertThat(send(holding, placed(5))).as("the start is refused for I6").isNotNull();
        holdDirect(S + "@3", S, 3, placed(3), true);
        SagaExecution<OrderEvent, Order, OrderCommand> commitOrder = ex(config, false);
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));

        // When
        Throwable delivery = send(commitOrder, item(4));

        // Then
        assertAll(
                () -> assertThat(instance().startMark().version()).as("the start copy lowered the start below the event").isEqualTo(3),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(delivery).as("the quarantine lets the delivery past").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the event, applied before the quarantine").isEqualTo(1),
                () -> assertThat(heldKeys()).as("the event is applied, so no copy of it is held").doesNotContain(S + "@4")
        );
    }

    @Test
    void a_delivery_whose_waiting_copy_its_own_sweep_applied_before_a_later_copy_failed_is_not_kept_again_and_reacts_once() {
        // Given a copy of the delivery waiting below the start 5, a start copy at 3 and copies at 6 to 8 of which 8 fails
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        assertThat(send(ex, placed(5))).isNull();
        assertThat(send(ex, item(4))).as("I4 waits below the start").isNull();
        assertThat(heldKeys()).containsExactly(S + "@4");
        holdDirect(S + "@3", S, 3, placed(3), true);
        holdDirect(S + "@6", S, 6, item(6), false);
        holdDirect(S + "@7", S, 7, item(7), false);
        holdDirect(S + "@8", S, 8, item(8), false);
        failing.add("I8");

        // When
        Throwable refused = send(ex, item(4));
        failing.clear();
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for I8").isNotNull().hasMessageContaining(S + "@8"),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void a_delivery_whose_waiting_copy_its_own_sweep_applied_before_the_release_of_a_later_copy_failed_is_not_kept_again_and_reacts_once() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        assertThat(send(ex, placed(5))).isNull();
        assertThat(send(ex, item(4))).as("I4 waits below the start").isNull();
        holdDirect(S + "@3", S, 3, placed(3), true);
        holdDirect(S + "@6", S, 6, item(6), false);
        holdDirect(S + "@7", S, 7, item(7), false);
        // The copies are released as P3, I4, I6 and I7, and the fourth release fails
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> {
            store.interleaveAt = "afterRelease";
            store.interleaveAction = () -> {
                store.interleaveAt = "afterRelease";
                store.interleaveAction = () -> store.failRelease.set(1);
            };
        };

        // When
        Throwable refused = send(ex, item(4));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the release of I7").isNotNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void a_delivery_whose_waiting_copy_was_applied_before_it_is_refused_for_a_copy_above_it_and_reacts_once_when_that_copy_is_fixed() {
        // Given a start at 5 refused for I6, an item below the start held, and a lower start held
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(3));
        failing.add("I6");
        send(ex, item(6));
        send(ex, item(7));
        send(ex, item(8));
        assertThat(send(ex, placed(5))).as("the start is refused for I6").isNotNull();
        holdDirect(S + "@4", S, 4, item(4), false);
        holdDirect(S + "@3", S, 3, placed(3), true);
        assertThat(heldKeys()).contains(S + "@3", S + "@4");
        failing.clear();
        failing.add("I8");

        // When
        Throwable refused = send(ex, item(4));
        failing.clear();
        Throwable redelivery = send(ex, item(4));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for I8").isNotNull().hasMessageContaining(S + "@8"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the delivery").isEqualTo(1)
        );
    }

    @Test
    void a_keyless_copy_with_a_clock_older_than_the_budget_whose_release_fails_once_after_it_applied_does_not_quarantine_the_instance() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT).withQuarantineAfter(BUDGET));
        failing.add("K");
        send(ex, keyless("K"));
        send(ex, placed(1));
        assertThat(store.d.heldIndex(ORDER).getFirst().failingSince()).as("the copy has a clock").isNotNull();
        failing.clear();
        now.updateAndGet(i -> i.plus(BUDGET).plusSeconds(1));
        store.failRelease.set(1);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(reactionsTo("K")).as("the reaction of the copy, whose save stands").isEqualTo(1),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_copy_that_completes_the_instance_and_whose_release_fails_is_released_by_the_redelivery_which_logs_no_error() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
        holdDirect(S + "@3", S, 3, keyed(3, new PaymentReceived("Z3", ORDER)), false);
        store.failRelease.set(1);

        // When
        Throwable refused = send(ex, item(2));
        List<String> heldAfterTheRefusal = heldKeys();
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(heldAfterTheRefusal).as("only the copy is held, since the delivery was applied before it").containsExactly(S + "@3"),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery, which the copy that completes the instance does not drop").isEqualTo(1),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.COMPLETED),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty(),
                () -> assertThat(errors()).as("nothing is logged as an error").isEmpty()
        );
    }

    @Test
    void a_delivery_applied_before_the_copies_above_it_is_not_kept_when_a_later_release_fails_and_reacts_once() {
        // Given copies at 5 and 6 that apply after the delivery at 2, and a window of 2 that they would pass it with
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect(S + "@5", S, 5, item(5), false);
        holdDirect(S + "@6", S, 6, item(6), false);
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> store.failRelease.set(1);

        // When
        Throwable refused = send(ex, item(2));
        List<String> heldAfterTheRefusal = heldKeys();
        Throwable redelivery = send(ex, item(2));
        send(ex, item(7));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the release of I6").isNotNull().hasMessageContaining(S + "@6"),
                () -> assertThat(heldAfterTheRefusal).as("only the copy that could not be released is held").containsExactly(S + "@6"),
                () -> assertThat(instance().state().items()).as("the delivery is applied").contains("I2"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(reactionsTo("I5")).as("the reaction of the first copy").isEqualTo(1),
                () -> assertThat(reactionsTo("I6")).as("the reaction of the second copy").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_keyed_copy_behind_the_window_whose_release_fails_is_not_applied_twice_by_later_deliveries_of_its_type() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        for (long v = 3; v <= 6; v++) {
            assertThat(send(ex, item(v))).isNull();
        }
        holdDirect(S + "@2", S, 2, item(2), false);
        store.failRelease.set(1);

        // When
        Throwable first = send(ex, item(7));
        send(ex, item(8));
        send(ex, item(7));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the copy behind the window").isEqualTo(1),
                () -> assertThat(reactionsTo("I7")).as("the reaction of the first delivery").isEqualTo(1),
                () -> assertThat(reactionsTo("I8")).as("the reaction of the second delivery").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_copy_behind_the_window_whose_release_fails_with_a_kept_delivery_of_another_stream_reacts_once_for_each() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        for (long v = 7; v <= 10; v++) {
            assertThat(send(ex, item(v))).isNull();
        }
        holdDirect(S + "@6", S, 6, item(6), false);
        store.failRelease.set(1);

        // When
        Throwable refused = send(ex, otherStream(1));
        List<String> heldAfterTheRefusal = heldKeys();
        Throwable again = send(ex, otherStream(1));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(heldAfterTheRefusal).as("the delivery is kept beside the copy").contains(S + "@6", "t@1"),
                () -> assertThat(again).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I6")).as("the reaction of the copy").isEqualTo(1),
                () -> assertThat(reactionsTo("T1")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_delivery_with_a_waiting_copy_whose_sweep_applies_every_copy_and_lowers_the_start_reacts_once_and_logs_no_error() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        assertThat(send(ex, placed(5))).isNull();
        assertThat(send(ex, item(4))).isNull();
        holdDirect(S + "@3", S, 3, placed(3), true);
        holdDirect(S + "@6", S, 6, item(6), false);
        holdDirect(S + "@7", S, 7, item(7), false);
        holdDirect(S + "@8", S, 8, item(8), false);

        // When
        Throwable delivery = send(ex, item(4));
        send(ex, item(9));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(errors()).as("nothing is logged as an error").isEmpty(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A fallback entry of a delivery refused for a failing copy that could not be held

    private SagaExecution<OrderEvent, Order, OrderCommand> started(SagaRunnerConfig config, boolean holds) {
        return startedAt(config, holds, 1);
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> startedAt(SagaRunnerConfig config, boolean holds, long version) {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(config, holds);
        assertThat(send(ex, placed(version))).isNull();
        return ex;
    }

    private void aFailingCopyOfAnotherStream() {
        failing.add("T1");
        holdDirect("t@1", "t", 1, otherStream(1), false);
    }

    @Test
    void a_delivery_listed_as_pending_by_the_fallback_is_not_quarantined_by_its_first_own_failure_past_the_budget() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);
        Throwable refused = send(ex, item(6));
        failing.clear();
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        failing.add("I6");

        // When
        Throwable firstOwnFailure = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the copy with nothing of its own wrong").isNotNull().hasMessageContaining("t@1"),
                () -> assertThat(reactionsTo("T1")).as("the reaction of the copy").isEqualTo(1),
                () -> assertThat(firstOwnFailure).as("the first failure of the event itself is rethrown").isNotNull(),
                () -> assertThat(instance().status()).as("one failure of its own cannot use up a budget").isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_pending_entry_from_the_fallback_never_quarantines_while_the_delivery_is_only_refused_and_the_clock_of_the_copy_decides_once_it_is_held() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        aFailingCopyOfAnotherStream();
        store.failHold.set(2);
        Throwable first = send(ex, item(6));
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));

        // When
        Throwable pastTheBudgetStillNotHeld = send(ex, item(6));
        SagaStatus statusWhileNotHeld = instance().status();
        Throwable pastTheBudgetHeld = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the copy").isNotNull(),
                () -> assertThat(pastTheBudgetStillNotHeld).as("the delivery still cannot be held, so it is refused").isNotNull(),
                () -> assertThat(statusWhileNotHeld).as("the instance status while nothing is held").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(pastTheBudgetHeld).as("held now, so the budget of the copy lets the delivery past").isNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(instance().failure().input()).as("the quarantined input").isEqualTo("t@1"),
                () -> assertThat(heldKeys()).as("the delivery is held").contains(S + "@6")
        );
    }

    @Test
    void a_pending_entry_from_the_fallback_on_a_holding_feed_applies_once_and_clears_when_a_lower_start_arrives() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(8), true, 10);
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);
        Throwable refused = send(ex, item(5));
        List<Long> pendingAfterTheRefusal = pendingVersions();
        failing.clear();
        Throwable redelivery = send(ex, item(5));
        List<String> heldAfterTheRedelivery = heldKeys();

        // When
        Throwable lowerStart = send(ex, placed(3));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the copy").isNotNull(),
                () -> assertThat(pendingAfterTheRefusal).as("the fallback lists the delivery").containsExactly(5L),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(heldAfterTheRedelivery).as("the delivery waits below the start").containsExactly(S + "@5"),
                () -> assertThat(lowerStart).as("the lower start is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I5")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(pendingVersions()).as("the pending entries").isEmpty(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_pending_entry_from_the_fallback_on_a_feed_in_commit_order_applies_in_place_with_no_copy_and_clears() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        send(ex, placed(1));
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);
        Throwable refused = send(ex, item(2));
        List<Long> pendingAfterTheRefusal = pendingVersions();
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the copy").isNotNull(),
                () -> assertThat(pendingAfterTheRefusal).as("the fallback lists the delivery").containsExactly(2L),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(pendingVersions()).as("the pending entries").isEmpty(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void the_fallback_lists_no_pending_entry_for_an_event_below_the_start_that_a_feed_in_commit_order_skips() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        send(ex, placed(10));
        aFailingCopyOfAnotherStream();
        store.failFindWithoutState.set(1);
        store.failHold.set(1);
        Throwable refused = send(ex, item(5));
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the copy").isNotNull().hasMessageContaining("t@1"),
                () -> assertThat(redelivery).as("the redelivery is skipped and acknowledged").isNull(),
                () -> assertThat(reactionsTo("I5")).as("the reaction of the event below the start").isZero(),
                () -> assertThat(pendingVersions()).as("nothing would ever apply the entry, so nothing may list it").isEmpty()
        );
    }

    // A store failure on an event below the start that a feed in commit order skips

    private void theHeldIndexCannotBeReadAfterTheFirstRead() {
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            store.interleaveAt = "afterHeldIndex";
            store.interleaveAction = () -> {
                throw new IllegalStateException("cannot read the held index");
            };
        };
    }

    @Test
    void a_store_failure_lists_no_pending_entry_for_an_event_below_the_start_that_a_feed_in_commit_order_skips() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        send(ex, placed(10));
        theHeldIndexCannotBeReadAfterTheFirstRead();

        // When
        Throwable failed = send(ex, item(5));
        Throwable redelivery = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(failed).as("the store failure refuses the delivery").isNotNull().hasMessageContaining("cannot read the held index"),
                () -> assertThat(redelivery).as("the redelivery is skipped and acknowledged").isNull(),
                () -> assertThat(pendingVersions()).as("the pending entries").isEmpty()
        );
    }

    @Test
    void two_store_failures_a_budget_apart_on_an_event_below_the_start_that_a_feed_in_commit_order_skips_do_not_quarantine_the_instance() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2), false);
        send(ex, placed(10));
        theHeldIndexCannotBeReadAfterTheFirstRead();
        Throwable first = send(ex, item(5));
        Throwable skipped = send(ex, item(5));
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        theHeldIndexCannotBeReadAfterTheFirstRead();

        // When
        Throwable second = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(first).as("the first store failure refuses the delivery").isNotNull(),
                () -> assertThat(skipped).as("the event is skipped and acknowledged in between").isNull(),
                () -> assertThat(second).as("the second store failure refuses the delivery").isNotNull(),
                () -> assertThat(instance().status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE)
        );
    }

    // A replay behind the window whose reads fail

    @Test
    void a_replay_behind_the_window_on_a_feed_in_commit_order_refused_for_a_failing_copy_whose_read_fails_reacts_once() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        send(ex, placed(1));
        for (long v = 2; v <= 6; v++) {
            assertThat(send(ex, item(v))).isNull();
        }
        aFailingCopyOfAnotherStream();
        store.failFindWithoutState.set(1);
        Throwable refused = send(ex, item(2));
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the replay is refused for the copy").isNotNull().hasMessageContaining("t@1"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of an event applied long ago").isEqualTo(1)
        );
    }

    @Test
    void a_replay_behind_the_window_on_a_holding_feed_refused_for_a_failing_copy_whose_read_fails_reacts_once() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        for (long v = 2; v <= 6; v++) {
            assertThat(send(ex, item(v))).isNull();
        }
        aFailingCopyOfAnotherStream();
        store.failFindWithoutState.set(1);
        send(ex, item(2));
        failing.clear();

        // When
        send(ex, item(2));

        // Then
        assertThat(reactionsTo("I2")).as("the reaction of an event applied long ago").isEqualTo(1);
    }

    @Test
    void a_read_failure_for_a_version_the_marks_prove_applied_leaves_one_reaction_and_no_copy_on_a_feed_in_commit_order() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        send(ex, placed(1));
        send(ex, item(5));
        send(ex, item(6));
        aFailingCopyOfAnotherStream();
        store.failFindWithoutState.set(1);
        Throwable refused = send(ex, item(6));
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(refused).as("the replay is refused for the copy").isNotNull(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I6")).as("the reaction of the version").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // An event another deliverer applied and passed before this delivery failed

    @Test
    void on_a_feed_in_commit_order_an_event_another_deliverer_applied_and_passed_does_not_react_again_when_this_delivery_fails_in_evolve() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            for (long v = 2; v <= 5; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
            poisoned.add("I2");
        };
        Throwable first = send(ex, item(2));
        poisoned.clear();

        // When
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery fails in evolve").hasMessageContaining("evolve fails on I2"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the other deliverer reacted and this delivery issued nothing").isEqualTo(1)
        );
    }

    @Test
    void on_a_feed_in_commit_order_an_event_another_deliverer_applied_and_passed_does_not_react_again_when_this_dispatch_throws() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            for (long v = 2; v <= 5; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
            failing.add("I2");
        };
        Throwable first = send(ex, item(2));
        failing.clear();

        // When
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the dispatch throws").hasMessageContaining("fail I2"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the other deliverer reacted and this delivery issued nothing").isEqualTo(1)
        );
    }

    @Test
    void on_a_holding_feed_an_event_another_deliverer_applied_and_passed_reacts_again_when_this_delivery_fails_in_evolve_which_is_the_named_limit_of_not_telling_it_from_one_passed_unapplied() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        store.interleaveAt = "afterFind";
        store.interleaveAction = () -> {
            for (long v = 2; v <= 5; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
            poisoned.add("I2");
        };
        send(ex, item(2));
        poisoned.clear();

        // When
        send(ex, item(2));

        // Then
        assertThat(reactionsTo("I2")).as("the reaction of the other deliverer and the reaction of the redelivery").isEqualTo(2);
    }

    // An event below the start that a feed in commit order skips after its own sweep recorded it as new

    private SagaExecution<OrderEvent, Order, OrderCommand> startedAtTenWithCopiesAtElevenToThirteen() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false, 10);
        holdDirect(S + "@11", S, 11, item(11), false);
        holdDirect(S + "@12", S, 12, item(12), false);
        holdDirect(S + "@13", S, 13, item(13), false);
        return ex;
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_whose_copies_above_it_apply_and_whose_save_is_lost_is_acknowledged_with_no_copy() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAtTenWithCopiesAtElevenToThirteen();
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> {
            store.interleaveAt = "afterRelease";
            store.interleaveAction = () -> store.loseCas.set(1);
        };

        // When
        Throwable delivery = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty(),
                () -> assertThat(reactionsTo("I5")).as("the reaction of the event below the start").isZero(),
                () -> assertThat(reactionsTo("I13")).as("the reaction of the copy dispatched before the save it lost, and again").isEqualTo(2),
                () -> assertThat(errors()).as("nothing is logged as an error").isEmpty()
        );
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_whose_find_fails_on_the_second_attempt_is_acknowledged_with_no_copy() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAtTenWithCopiesAtElevenToThirteen();
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> {
            store.interleaveAt = "afterRelease";
            store.interleaveAction = () -> {
                store.loseCas.set(1);
                store.failFind.set(1);
            };
        };

        // When
        Throwable delivery = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(heldKeys()).as("no copy of the event").doesNotContain(S + "@5")
        );
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_whose_copies_above_it_applied_gets_no_copy_from_a_store_failure_on_the_next_attempt() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAtTenWithCopiesAtElevenToThirteen();
        store.interleaveAt = "afterRelease";
        store.interleaveAction = () -> {
            store.interleaveAt = "afterRelease";
            store.interleaveAction = () -> {
                store.loseCas.set(1);
                theHeldIndexCannotBeReadAfterTheFirstRead();
            };
        };
        Throwable first = send(ex, item(5));
        List<String> heldAfterTheFirst = heldKeys();

        // When
        Throwable redelivery = send(ex, item(5));

        // Then
        assertAll(
                () -> assertThat(first).as("the store failure refuses the delivery").isNotNull().hasMessageContaining("cannot read the held index"),
                () -> assertThat(heldAfterTheFirst).as("no copy of an event the feed skips").doesNotContain(S + "@5"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A feed in commit order, the gate on keeping a delivery, and the clock of an entry the fallback lists

    private void holdItems(long from, long to) {
        for (long v = from; v <= to; v++) {
            holdDirect(S + "@" + v, S, v, item(v), false);
        }
    }

    // The nth read of the held index from here on fails, and nothing before it.
    private void theNthReadOfTheHeldIndexFails(int n) {
        store.interleaveAt = "afterHeldIndex";
        store.interleaveAction = () -> {
            if (n == 1) throw new IllegalStateException("held index unreadable");
            theNthReadOfTheHeldIndexFails(n - 1);
        };
    }

    private void everyReadOfTheHeldIndexFails() {
        store.interleaveAt = "afterHeldIndex";
        store.interleaveAction = () -> {
            everyReadOfTheHeldIndexFails();
            throw new IllegalStateException("held index unreadable");
        };
    }

    private void theHeldIndexReadsAgain() {
        store.interleaveAt = null;
        store.interleaveAction = null;
    }

    private void replaceInstance(SagaFailure failure, Map<String, StreamMarks> marks) {
        SagaEnvelope<Order> e = instance();
        SagaEnvelope<Order> replaced = new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version() + 1, e.timers(), e.streamWatermarks(), null,
                e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), failure, marks, e.startMark());
        assertThat(store.d.compareAndSave(ORDER, replaced, e.version())).isTrue();
    }

    @Test
    void an_event_on_a_feed_in_commit_order_whose_listing_after_a_lowered_start_fails_is_listed_as_pending_and_reacts_once_on_redelivery() {
        // Given a start on stream s at 10, copies of stream t at 3..8 that wait for t@2, and a lower start that makes the attempt list the copies again
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false, 10);
        for (long v = 3; v <= 8; v++) {
            holdDirect("t@" + v, "t", v, otherStream(v), false);
        }
        holdDirect(S + "@5", S, 5, placed(5), true);
        // The list of the attempt, the check for a copy, and the list after the lowered start
        theNthReadOfTheHeldIndexFails(3);

        // When
        Throwable first = send(ex, otherStream(2));
        List<String> heldAfterTheFirst = heldKeys();
        List<Long> pendingVersionsAfterTheFirst = pendingVersions();
        Throwable redelivery = send(ex, otherStream(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused").hasMessageContaining("held index unreadable"),
                () -> assertThat(instance().startMark().version()).as("the lower start applied").isEqualTo(5),
                () -> assertThat(pendingVersionsAfterTheFirst).as("t@2 is listed as pending").contains(2L),
                () -> assertThat(heldAfterTheFirst).as("no copy of t@2 is held").doesNotContain("t@2"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("T2")).as("the reaction of t@2, which nothing applied").isEqualTo(1)
        );
    }

    @Test
    void an_event_on_a_feed_in_commit_order_refused_for_a_failing_copy_of_another_stream_whose_single_hold_failed_reacts_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        holdItems(3, 8);
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);

        // When
        Throwable first = send(ex, item(2));
        failing.clear();
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for the copy").hasMessageContaining("t@1"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of I2, which nothing applied").isEqualTo(1)
        );
    }

    @Test
    void an_event_on_a_feed_in_commit_order_that_another_deliverer_applied_gets_no_copy_from_an_unrelated_copy_of_another_stream_and_reacts_twice() {
        // Given the delivery of I2 dispatches, then loses its save to a deliverer that applies I2 up to I5, and an unrelated copy appears
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        store.interleaveAt = "beforeCas";
        store.interleaveAction = () -> {
            for (long v = 2; v <= 5; v++) {
                assertThat(send(ex, item(v))).isNull();
            }
            holdDirect("t@1", "t", 1, otherStream(1), false);
        };

        // When
        Throwable first = send(ex, item(2));
        List<String> heldAfterTheFirst = heldKeys();
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is overtaken").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("t@1 cannot have passed s@2 and s@2 was applied, so s@2 gets no copy").doesNotContain(S + "@2"),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the dispatch before the lost save and the other deliverer").isEqualTo(2)
        );
    }

    @Test
    void a_converter_that_fails_on_its_second_conversion_does_not_stop_a_reaction_failure_on_a_feed_in_commit_order_being_listed() {
        // Given a converter that throws on the second conversion of I6, which is the one the failure path adds
        AtomicInteger conversions = new AtomicInteger();
        CloudEventConverter<OrderEvent> flaky = new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(OrderEvent e) {
                return converter.toCloudEvent(e);
            }

            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return converter.getCloudEventType(type);
            }

            @Override
            public OrderEvent toDomainEvent(CloudEvent e) {
                if ("I6".equals(e.getId()) && conversions.incrementAndGet() == 2) throw new IllegalStateException("flaky converter");
                return converter.toDomainEvent(e);
            }
        };
        SagaExecution<OrderEvent, Order, OrderCommand> ex = new SagaExecution<>("orders", saga(), store, c -> {
            Reserve r = (Reserve) c;
            if (failing.contains(r.item())) throw new IllegalStateException("fail " + r.item());
            issued.add(r.item());
        }, flaky, SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), e -> true, now::get, false);
        assertThat(send(ex, placed(5))).isNull();
        failing.add("I6");

        // When
        send(ex, item(6));

        // Then
        assertAll(
                () -> assertThat(pendingVersions()).as("the failed version is listed").containsExactly(6L),
                () -> assertThat(errors()).as("no error blames the state store").isEmpty()
        );
    }

    @Test
    void a_delivery_on_a_feed_in_commit_order_that_the_skip_on_a_failed_read_leaves_unkept_is_not_acknowledged_by_a_quarantine_and_reacts_once_on_redelivery() {
        // Given a start at 5, a lower start held at 3, and a failing copy past its budget
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(8), false, 5);
        holdDirect(S + "@3", S, 3, placed(3), true);
        failing.add("T1");
        holdDirect("t@1", "t", 1, otherStream(1), false, now.get().minus(BUDGET).minusSeconds(1));
        store.failFindWithoutState.set(1);

        // When the read of the delivery fails, so the attempt's read, from before the sweep lowered the start, says the event is below it
        Throwable delivery = send(ex, item(4));
        SagaStatus statusAfterTheDelivery = instance().status();
        failing.clear();
        Throwable redelivery = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(instance().startMark().version()).as("the sweep lowered the start").isEqualTo(3),
                () -> assertThat(delivery).as("the delivery has no copy, so quarantine does not acknowledge it").hasMessageContaining("t@1"),
                () -> assertThat(statusAfterTheDelivery).as("the instance is not quarantined on that answer").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of I4").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void the_fallback_keeps_the_clock_a_legacy_failed_version_already_has() {
        // Given a version that first failed at T0 and is kept as a failure of unknown type
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        Instant firstFailedAt = now.get();
        StreamMarks marks = instance().streamMarks().get(S);
        replaceInstance(null, Map.of(S, new StreamMarks(marks.floor(), marks.byType(), marks.pending(), List.of(6L), Map.of(6L, firstFailedAt))));
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);
        assertThat(send(ex, item(6))).as("refused for the copy").isNotNull();
        failing.clear();
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        failing.add("I6");

        // When its own failure comes past the budget from T0
        send(ex, item(6));

        // Then
        assertThat(instance().status()).as("it has been failing since T0").isEqualTo(SagaStatus.QUARANTINED);
    }

    @Test
    void the_fallback_keeps_the_clock_of_a_failure_record_that_another_input_then_replaces() {
        // Given a failure record from T0 that names s@6, which is not pending, as a record from before pending or at the cap leaves it
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(2));
        Instant firstFailedAt = now.get();
        replaceInstance(new SagaFailure(S + "@6", null, firstFailedAt, "x", "m"), instance().streamMarks());
        aFailingCopyOfAnotherStream();
        store.failHold.set(1);
        assertThat(send(ex, item(6))).as("refused for the copy").isNotNull();
        failing.clear();
        failing.add("I7");
        send(ex, item(7));
        failing.clear();
        now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        failing.add("I6");

        // When
        send(ex, item(6));

        // Then
        assertThat(instance().status()).as("s@6 has been failing since T0").isEqualTo(SagaStatus.QUARANTINED);
    }

    // Named limits of the gate on keeping a delivery and of the store failure path

    @Test
    void an_event_behind_the_window_on_a_feed_in_commit_order_that_copies_held_before_the_deploy_passed_is_acknowledged_with_a_warning_and_does_not_react_which_is_the_named_limit_of_copies_held_before_the_deploy() {
        // Given copies at 3..8 held before the subscription declared commit order, and a delivery of another stream that sweeps them
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false);
        holdItems(3, 8);
        assertThat(send(ex, otherStream(1))).isNull();

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(warnings()).as("a warning says the event was passed").isNotEmpty(),
                () -> assertThat(errors()).as("nothing is logged as an error").isEmpty(),
                () -> assertThat(heldKeys()).as("the copies").isEmpty(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the event the copies passed").isZero()
        );
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_is_refused_while_the_held_index_cannot_be_read_and_acknowledged_once_it_can_which_is_the_named_limit_of_the_store_failure_path() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET).withReorderWindow(8), false, 5);
        everyReadOfTheHeldIndexFails();

        // When the index fails past the budget more than once
        List<Throwable> refusals = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            refusals.add(send(ex, item(4)));
            now.updateAndGet(t -> t.plus(BUDGET).plusSeconds(1));
        }
        SagaStatus statusWhileItFailed = instance().status();
        theHeldIndexReadsAgain();
        Throwable afterwards = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(refusals).as("every delivery is refused").allSatisfy(t -> assertThat(t).hasMessageContaining("held index unreadable")),
                () -> assertThat(statusWhileItFailed).as("the instance is not quarantined").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(afterwards).as("the delivery is acknowledged once the index reads again").isNull(),
                () -> assertThat(instance().status()).as("the instance stays active").isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_holding_delivery_whose_waiting_copy_its_sweep_applied_reacts_once_when_a_later_copy_fails_and_the_read_fails() {
        // Given a start at 5 and I4 held below it, then a lower start and copies above I4, then a failing copy
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), true, 5);
        assertThat(send(ex, item(4))).isNull();
        holdDirect(S + "@3", S, 3, placed(3), true);
        holdItems(6, 8);
        aFailingCopyOfAnotherStream();
        store.failFindWithoutState.set(1);

        // When the redelivery of I4 sweeps
        Throwable first = send(ex, item(4));
        long reactionsAfterTheSweep = reactionsTo("I4");
        failing.clear();
        Throwable redelivery = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused for t@1").hasMessageContaining("t@1"),
                () -> assertThat(reactionsAfterTheSweep).as("the sweep applied the waiting copy").isEqualTo(1),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the sweep, which the marks prove to the redelivery").isEqualTo(1)
        );
    }

    @Test
    void an_event_below_the_start_on_a_feed_in_commit_order_with_a_lower_start_held_applies_once_on_its_redelivery_after_a_store_failure() {
        // Given a start at 5 and a copy of a lower start held before commit order
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(8), false, 5);
        holdDirect(S + "@3", S, 3, placed(3), true);
        theNthReadOfTheHeldIndexFails(1);

        // When
        Throwable first = send(ex, item(4));
        List<Long> pendingAfterTheFirst = pendingVersions();
        Throwable redelivery = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(first).as("the store failure refuses the delivery").hasMessageContaining("held index unreadable"),
                () -> assertThat(pendingAfterTheFirst).as("nothing is listed").isEmpty(),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the reaction of I4").isEqualTo(1),
                () -> assertThat(pendingVersions()).as("no pending entry is left").isEmpty()
        );
    }

    // A driver below a copy on its own stream

    @Test
    void a_driver_below_a_copy_on_its_stream_that_failed_is_applied_before_it_when_one_hold_fails_with_a_window_of_2() {
        // Given copies at 3 to 8 held before the start, the one at 3 failing, and one hold that fails
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        for (long v = 3; v <= 8; v++) {
            send(ex, item(v));
        }
        failing.add("I3");
        assertThat(send(ex, placed(1))).as("the start is refused for the copy at 3").isNotNull();
        failing.remove("I3");
        store.failHold.set(1);

        // When
        Throwable first = send(ex, item(2));
        Throwable second = send(ex, item(2));
        Throwable third = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the first delivery").isNull(),
                () -> assertThat(second).as("the second delivery").isNull(),
                () -> assertThat(third).as("the third delivery").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the driver").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_driver_below_a_copy_on_its_stream_that_failed_is_applied_before_it_when_one_hold_fails_with_the_default_window() {
        // Given a copy at 300 held before the start and failing, and one hold that fails
        SagaExecution<OrderEvent, Order, OrderCommand> ex = ex(SagaRunnerConfig.defaults().disableQuarantine());
        send(ex, item(300));
        failing.add("I300");
        assertThat(send(ex, placed(1))).as("the start is refused for the copy at 300").isNotNull();
        failing.remove("I300");
        store.failHold.set(1);

        // When
        Throwable first = send(ex, item(2));
        Throwable second = send(ex, item(2));
        Throwable third = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(first).as("the first delivery").isNull(),
                () -> assertThat(second).as("the second delivery").isNull(),
                () -> assertThat(third).as("the third delivery").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the driver").isEqualTo(1),
                () -> assertThat(reactionsTo("I300")).as("the reaction of the copy").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // Copies above the delivery apply once it is decided, whichever way it is decided

    private List<String> reactionsOf(String... items) {
        Set<String> wanted = Set.of(items);
        return issued.stream().filter(wanted::contains).toList();
    }

    @Test
    void a_copy_above_a_delivery_that_is_its_own_copy_applies_after_it_in_the_same_delivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect(S + "@2", S, 2, item(2), false);
        holdDirect(S + "@3", S, 3, item(3), false);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(reactionsOf("I2", "I3")).as("the delivery's copy, then the copy above it").containsExactly("I2", "I3"),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_copy_above_a_delivery_below_the_start_on_a_feed_in_commit_order_applies_in_the_same_delivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false, 5);
        holdDirect(S + "@6", S, 6, item(6), false);

        // When
        Throwable delivery = send(ex, item(4));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I4")).as("the event below the start is skipped").isZero(),
                () -> assertThat(reactionsTo("I6")).as("the copy above it").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_copy_above_a_delivery_behind_the_reorder_window_applies_in_the_same_delivery() {
        // Given versions up to 6 applied under a window of 1, and a copy at 7
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(1));
        for (long v = 3; v <= 6; v++) {
            send(ex, item(v));
        }
        holdDirect(S + "@7", S, 7, item(7), false);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(warnings()).as("a warning says the delivery is behind the window").isNotEmpty(),
                () -> assertThat(reactionsTo("I2")).as("the delivery behind the window is skipped").isZero(),
                () -> assertThat(reactionsTo("I7")).as("the copy above it").isEqualTo(1),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_copy_above_a_delivery_that_is_saved_applies_after_it_in_the_same_delivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect(S + "@3", S, 3, item(3), false);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(reactionsOf("I2", "I3")).as("the delivery, then the copy above it").containsExactly("I2", "I3"),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A save that is lost while a waiting copy applies

    @Test
    void a_lost_save_while_a_copy_above_the_delivery_applies_retries_the_body_and_applies_the_copy_in_the_same_delivery() {
        // Given a copy at 3 whose apply save loses once, which is the second save of the delivery
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect(S + "@3", S, 3, item(3), false);
        store.interleaveAt = "beforeCas";
        store.interleaveAction = () -> {
            store.interleaveAt = "beforeCas";
            store.interleaveAction = () -> store.loseCas.set(1);
        };

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged after the retry").isNull(),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the delivery, which the retry finds applied").isEqualTo(1),
                () -> assertThat(instance().state().items()).as("the copy is applied").contains("I3"),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A start that moves while the copies up to the delivery apply

    @Test
    void a_copy_below_the_start_that_the_first_list_left_out_applies_in_the_delivery_that_a_copy_up_to_it_lowered_the_start_for() {
        // Given a start at 5, a start copy at 2 and a copy at 4 that waits below the start
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(3), true, 5);
        holdDirect(S + "@2", S, 2, placed(2), true);
        holdDirect(S + "@4", S, 4, item(4), false);

        // When
        Throwable delivery = send(ex, item(3));
        List<String> heldAfterTheDelivery = heldKeys();

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(instance().startMark().version()).as("the start copy lowered the start").isEqualTo(2),
                () -> assertThat(heldAfterTheDelivery).as("the copy that waited is applied by the same delivery").isEmpty(),
                () -> assertThat(reactionsOf("P2", "I3", "I4")).as("the start copy, the delivery, then the copy above it").containsExactly("P2", "I3", "I4")
        );
    }

    // A delivery that cannot be listed as pending on a feed in commit order, with a copy of its own stream held

    private SagaExecution<OrderEvent, Order, OrderCommand> failingOnAFeedInCommitOrderWithACopyAboveIt() {
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), false, 1);
        holdDirect(S + "@4", S, 4, item(4), false);
        failing.add("I2");
        return ex;
    }

    @Test
    void a_failure_that_kept_changing_while_listed_logs_an_error_and_stores_no_copy_on_a_feed_in_commit_order_with_a_copy_of_its_stream_held() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = failingOnAFeedInCommitOrderWithACopyAboveIt();
        store.loseWithoutState.set(100);

        // When
        Throwable thrown = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(thrown).as("the failure of the reaction is rethrown").hasMessageContaining("fail I2"),
                () -> assertThat(errors()).as("the error says the instance kept changing").anyMatch(e -> e.contains("kept changing")),
                () -> assertThat(heldKeys()).as("only the copy that was held before").containsExactly(S + "@4")
        );
    }

    @Test
    void a_failure_at_the_pending_cap_logs_an_error_and_stores_no_copy_on_a_feed_in_commit_order_with_a_copy_of_its_stream_held() {
        // Given an instance that lists as many pending versions as it may
        SagaExecution<OrderEvent, Order, OrderCommand> ex = failingOnAFeedInCommitOrderWithACopyAboveIt();
        Map<String, StreamMarks> marks = new HashMap<>(instance().streamMarks());
        marks.put("other-stream", new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of("other.Type", LongStream.rangeClosed(1, SagaExecutionSupport.PENDING_CAP).boxed().toList())));
        replaceInstance(null, marks);

        // When
        Throwable thrown = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(thrown).as("the failure of the reaction is rethrown").hasMessageContaining("fail I2"),
                () -> assertThat(errors()).as("the error says the instance lists the most it lists").anyMatch(e -> e.contains("the most it lists")),
                () -> assertThat(heldKeys()).as("only the copy that was held before").containsExactly(S + "@4")
        );
    }

    @Test
    void a_failure_whose_listing_failed_in_the_store_logs_an_error_and_stores_no_copy_on_a_feed_in_commit_order_with_a_copy_of_its_stream_held() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = failingOnAFeedInCommitOrderWithACopyAboveIt();
        store.failFindWithoutState.set(1);

        // When
        Throwable thrown = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(thrown).as("the failure of the reaction is rethrown").hasMessageContaining("fail I2"),
                () -> assertThat(errors()).as("the error says the state store failed").anyMatch(e -> e.contains("the state store failed")),
                () -> assertThat(heldKeys()).as("only the copy that was held before").containsExactly(S + "@4")
        );
    }

    // The copies that apply before the driver

    @Test
    void a_copy_of_another_stream_with_a_higher_version_applies_before_the_driver_and_a_copy_above_it_on_its_own_stream_waits() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = started(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2));
        holdDirect("t@5", "t", 5, otherStream(5), false);
        holdDirect(S + "@3", S, 3, item(3), false);

        // When
        Throwable delivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(delivery).as("the delivery is acknowledged").isNull(),
                () -> assertThat(reactionsOf("T5", "I2", "I3")).as("the copy of the other stream, the driver, then the copy above it").containsExactly("T5", "I2", "I3"),
                () -> assertThat(heldKeys()).as("nothing is left held").isEmpty()
        );
    }

    // A copy applied behind the window whose release failed, and a lower pending version of its type that saves

    private void aCopyAppliedBehindTheWindowWhoseReleaseFailedDoesNotApplyAgainWhenALowerPendingVersionSaves(boolean holds) {
        // Given I2 listed as pending, I4 to I10 applied, and a copy at 3 that is behind the window
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startedAt(SagaRunnerConfig.defaults().disableQuarantine().withReorderWindow(2), holds, 1);
        failing.add("I2");
        assertThat(send(ex, item(2))).as("I2 is refused and listed as pending").isNotNull();
        failing.remove("I2");
        for (long v = 4; v <= 10; v++) {
            send(ex, item(v));
        }
        holdDirect(S + "@3", S, 3, item(3), false);
        store.failRelease.set(1);

        // When
        Throwable refused = send(ex, otherStream(1));
        Throwable redelivery = send(ex, item(2));

        // Then
        assertAll(
                () -> assertThat(refused).as("the delivery is refused for the release").isNotNull(),
                () -> assertThat(redelivery).as("the redelivery of I2 is acknowledged").isNull(),
                () -> assertThat(reactionsTo("I3")).as("the copy is not applied again when I2 saves").isEqualTo(1),
                () -> assertThat(reactionsTo("I2")).as("the reaction of the pending version").isEqualTo(1),
                () -> assertThat(heldKeys()).as("the copy is no longer held").doesNotContain(S + "@3")
        );
    }

    @Test
    void a_copy_applied_behind_the_window_whose_release_failed_is_not_applied_again_when_a_lower_pending_version_saves_on_a_holding_feed() {
        aCopyAppliedBehindTheWindowWhoseReleaseFailedDoesNotApplyAgainWhenALowerPendingVersionSaves(true);
    }

    @Test
    void a_copy_applied_behind_the_window_whose_release_failed_is_not_applied_again_when_a_lower_pending_version_saves_on_a_feed_in_commit_order() {
        aCopyAppliedBehindTheWindowWhoseReleaseFailedDoesNotApplyAgainWhenALowerPendingVersionSaves(false);
    }
}
