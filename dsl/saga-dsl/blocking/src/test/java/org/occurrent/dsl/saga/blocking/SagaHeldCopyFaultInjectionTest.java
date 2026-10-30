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
import java.util.concurrent.atomic.AtomicBoolean;
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
    void a_delivery_that_read_the_instance_as_new_and_whose_own_sweep_moves_its_type_past_it_is_refused_and_held_and_runs_once_on_redelivery() {
        // Given
        SagaExecution<OrderEvent, Order, OrderCommand> ex = startAtFiveWithCopiesAtTenAndElevenOfWhichTheCopyAtTenFailed();

        // When
        Throwable first = send(ex, otherStream(8));
        List<String> heldAfterTheFirst = store.d.heldIndex(ORDER).stream().map(HeldEventRef::key).toList();
        Throwable replay = send(ex, otherStream(8));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused so that nothing acknowledges it unapplied").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("a copy of the delivery is held").contains("t@8"),
                () -> assertThat(replay).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(issued.stream().filter("T8"::equals).count()).as("the reaction of the delivery").isEqualTo(1),
                () -> assertThat(issued.stream().filter("T11"::equals).count()).as("the reaction of the copy the sweep applied").isEqualTo(1),
                () -> assertThat(store.d.heldIndex(ORDER)).as("nothing is left held").isEmpty()
        );
    }

    @Test
    void a_delivery_that_read_the_instance_as_new_is_held_and_runs_once_on_redelivery_when_a_later_copy_of_its_own_sweep_fails() {
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
                () -> assertThat(first).as("the delivery is refused for the copy that fails").isNotNull(),
                () -> assertThat(heldAfterTheFirst).as("a copy of the delivery is held").contains("t@8"),
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
        assertThat(send(ex, item(4))).as("the item at 4 is refused and kept").isNotNull();
        assertThat(send(ex, placed(3))).as("the start at 3 is refused and kept").isNotNull();
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
    void an_event_on_a_feed_in_commit_order_that_a_copy_held_before_the_deploy_passes_by_the_window_is_refused_with_an_error_and_never_reacts() {
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
        List<String> errorsAfterTheFirst = errors();
        Throwable redelivery = send(commitOrder, otherStream(8));

        // Then
        assertAll(
                () -> assertThat(first).as("the delivery is refused since its own sweep passed it by the window").isNotNull(),
                () -> assertThat(errorsAfterTheFirst).as("the ERROR names the event").anySatisfy(message -> assertThat(message).contains("t@8")),
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(reactionsTo("T8")).as("the reaction of the event, which never ran").isZero(),
                () -> assertThat(reactionsTo("T11")).as("the reaction of the copy the sweep applied").isEqualTo(1)
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
        store.failFindWithoutState.set(1);

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
}
