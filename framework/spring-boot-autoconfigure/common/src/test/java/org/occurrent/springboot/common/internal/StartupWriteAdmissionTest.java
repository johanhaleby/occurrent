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
package org.occurrent.springboot.common.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class StartupWriteAdmissionTest {

    private static final long WAIT_SECONDS = 10;

    @Test
    void admits_no_write_until_it_is_opened_and_every_write_after() {
        StartupWriteAdmission admission = new StartupWriteAdmission();

        assertThat(admission.admitsWrite()).as("[before open()]").isFalse();

        admission.open();

        assertThat(admission.admitsWrite()).as("[after open()]").isTrue();
    }

    @Test
    void refuse_if_open_throws_only_once_the_gate_is_open() {
        StartupWriteAdmission admission = new StartupWriteAdmission();

        assertThatCode(() -> admission.refuseIfOpen("receiver", StartupWriteAdmissionTest::handler)).as("[before open()]").doesNotThrowAnyException();

        admission.open();

        assertThatThrownBy(() -> admission.refuseIfOpen("receiver", StartupWriteAdmissionTest::handler))
                .as("[after open()]")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bean 'receiver' declares the synchronous handler");
    }

    @Test
    void refuse_if_open_does_not_ask_for_the_handler_while_the_gate_is_closed() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        AtomicBoolean asked = new AtomicBoolean();

        admission.refuseIfOpen("receiver", () -> {
            asked.set(true);
            return null;
        });

        assertThat(asked).as("[handler asked for]").isFalse();
    }

    @Test
    void refuse_if_open_does_not_refuse_when_the_gate_is_open_and_the_handler_is_no_longer_unregistered() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.open();

        assertThatCode(() -> admission.refuseIfOpen("receiver", () -> null)).doesNotThrowAnyException();
    }

    @Test
    void run_while_closed_runs_the_hand_over_and_returns_true_while_the_gate_is_closed() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        AtomicBoolean ran = new AtomicBoolean();

        boolean handedOver = admission.runWhileClosed(() -> ran.set(true));

        assertThat(handedOver).as("[returned]").isTrue();
        assertThat(ran).as("[hand-over ran]").isTrue();
        assertThat(admission.admitsWrite()).as("[gate after the hand-over]").isFalse();
    }

    @Test
    void run_while_closed_does_not_run_the_hand_over_and_returns_false_once_the_gate_is_open() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.open();
        AtomicBoolean ran = new AtomicBoolean();

        boolean handedOver = admission.runWhileClosed(() -> ran.set(true));

        assertThat(handedOver).as("[returned]").isFalse();
        assertThat(ran).as("[hand-over ran]").isFalse();
    }

    @Test
    void open_if_nothing_left_stays_closed_while_the_supplier_says_something_is_left() {
        StartupWriteAdmission admission = new StartupWriteAdmission();

        boolean opened = admission.openIfNothingLeft(() -> false);

        assertThat(opened).as("[returned]").isFalse();
        assertThat(admission.admitsWrite()).as("[gate]").isFalse();
    }

    @Test
    void open_if_nothing_left_opens_the_gate_once_the_supplier_says_nothing_is_left() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        AtomicBoolean somethingLeft = new AtomicBoolean(true);

        assertThat(admission.openIfNothingLeft(() -> !somethingLeft.get())).as("[returned while something is left]").isFalse();
        somethingLeft.set(false);

        assertThat(admission.openIfNothingLeft(() -> !somethingLeft.get())).as("[returned once nothing is left]").isTrue();
        assertThat(admission.admitsWrite()).as("[gate]").isTrue();
    }

    @Test
    void a_hand_over_in_progress_keeps_open_if_nothing_left_on_another_thread_waiting_until_it_returns() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        List<String> order = new CopyOnWriteArrayList<>();
        CountDownLatch handOverStarted = new CountDownLatch(1);
        CountDownLatch handOverMayReturn = new CountDownLatch(1);
        Thread handingOver = new Thread(() -> admission.runWhileClosed(() -> {
            handOverStarted.countDown();
            await(handOverMayReturn);
            order.add("hand-over returned");
        }), "hand-over");
        Thread opening = new Thread(() -> admission.openIfNothingLeft(() -> {
            order.add("nothing left read");
            return true;
        }), "opening");
        try {
            handingOver.start();
            assertThat(handOverStarted.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("[hand-over started]").isTrue();

            opening.start();
            awaitState(opening, Thread.State.BLOCKED);

            assertThat(admission.admitsWrite()).as("[gate while the hand-over is in progress]").isFalse();
            assertThat(order).as("[events while the hand-over is in progress]").isEmpty();

            handOverMayReturn.countDown();
            handingOver.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            opening.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

            assertThat(order).as("[order]").containsExactly("hand-over returned", "nothing left read");
            assertThat(admission.admitsWrite()).as("[gate after the hand-over returned]").isTrue();
        } finally {
            handOverMayReturn.countDown();
        }
    }

    @Test
    void a_counted_registration_keeps_open_if_nothing_left_on_another_thread_waiting_until_it_ends() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        List<String> order = new CopyOnWriteArrayList<>();
        assertThat(admission.registrationStarts()).as("[registration counted]").isTrue();
        Thread opening = new Thread(() -> admission.openIfNothingLeft(() -> {
            order.add("nothing left read");
            return true;
        }), "opening");
        try {
            opening.start();
            awaitState(opening, Thread.State.WAITING);

            assertThat(admission.admitsWrite()).as("[gate while the registration is counted]").isFalse();
            assertThat(order).as("[events while the registration is counted]").isEmpty();

            order.add("registration ended");
            admission.registrationEnds();
            opening.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

            assertThat(order).as("[order]").containsExactly("registration ended", "nothing left read");
            assertThat(admission.admitsWrite()).as("[gate after the registration ended]").isTrue();
        } finally {
            opening.interrupt();
        }
    }

    @Test
    void every_counted_registration_has_to_end_before_open_if_nothing_left_reads_the_supplier() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        AtomicInteger endedWhenRead = new AtomicInteger(-1);
        AtomicInteger ended = new AtomicInteger();
        admission.registrationStarts();
        admission.registrationStarts();
        Thread opening = new Thread(() -> admission.openIfNothingLeft(() -> {
            endedWhenRead.set(ended.get());
            return true;
        }), "opening");
        try {
            opening.start();
            awaitState(opening, Thread.State.WAITING);

            ended.incrementAndGet();
            admission.registrationEnds();
            ended.incrementAndGet();
            admission.registrationEnds();
            opening.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

            assertThat(endedWhenRead).as("[registrations ended when the supplier was read]").hasValue(2);
            assertThat(admission.admitsWrite()).as("[gate]").isTrue();
        } finally {
            opening.interrupt();
        }
    }

    @Test
    void registration_starts_returns_false_and_counts_nothing_once_the_gate_is_open() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.open();

        boolean counted = admission.registrationStarts();

        assertThat(counted).as("[registration counted]").isFalse();
        AtomicBoolean opened = new AtomicBoolean();
        Thread opening = new Thread(() -> opened.set(admission.openIfNothingLeft(() -> true)), "opening");
        try {
            opening.start();
            opening.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

            assertThat(opening.isAlive()).as("[open_if_nothing_left still waiting]").isFalse();
            assertThat(opened).as("[open_if_nothing_left returned]").isTrue();
        } finally {
            opening.interrupt();
        }
    }

    @Test
    void an_interrupt_while_open_if_nothing_left_waits_for_a_registration_throws_and_keeps_the_interrupt_flag() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.registrationStarts();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread opening = new Thread(() -> {
            try {
                admission.openIfNothingLeft(() -> true);
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        }, "opening");
        opening.start();
        awaitState(opening, Thread.State.WAITING);

        opening.interrupt();
        opening.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

        assertThat(thrown.get()).as("[thrown]").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Interrupted while waiting for another thread to finish registering a handler")
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(interruptedAfter).as("[interrupt flag after the throw]").isTrue();
        assertThat(admission.admitsWrite()).as("[gate]").isFalse();
    }

    @Test
    void the_refusal_names_the_bean_and_the_handler() {
        IllegalStateException refusal = StartupWriteAdmission.refusal("lateReceiver", handler());

        assertThat(refusal).hasMessageContaining("Bean 'lateReceiver' declares the synchronous handler " + Receiver.class.getName() + "#on")
                .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for a latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    // Spins on the thread state instead of sleeping, so it returns as soon as the thread is parked on the monitor
    private static void awaitState(Thread thread, Thread.State state) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (thread.getState() != state) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Thread " + thread.getName() + " is " + thread.getState() + ", expected " + state);
            }
            Thread.onSpinWait();
        }
    }

    private static Method handler() {
        try {
            return Receiver.class.getDeclaredMethod("on");
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    static class Receiver {
        void on() {
        }
    }
}
