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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription whose lock another call holds when a start(..) or stop() is applied to it is handed over to a thread
 * of its own. Once that thread is done, the subscription is where it would be had each start(..) and stop() found its
 * lock free, whether this model or the user paused it and whether the wrapped model runs included. That holds for
 * every order of two and of three calls, from a running and from a stopped model, both when the later calls come while
 * the lock is still held and when they come while that thread applies the first.
 */
class CompetingConsumerHandedOverLifecycleTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

    @TestFactory
    Stream<DynamicTest> a_subscription_handed_over_ends_where_it_would_with_its_lock_free() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Initially initially : Initially.values()) {
            for (List<Call> calls : everyOrderOfTwoAndOfThreeCalls()) {
                for (When when : When.values()) {
                    String described = calls.stream().map(call -> call.description).collect(Collectors.joining(", then "));
                    String name = "from a " + initially.description + " model, " + described + " " + when.description;
                    tests.add(DynamicTest.dynamicTest(name, () -> {
                        State expected = withTheLockFree(initially, calls);
                        State actual = handedOver(initially, calls, when);
                        assertThat(actual).as("[s1 once the thread it was handed over to is done, from a %s model, %s %s]",
                                initially.description, described, when.description).isEqualTo(expected);
                    }));
                }
            }
        }
        return tests.stream();
    }

    private static List<List<Call>> everyOrderOfTwoAndOfThreeCalls() {
        List<List<Call>> orders = new ArrayList<>();
        for (Call first : Call.values()) {
            for (Call second : Call.values()) {
                orders.add(List.of(first, second));
                for (Call third : Call.values()) {
                    orders.add(List.of(first, second, third));
                }
            }
        }
        return orders;
    }

    // A stop() that begins after the thread s1 was handed to has failed to apply start(true) to it, and finds the lock
    // of s1 free, applies that start(true) itself before pausing s1
    @Test
    void a_stop_that_applies_a_start_handed_over_before_it_pauses_the_subscription_as_by_the_user() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            Gate backingOff = fixture.handedOverThreadAboutToBackOff();
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            // A RuntimeException would go to the try of s1 and count as applied, so the thread s1 is handed to gets an
            // Error, and then stands before its backoff with the lock of s1 free
            fixture.wrapped.errorsFromIsRunningOfS1OnALifecycleThread.set(1);
            fixture.model.start(true);
            grantHoldingTheLock.open();
            assertThat(backingOff.awaitEntered()).as("the thread s1 was handed to failed and let go of the lock").isTrue();

            fixture.model.stop();
            fixture.model.start(false);
            backingOff.open();
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    // A grant that took the lock of s1 before stop() began comes before that stop(), also when a start(false) after
    // the stop() has begun by the time the grant runs s1
    @Test
    void a_grant_holding_the_lock_when_stop_begins_comes_before_that_stop() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.strategy.leaseHeldAfterTheGate.set(true);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.model.start(true);
            fixture.model.stop();
            fixture.model.start(false);
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once the grant, start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    // A grant that took the lock of s1 before stop() began comes before that stop(), also when s1 was paused for the
    // lease it had lost, so the stop() pauses s1 as by the user
    @Test
    void a_grant_holding_the_lock_of_a_subscription_paused_for_its_lost_lease_when_stop_begins_comes_before_that_stop() {
        Fixture free = new Fixture(Initially.RUNNING);
        State expected;
        try {
            free.loseTheLeaseOfS1();
            free.strategy.holders.add("s1");
            free.model.onConsumeGranted("s1", NODE);
            free.model.stop();
            awaitNothingLeftForS1();
            expected = free.state();
        } finally {
            free.model.shutdown();
        }
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.loseTheLeaseOfS1();
            fixture.strategy.holders.add("s1");
            fixture.strategy.leaseHeldAfterTheGate.set(true);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.model.stop();
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("[s1 once the grant and stop() are applied]").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A grant that took the lock of s1 before stop() began comes before that stop(), also when s1 was waiting for its
    // lease, so the wrapped model holds s1 paused and the stop() pauses s1 as by the user
    @Test
    void a_grant_holding_the_lock_of_a_subscription_waiting_for_its_lease_when_stop_begins_comes_before_that_stop() {
        Fixture free = new Fixture(Initially.RUNNING, true);
        Made expected;
        try {
            free.strategy.anotherNodeHoldsS1.set(false);
            free.strategy.holders.add("s1");
            free.model.onConsumeGranted("s1", NODE);
            free.model.stop();
            awaitNothingLeftForS1();
            expected = free.made();
        } finally {
            free.model.shutdown();
        }
        Fixture fixture = new Fixture(Initially.RUNNING, true);
        try {
            fixture.strategy.anotherNodeHoldsS1.set(false);
            fixture.strategy.holders.add("s1");
            fixture.strategy.leaseHeldAfterTheGate.set(true);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.model.stop();
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.made()).as("[s1, and whether the wrapped model holds it paused, once the grant and stop() are applied]").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume that took the lock of s1 before stop() began comes before that stop(), also when another node takes the
    // lease once stop() has returned, while the resume still registers s1. So the stop() pauses s1 as by the user, and
    // s1 does not compete for its lease after start(false).
    @Test
    void a_resume_registering_a_subscription_when_stop_begins_comes_before_that_stop() {
        Fixture free = new Fixture(Initially.RUNNING);
        Ended expected;
        try {
            free.model.pauseSubscription("s1");
            free.model.resumeSubscription("s1");
            free.model.stop();
            free.strategy.anotherNodeHoldsS1.set(true);
            awaitNothingLeftForS1();
            expected = new Ended(free.state(), free.strategy.candidates.contains("s1"));
        } finally {
            free.model.shutdown();
        }
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.model.pauseSubscription("s1");
            Gate resumeHoldingTheLock = new Gate();
            fixture.strategy.nextRegisterOfS1OnTheTestThread.set(resumeHoldingTheLock);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeHoldingTheLock.awaitEntered()).as("the resume of s1 holds its lock while it registers s1").isTrue();

            fixture.model.stop();
            fixture.strategy.anotherNodeHoldsS1.set(true);
            resumeHoldingTheLock.open();
            assertThat(resume).as("the resume of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(new Ended(fixture.state(), fixture.strategy.candidates.contains("s1")))
                    .as("[s1, and whether it competes for its lease, once the resume, stop() and start(false) are applied]").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A try that took the lock of s1 before stop() began, to pause s1 for the lease it lost, comes before that stop(),
    // so s1 stays paused for its lease rather than as by the user, and start(false) has it compete for its lease again
    @Test
    void a_try_pausing_a_subscription_for_its_lost_lease_when_stop_begins_comes_before_that_stop() {
        Fixture free = new Fixture(Initially.RUNNING);
        State expected;
        try {
            free.loseTheLeaseOfS1();
            free.model.stop();
            awaitNothingLeftForS1();
            expected = free.state();
        } finally {
            free.model.shutdown();
        }
        Fixture fixture = new Fixture(Initially.RUNNING);
        Gate resumeHoldingTheLock = new Gate();
        Gate pauseOnATry = new Gate();
        try {
            // A resume of s1, which runs, holds its lock, so the loss of the lease is left to a try
            fixture.wrapped.nextIsRunningOfS1OnTheTestThread.set(resumeHoldingTheLock);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeHoldingTheLock.awaitEntered()).as("the resume of s1 holds its lock").isTrue();
            fixture.wrapped.nextPauseOfS1OnATry.set(pauseOnATry);
            fixture.loseTheLeaseOfS1();
            resumeHoldingTheLock.open();
            assertThat(resume).as("the resume of s1, which runs").failsWithin(EVENTUALLY);
            assertThat(pauseOnATry.awaitEntered()).as("a try holds the lock of s1 while it pauses s1").isTrue();

            fixture.model.stop();
            pauseOnATry.open();
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("[s1 once the try and stop() are applied]").isEqualTo(expected);
        } finally {
            resumeHoldingTheLock.open();
            pauseOnATry.open();
            fixture.model.shutdown();
        }
    }

    // A failure of a start(..) handed over is tried again by the thread it was handed to, also when a later start(..)
    // finds the lock free first and fails to apply it
    @Test
    void a_start_handed_over_for_a_subscription_that_does_not_compete_is_tried_until_it_is_applied() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            fixture.model.subscribe(NODE, "n1", null, StartAt.dynamic(__ -> null), __ -> {
            });
            Gate backingOff = fixture.handedOverThreadAboutToBackOff();
            Gate pauseHoldingTheLock = new Gate();
            fixture.wrapped.nextIsPausedOfN1OnTheTestThread.set(pauseHoldingTheLock);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("n1"));
            assertThat(pauseHoldingTheLock.awaitEntered()).as("the pause of n1 holds its lock").isTrue();
            // Fails once on the thread n1 is handed to, which then stands before its backoff with the lock of n1 free,
            // and twice more, so the second start(true) fails to apply the first and, unless it hands both back to that
            // thread, its own
            fixture.wrapped.failuresFromResumingN1.set(3);
            fixture.model.start(true);
            pauseHoldingTheLock.open();
            assertThat(pause).as("the pause of n1").failsWithin(EVENTUALLY);
            assertThat(backingOff.awaitEntered()).as("the thread n1 was handed to failed and let go of the lock").isTrue();

            Throwable thrownBySecondStart = catchThrowable(() -> fixture.model.start(true));
            assertThat(fixture.wrapped.resumeFailuresOfN1.awaitCount(2)).as("the second start(true) failed to apply the first").isTrue();
            backingOff.open();

            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(fixture.wrapped.isRunning("n1")).as("n1 runs in the wrapped model").isTrue());
            assertThat(thrownBySecondStart).as("what the second start(true) threw").isNull();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume that finds a start(true) handed over for a subscription that does not compete still failing is made all
    // the same, and replaces that start(true), so the thread it was handed to ends without trying it again
    @Test
    void a_resume_that_finds_a_start_handed_over_still_failing_is_made_and_replaces_it() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            // Fails once on the thread n1 is handed to and once when the resume tries the start(true) first, and the
            // resume's own call then succeeds
            Gate backingOff = fixture.handOverAStartOfN1ThatFails(2);

            Throwable thrownByResume = catchThrowable(() -> fixture.model.resumeSubscription("n1"));
            backingOff.open();

            assertThat(thrownByResume).as("what the resume of n1 threw").isNull();
            assertThat(fixture.wrapped.isRunning("n1")).as("n1 runs in the wrapped model").isTrue();
            awaitNothingLeftFor("n1");
            assertThat(fixture.wrapped.resumeFailuresOfN1.awaitCount(2)).as("the resume tried the start(true) first").isTrue();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A pause that finds a start(true) handed over for a subscription that does not compete still failing throws only
    // what its own call threw, and the thread that start(true) was handed to ends without trying it again
    @Test
    void a_pause_that_finds_a_start_handed_over_still_failing_throws_only_its_own_failure_and_ends_that_start() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            Gate backingOff = fixture.handOverAStartOfN1ThatFails(Integer.MAX_VALUE);

            Throwable thrownByPause = catchThrowable(() -> fixture.model.pauseSubscription("n1"));
            backingOff.open();

            assertThat(thrownByPause).as("what the pause of n1, which is paused already, threw").isInstanceOf(SubscriptionNotRunningException.class);
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(noThreadLeftFor("n1")).as("no thread is left trying start(true) for n1").isTrue());
            assertThat(fixture.wrapped.holdsPaused("n1")).as("n1 is paused in the wrapped model").isTrue();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A cancel that finds a start(true) handed over for a subscription that does not compete still failing is made all
    // the same, and the thread that start(true) was handed to ends without trying it again
    @Test
    void a_cancel_that_finds_a_start_handed_over_still_failing_is_made_and_ends_that_start() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            Gate backingOff = fixture.handOverAStartOfN1ThatFails(Integer.MAX_VALUE);

            Throwable thrownByCancel = catchThrowable(() -> fixture.model.cancelSubscription("n1"));
            backingOff.open();

            assertThat(thrownByCancel).as("what the cancel of n1 threw").isNull();
            assertThat(fixture.wrapped.holdsPaused("n1") || fixture.wrapped.isRunning("n1")).as("whether the wrapped model still holds n1").isFalse();
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(noThreadLeftFor("n1")).as("no thread is left trying start(true) for n1").isTrue());
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume of s1 that registers it with the lease strategy while shutdown() runs gives up the lease it took once that
    // register returns, so no other node waits for the lease to expire
    @Test
    void a_lease_taken_by_a_register_under_way_when_shutdown_begins_is_given_up() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        Gate resumeRegistering = new Gate();
        try {
            fixture.model.pauseSubscription("s1");
            fixture.strategy.nextRegisterOfS1OnTheTestThread.set(resumeRegistering);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeRegistering.awaitEntered()).as("the resume of s1 registers it with the lease strategy").isTrue();

            fixture.model.shutdown();
            resumeRegistering.open();
            await().atMost(EVENTUALLY).until(resume::isDone);

            assertThat(fixture.strategy.holders).as("the leases this node holds once shutdown() and the resume of s1 returned").doesNotContain("s1");
            assertThat(fixture.strategy.candidates).as("the subscriptions this node competes for once shutdown() and the resume of s1 returned").doesNotContain("s1");
        } finally {
            resumeRegistering.open();
            fixture.model.shutdown();
        }
    }

    // A resume that took the lock of s1 before stop() began runs first, and stop() pauses s1 after it
    @Test
    void a_resume_under_way_when_stop_begins_ends_paused_by_the_user() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.model.pauseSubscription("s1");
            Gate resumeHoldingTheLock = new Gate();
            fixture.wrapped.nextIsRunningOfS1OnTheTestThread.set(resumeHoldingTheLock);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeHoldingTheLock.awaitEntered()).as("the resume of s1 holds its lock").isTrue();

            fixture.model.stop();
            resumeHoldingTheLock.open();
            assertThat(resume).as("the resume of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once the resume and stop() are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    // A pause made once stop() and start(true) have returned comes after both, also when the lock of s1 was held when
    // they were made and the thread they were handed to has yet to apply them
    @Test
    void a_pause_after_a_stop_and_a_start_that_were_handed_over_comes_after_both() {
        State expected = withTheLockFree(Initially.RUNNING, model -> {
            model.stop();
            model.start(true);
            model.pauseSubscription("s1");
        });
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            Gate backingOff = fixture.handedOverThreadAboutToBackOff();
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.model.stop();
            fixture.model.start(true);
            // The thread s1 is handed to gets an Error applying stop(), and then stands before its backoff with the lock
            // of s1 free and neither call applied
            fixture.wrapped.errorsFromIsRunningOfS1OnALifecycleThread.set(1);
            grantHoldingTheLock.open();
            assertThat(backingOff.awaitEntered()).as("the thread s1 was handed to failed and let go of the lock").isTrue();

            fixture.model.pauseSubscription("s1");
            backingOff.open();
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once stop(), start(true) and the pause after them are applied").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume of a subscription that does not compete, under way in the wrapped model when stop() begins, comes before
    // that stop(), so the stop() pauses it in the wrapped model once the resume returns
    @Test
    void a_resume_of_a_subscription_that_does_not_compete_under_way_when_stop_begins_comes_before_that_stop() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.subscribeN1();
            fixture.model.pauseSubscription("n1");
            Gate resumeInTheWrappedModel = new Gate();
            fixture.wrapped.nextResumeOfN1OnTheTestThread.set(resumeInTheWrappedModel);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("n1"));
            assertThat(resumeInTheWrappedModel.awaitEntered()).as("the resume of n1 is under way in the wrapped model").isTrue();

            Thread stopping = new Thread(fixture.model::stop, "test-stopping-thread");
            stopping.setDaemon(true);
            stopping.start();
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> stopping.getState() == Thread.State.TERMINATED || waitsOnAMonitor(stopping));
            resumeInTheWrappedModel.open();
            assertThat(resume).as("the resume of n1").succeedsWithin(EVENTUALLY);
            await().atMost(EVENTUALLY).until(() -> stopping.getState() == Thread.State.TERMINATED);

            assertThat(fixture.wrapped.holdsPaused("n1")).as("n1 is paused in the wrapped model once the resume and stop() are applied").isTrue();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A grant under way when shutdown() begins neither starts the wrapped model nor resumes a subscription there once
    // shutdown() has returned. The grant read s1 as running before shutdown() began, and then finds the wrapped model
    // not running it, since shutdown() stopped it.
    @Test
    void a_grant_under_way_when_shutdown_begins_runs_nothing_in_the_wrapped_model_after_shutdown_returned() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.strategy.leaseHeldAfterTheGate.set(true);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();

            fixture.model.shutdown();
            grantHoldingTheLock.open();
            await().atMost(EVENTUALLY).until(fixture.grant::isDone);
            awaitNothingLeftForS1();

            assertThat(fixture.wrapped.callsAfterShutdown).as("what the wrapped model was asked to start or run once it was shut down").isEmpty();
        } finally {
            fixture.model.shutdown();
        }
    }

    // An Error from registering s1, while the thread s1 was handed to applies start(true), has s1 tried again instead
    // of recorded as running without its lease
    @Test
    void an_error_from_registering_on_the_thread_a_start_was_handed_to_leaves_the_subscription_to_be_tried_again() {
        State expected = withTheLockFree(Initially.STOPPED, List.of(Call.START_RESUMING));
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.strategy.errorsFromRegisteringS1OnALifecycleThread.set(1);
            fixture.model.start(true);
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once start(true) is applied").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A cancel holding the lock of a subscription that does not compete when start(false) begins comes before that
    // start(false), after which the wrapped model stays stopped when no other such subscription is left to start
    @Test
    void a_cancel_of_the_last_subscription_that_does_not_compete_holding_its_lock_when_start_begins_comes_before_that_start() {
        Fixture free = new Fixture(Initially.STOPPED);
        boolean expected;
        try {
            free.subscribeN1();
            free.model.cancelSubscription("n1");
            free.model.start(false);
            awaitNothingLeftFor("n1");
            expected = free.wrapped.isRunning();
        } finally {
            free.model.shutdown();
        }
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            fixture.subscribeN1();
            Gate cancelInTheWrappedModel = new Gate();
            fixture.wrapped.nextCancelOfN1OnTheTestThread.set(cancelInTheWrappedModel);
            CompletableFuture<Void> cancel = runOnTheTestThread(() -> fixture.model.cancelSubscription("n1"));
            assertThat(cancelInTheWrappedModel.awaitEntered()).as("the cancel of n1 holds its lock").isTrue();

            fixture.model.start(false);
            cancelInTheWrappedModel.open();
            assertThat(cancel).as("the cancel of n1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftFor("n1");

            assertThat(fixture.wrapped.isRunning()).as("whether the wrapped model runs once the cancel and start(false) are applied").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    private static boolean waitsOnAMonitor(Thread thread) {
        return thread.getState() == Thread.State.WAITING && Stream.of(thread.getStackTrace())
                .anyMatch(frame -> frame.getClassName().equals(Object.class.getName()) && frame.getMethodName().startsWith("wait"));
    }

    private static CompletableFuture<Void> runOnTheTestThread(Runnable call) {
        CompletableFuture<Void> called = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                call.run();
                called.complete(null);
            } catch (Throwable e) {
                called.completeExceptionally(e);
            }
        }, WrappedModel.TEST_THREAD);
        thread.setDaemon(true);
        thread.start();
        return called;
    }

    private static State withTheLockFree(Initially initially, List<Call> calls) {
        return withTheLockFree(initially, model -> calls.forEach(call -> call.apply(model)));
    }

    private static State withTheLockFree(Initially initially, Consumer<CompetingConsumerSubscriptionModel> calls) {
        Fixture fixture = new Fixture(initially);
        try {
            calls.accept(fixture.model);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    private static State handedOver(Initially initially, List<Call> calls, When when) {
        Fixture fixture = new Fixture(initially);
        try {
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            List<Call> later = calls.subList(1, calls.size());
            calls.getFirst().apply(fixture.model);
            if (when == When.WHILE_THE_FIRST_IS_APPLIED) {
                // The first call is applied once the grant returns, and calls into the wrapped model unless it has
                // nothing to do there
                Gate applyingTheFirst = new Gate();
                fixture.wrapped.nextIsRunningOfS1OnAHandedOverThread.set(applyingTheFirst);
                grantHoldingTheLock.open();
                await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> applyingTheFirst.hasEntered() || noThreadLeftForS1());
                boolean takenByTheFirst = fixture.wrapped.nextIsRunningOfS1OnAHandedOverThread.getAndSet(null) == null;
                if (takenByTheFirst) {
                    assertThat(applyingTheFirst.awaitEntered()).as("the first call is being applied to s1").isTrue();
                }
                later.forEach(call -> call.apply(fixture.model));
                applyingTheFirst.open();
            } else {
                later.forEach(call -> call.apply(fixture.model));
                grantHoldingTheLock.open();
            }
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    // Neither a thread applying a start(..) or stop() to s1, nor a try of s1, is left
    private static void awaitNothingLeftForS1() {
        awaitNothingLeftFor("s1");
    }

    private static void awaitNothingLeftFor(String subscriptionId) {
        await().atMost(EVENTUALLY).pollInterval(5, MILLISECONDS).until(() -> noThreadLeftFor(subscriptionId));
    }

    private static boolean noThreadLeftForS1() {
        return noThreadLeftFor("s1");
    }

    private static boolean noThreadLeftFor(String subscriptionId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .noneMatch(name -> name.equals("occurrent-competing-consumer-lifecycle-" + subscriptionId) || name.equals("occurrent-competing-consumer-reconcile-" + subscriptionId));
    }

    private enum Initially {
        RUNNING("running"),
        STOPPED("stopped");

        private final String description;

        Initially(String description) {
            this.description = description;
        }
    }

    private enum Call {
        START_RESUMING("start(true)"),
        START_NOT_RESUMING("start(false)"),
        STOP("stop()");

        private final String description;

        Call(String description) {
            this.description = description;
        }

        private void apply(CompetingConsumerSubscriptionModel model) {
            switch (this) {
                case START_RESUMING -> model.start(true);
                case START_NOT_RESUMING -> model.start(false);
                case STOP -> model.stop();
            }
        }
    }

    private enum When {
        WHILE_THE_LOCK_IS_HELD("while the lock is still held"),
        WHILE_THE_FIRST_IS_APPLIED("while the first is being applied");

        private final String description;

        When(String description) {
            this.description = description;
        }
    }

    // Where s1 is once start(false) is applied last, and whether it competes for its lease then
    private record Ended(State state, boolean competes) {
    }

    // Whether the wrapped model holds s1 paused, and where s1 is
    private record Made(boolean heldPausedInTheWrappedModel, State state) {
    }

    // Paused by the user, or by stop(), is a pause that start(false) keeps
    private record State(boolean pausedInThisModel, boolean pausedByTheUser, boolean runsInTheWrappedModel, boolean holdsTheLease, boolean wrappedModelRunning) {
    }

    // A started model with s1 running, which a stop() stops when the model starts out stopped. With another node
    // holding the lease of s1 at first, s1 waits for it instead.
    private static final class Fixture {
        private final WrappedModel wrapped = new WrappedModel();
        private final Strategy strategy = new Strategy();
        private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        private final CompletableFuture<Void> grant = new CompletableFuture<>();

        private Fixture(Initially initially) {
            this(initially, false);
        }

        private Fixture(Initially initially, boolean anotherNodeHoldsS1) {
            strategy.anotherNodeHoldsS1.set(anotherNodeHoldsS1);
            model.subscribe(NODE, "s1", null, StartAt.subscriptionModelDefault(), __ -> {
            });
            if (initially == Initially.STOPPED) {
                model.stop();
            }
        }

        // A thread a subscription is handed to waits at the gate each time it has failed and let go of the lock, before
        // its backoff, until the test opens it
        private Gate handedOverThreadAboutToBackOff() {
            Gate backingOff = new Gate();
            model.runBeforeAHandedOverThreadBacksOff(backingOff::pass);
            return backingOff;
        }

        // A grant for s1 that waits inside hasLock holds the lock of s1, and then finds the lease gone, so the grant
        // itself changes nothing
        private Gate grantHoldingTheLockOfS1() {
            Gate grantHoldingTheLock = new Gate();
            strategy.nextHasLockForS1.set(grantHoldingTheLock);
            Thread granting = new Thread(() -> {
                try {
                    model.onConsumeGranted("s1", NODE);
                    grant.complete(null);
                } catch (Throwable e) {
                    grant.completeExceptionally(e);
                }
            }, "test-granting-thread");
            granting.setDaemon(true);
            granting.start();
            assertThat(grantHoldingTheLock.awaitEntered()).as("the grant of s1 holds its lock").isTrue();
            return grantHoldingTheLock;
        }

        private void loseTheLeaseOfS1() {
            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", NODE);
        }

        private void subscribeN1() {
            model.subscribe(NODE, "n1", null, StartAt.dynamic(__ -> null), __ -> {
            });
        }

        // Subscribes n1, which does not compete, to a stopped model, and has start(true) handed over for it while a
        // pause of n1 holds its lock. The pause then fails, since n1 is paused already. Resuming n1 fails as often as
        // given, and the thread n1 is handed to waits at the returned gate after its first failure, with the lock free.
        private Gate handOverAStartOfN1ThatFails(int failures) {
            subscribeN1();
            Gate backingOff = handedOverThreadAboutToBackOff();
            Gate pauseHoldingTheLock = new Gate();
            wrapped.nextIsPausedOfN1OnTheTestThread.set(pauseHoldingTheLock);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> model.pauseSubscription("n1"));
            assertThat(pauseHoldingTheLock.awaitEntered()).as("the pause of n1 holds its lock").isTrue();
            wrapped.failuresFromResumingN1.set(failures);
            model.start(true);
            pauseHoldingTheLock.open();
            assertThat(pause).as("the pause of n1").failsWithin(EVENTUALLY);
            assertThat(backingOff.awaitEntered()).as("the thread n1 was handed to failed and let go of the lock").isTrue();
            return backingOff;
        }

        private Made made() {
            boolean heldPaused = wrapped.isPaused("s1");
            return new Made(heldPaused, state());
        }

        // Applies start(false) last when s1 is paused, which tells whether the user paused it
        private State state() {
            boolean paused = model.isPaused("s1");
            boolean runs = wrapped.isRunning("s1");
            boolean holdsTheLease = strategy.holders.contains("s1");
            boolean wrappedModelRunning = wrapped.isRunning();
            boolean pausedByTheUser = false;
            if (paused) {
                model.start(false);
                awaitNothingLeftForS1();
                pausedByTheUser = model.isPaused("s1");
            }
            return new State(paused, pausedByTheUser, runs, holdsTheLease, wrappedModelRunning);
        }
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            if (!latch.await(EVENTUALLY.toMillis(), MILLISECONDS)) {
                throw new IllegalStateException("Timed out waiting for a latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);

        private void pass() {
            entered.countDown();
            awaitOrFail(open);
        }

        private boolean hasEntered() {
            return entered.getCount() == 0;
        }

        private boolean awaitEntered() {
            try {
                return entered.await(EVENTUALLY.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private void open() {
            open.countDown();
        }
    }

    // How often something happened, which a test can wait for
    private static final class Counter {
        private final AtomicInteger count = new AtomicInteger();

        private void increment() {
            synchronized (count) {
                count.incrementAndGet();
                count.notifyAll();
            }
        }

        private boolean awaitCount(int expected) {
            long deadline = System.nanoTime() + EVENTUALLY.toNanos();
            synchronized (count) {
                while (count.get() < expected) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        return false;
                    }
                    try {
                        count.wait(Math.max(1, left / 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return true;
            }
        }
    }

    // Grants every lease asked for, apart from that of s1 while another node holds it, and records which subscriptions
    // compete for their lease. The next hasLock for s1 waits at a gate once the test sets one, and then answers that the
    // lease is not held, unless the test says it is. The next register of s1 on the test thread waits at a gate too,
    // before it decides whether to grant the lease.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Gate> nextHasLockForS1 = new AtomicReference<>();
        private final AtomicBoolean leaseHeldAfterTheGate = new AtomicBoolean();
        private final AtomicInteger errorsFromRegisteringS1OnALifecycleThread = new AtomicInteger();
        private final AtomicBoolean anotherNodeHoldsS1 = new AtomicBoolean();
        private final AtomicReference<@Nullable Gate> nextRegisterOfS1OnTheTestThread = new AtomicReference<>();
        private final Set<String> candidates = ConcurrentHashMap.newKeySet();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (subscriptionId.equals("s1") && Thread.currentThread().getName().equals(WrappedModel.LIFECYCLE_THREAD_OF_S1)
                    && errorsFromRegisteringS1OnALifecycleThread.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new AssertionError("Registering s1 failed on " + Thread.currentThread().getName());
            }
            if (subscriptionId.equals("s1") && Thread.currentThread().getName().equals(WrappedModel.TEST_THREAD)) {
                Gate gate = nextRegisterOfS1OnTheTestThread.getAndSet(null);
                if (gate != null) {
                    gate.pass();
                }
            }
            candidates.add(subscriptionId);
            if (subscriptionId.equals("s1") && anotherNodeHoldsS1.get()) {
                return false;
            }
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            candidates.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (subscriptionId.equals("s1")) {
                Gate gate = nextHasLockForS1.getAndSet(null);
                if (gate != null) {
                    gate.pass();
                    return leaseHeldAfterTheGate.get();
                }
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
        }
    }

    // A model of a user's own, which pauses what it runs when it is stopped and runs nothing while it is stopped. The
    // calls a test waits at, or makes fail, are each set by a field of their own.
    private static final class WrappedModel implements SubscriptionModel {
        private static final String TEST_THREAD = "test-calling-thread";
        private static final String LIFECYCLE_THREAD_OF_S1 = "occurrent-competing-consumer-lifecycle-s1";
        private static final String TRY_OF_S1 = "occurrent-competing-consumer-reconcile-s1";

        private final AtomicReference<@Nullable Gate> nextIsRunningOfS1OnAHandedOverThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextIsRunningOfS1OnTheTestThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextIsPausedOfN1OnTheTestThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextResumeOfN1OnTheTestThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextCancelOfN1OnTheTestThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextPauseOfS1OnATry = new AtomicReference<>();
        private final AtomicInteger errorsFromIsRunningOfS1OnALifecycleThread = new AtomicInteger();
        private final AtomicInteger failuresFromResumingN1 = new AtomicInteger();
        private final Counter resumeFailuresOfN1 = new Counter();
        private final List<String> callsAfterShutdown = new CopyOnWriteArrayList<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;
        private boolean shutDown;

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            calledAfterShutdown("subscribe " + subscriptionId);
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            calledAfterShutdown("subscribePaused " + subscriptionId);
            pausedIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        // Waits at the gate before it takes the monitor, so the calls a test makes meanwhile are not held up by it
        @Override
        public void cancelSubscription(String subscriptionId) {
            if (subscriptionId.equals("n1") && Thread.currentThread().getName().equals(TEST_THREAD)) {
                passIfSet(nextCancelOfN1OnTheTestThread);
            }
            synchronized (this) {
                runningIds.remove(subscriptionId);
                pausedIds.remove(subscriptionId);
            }
        }

        @Override
        public synchronized void shutdown() {
            stop();
            shutDown = true;
        }

        private synchronized boolean holdsPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId) && !runningIds.contains(subscriptionId);
        }

        private void calledAfterShutdown(String call) {
            if (shutDown) {
                callsAfterShutdown.add(call);
            }
        }

        @Override
        public synchronized void stop() {
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            calledAfterShutdown("start");
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (subscriptionId.equals("s1")) {
                String thread = Thread.currentThread().getName();
                if (thread.equals(LIFECYCLE_THREAD_OF_S1)) {
                    if (errorsFromIsRunningOfS1OnALifecycleThread.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                        throw new AssertionError("isRunning of s1 failed on " + thread);
                    }
                    passIfSet(nextIsRunningOfS1OnAHandedOverThread);
                } else if (thread.equals(TEST_THREAD)) {
                    passIfSet(nextIsRunningOfS1OnTheTestThread);
                }
            }
            synchronized (this) {
                return running && runningIds.contains(subscriptionId);
            }
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            if (subscriptionId.equals("n1") && Thread.currentThread().getName().equals(TEST_THREAD)) {
                passIfSet(nextIsPausedOfN1OnTheTestThread);
            }
            synchronized (this) {
                return pausedIds.contains(subscriptionId);
            }
        }

        // Waits at the gate before it takes the monitor, so the calls a test makes meanwhile are not held up by it
        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (subscriptionId.equals("n1") && Thread.currentThread().getName().equals(TEST_THREAD)) {
                passIfSet(nextResumeOfN1OnTheTestThread);
            }
            synchronized (this) {
                calledAfterShutdown("resume " + subscriptionId);
                if (subscriptionId.equals("n1") && failuresFromResumingN1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                    resumeFailuresOfN1.increment();
                    throw new IllegalStateException("Resuming n1 failed");
                }
                pausedIds.remove(subscriptionId);
                runningIds.add(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
        }

        // Waits at the gate before it takes the monitor, so the calls a test makes meanwhile are not held up by it
        @Override
        public void pauseSubscription(String subscriptionId) {
            if (subscriptionId.equals("s1") && Thread.currentThread().getName().equals(TRY_OF_S1)) {
                passIfSet(nextPauseOfS1OnATry);
            }
            synchronized (this) {
                if (runningIds.remove(subscriptionId)) {
                    pausedIds.add(subscriptionId);
                }
            }
        }

        private static void passIfSet(AtomicReference<@Nullable Gate> gate) {
            Gate set = gate.getAndSet(null);
            if (set != null) {
                set.pass();
            }
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
