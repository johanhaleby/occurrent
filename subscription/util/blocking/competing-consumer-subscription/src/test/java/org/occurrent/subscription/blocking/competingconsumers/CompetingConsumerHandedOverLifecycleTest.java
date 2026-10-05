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
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
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

    // A pause, resume or cancel of s1 that waits for its lock comes before a start(..) or stop() that begins while it
    // waits, which is handed over and applied once the pause, resume or cancel has returned
    @TestFactory
    Stream<DynamicTest> a_call_waiting_for_the_lock_comes_before_a_start_or_stop_that_begins_meanwhile() {
        List<DynamicTest> tests = new ArrayList<>();
        for (From from : From.values()) {
            for (UserCall userCall : UserCall.values()) {
                for (Call later : Call.values()) {
                    String name = "from " + from.description + ", " + userCall.description + " waiting for the lock of s1, then " + later.description;
                    tests.add(DynamicTest.dynamicTest(name, () -> {
                        State expected = withTheLockFree(from, userCall, later);
                        State actual = whileTheCallWaitsForTheLock(from, userCall, later);
                        assertThat(actual).as("[s1 from %s once %s and then %s are applied]",
                                from.description, userCall.description, later.description).isEqualTo(expected);
                    }));
                }
            }
        }
        return tests.stream();
    }

    // A pause, resume or cancel of s1 that begins after a start(..) or stop() has begun comes after it, also when it
    // takes the lock of s1 before that start(..) or stop() has got to s1
    @TestFactory
    Stream<DynamicTest> a_call_that_begins_after_a_start_or_stop_comes_after_it_also_when_it_takes_the_lock_first() {
        List<DynamicTest> tests = new ArrayList<>();
        for (From from : From.values()) {
            for (Call earlier : Call.values()) {
                for (UserCall userCall : UserCall.values()) {
                    String name = "from " + from.description + ", " + earlier.description + ", then " + userCall.description + " that takes the lock of s1 first";
                    tests.add(DynamicTest.dynamicTest(name, () -> {
                        State expected = withTheLockFree(from, earlier, userCall);
                        State actual = beforeTheStartOrStopGetsToS1(from, earlier, userCall);
                        assertThat(actual).as("[s1 from %s once %s and then %s are applied]",
                                from.description, earlier.description, userCall.description).isEqualTo(expected);
                    }));
                }
            }
        }
        return tests.stream();
    }

    // A pause of s1 that takes its lock before a resume of s1 that began first lets the resume go first, so a stop() that
    // began between the two is applied after the resume and before the pause
    @Test
    void a_call_that_takes_the_lock_first_lets_a_call_waiting_since_before_a_stop_go_first() {
        State expected = withTheLockFree(Initially.RUNNING, model -> {
            From.RUNNING_WITH_S1_PAUSED.prepare(model);
            catchThrowable(() -> model.resumeSubscription("s1"));
            model.stop();
            catchThrowable(() -> model.pauseSubscription("s1"));
        });
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            From.RUNNING_WITH_S1_PAUSED.prepare(fixture.model);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            Gate resumeAboutToWait = new Gate();
            AtomicBoolean first = new AtomicBoolean(true);
            fixture.model.runBeforeACallWaitsForTheLock(() -> {
                if (first.getAndSet(false)) {
                    resumeAboutToWait.pass();
                }
            });
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeAboutToWait.awaitEntered()).as("the resume of s1 counts as waiting for its lock").isTrue();
            fixture.model.stop();
            CompletableFuture<Void> pause = new CompletableFuture<>();
            Thread pausing = startOnTheTestThread(() -> fixture.model.pauseSubscription("s1"), pause);
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> pause.isDone() || waitsForALock(pausing));

            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            // The pause has taken the lock of s1 by then, and waits for the resume, or has returned
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> pause.isDone() || waitsForItsTurn(pausing));
            resumeAboutToWait.open();
            await().atMost(EVENTUALLY).until(() -> resume.isDone() && pause.isDone());
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("[s1 once a resume, a stop() and a pause are applied in the order they began]").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
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

            // start(false) starts the wrapped model, though it wins no lease
            assertThat(fixture.state()).as("s1 once start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, true));
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

            // start(false) starts the wrapped model, though it wins no lease
            assertThat(fixture.state()).as("s1 once the grant, start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, true));
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

    // A cancel that gives up a start(true) handed over for a subscription that does not compete, while starting the
    // wrapped model fails, gives up only what that start(true) does for the subscription, and the wrapped model is
    // still started
    @Test
    void a_cancel_that_gives_up_a_start_handed_over_while_the_wrapped_model_fails_to_start_has_it_started_all_the_same() {
        Fixture fixture = new Fixture(Initially.STOPPED, true);
        try {
            // Fails once for the start(true) itself, once on the thread n1 is handed to and once when the cancel tries
            // the start(true) first
            Gate backingOff = fixture.handOverAStartOfN1WhileTheWrappedModelFailsToStart(3);

            Throwable thrownByCancel = catchThrowable(() -> fixture.model.cancelSubscription("n1"));
            backingOff.open();

            assertThat(thrownByCancel).as("what the cancel of n1 threw").isNull();
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(fixture.wrapped.isRunning()).as("whether the wrapped model runs").isTrue());
            assertThat(fixture.wrapped.holdsPaused("n1") || fixture.wrapped.isRunning("n1")).as("whether the wrapped model still holds n1").isFalse();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A pause that gives up a start(true) handed over for a subscription that does not compete, while starting the
    // wrapped model fails, pauses the subscription, and the wrapped model is still started
    @Test
    void a_pause_that_gives_up_a_start_handed_over_while_the_wrapped_model_fails_to_start_has_it_started_all_the_same() {
        Fixture fixture = new Fixture(Initially.STOPPED, true);
        try {
            // Fails once for the start(true) itself, once on the thread n1 is handed to and once when the pause tries
            // the start(true) first
            Gate backingOff = fixture.handOverAStartOfN1WhileTheWrappedModelFailsToStart(3);

            Throwable thrownByPause = catchThrowable(() -> fixture.model.pauseSubscription("n1"));
            backingOff.open();

            assertThat(thrownByPause).as("what the pause of n1 threw").isNull();
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(fixture.wrapped.isRunning()).as("whether the wrapped model runs").isTrue());
            assertThat(fixture.wrapped.holdsPaused("n1")).as("n1 is paused in the wrapped model").isTrue();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume that gives up a start(true) handed over for a subscription that does not compete, while starting the
    // wrapped model fails, has the subscription run once the wrapped model is started. The thread that goes on starting
    // the wrapped model can start it before the resume asks whether n1 runs, and the resume then finds n1 running
    // already and throws, so the resume may return or throw that.
    @Test
    void a_resume_that_gives_up_a_start_handed_over_while_the_wrapped_model_fails_to_start_has_the_subscription_run() {
        Fixture fixture = new Fixture(Initially.STOPPED, true);
        try {
            // Fails once for the start(true) itself, once on the thread n1 is handed to and once when the resume tries
            // the start(true) first
            Gate backingOff = fixture.handOverAStartOfN1WhileTheWrappedModelFailsToStart(3);

            Throwable thrownByResume = catchThrowable(() -> fixture.model.resumeSubscription("n1"));
            backingOff.open();

            assertThat(thrownByResume).as("what the resume of n1 threw").satisfiesAnyOf(
                    thrown -> assertThat(thrown).isNull(),
                    thrown -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class));
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(fixture.wrapped.isRunning("n1")).as("n1 runs in the wrapped model").isTrue());
        } finally {
            fixture.model.shutdown();
        }
    }

    // A pause that gives up a start(true) handed over for a subscription that does not compete throws the Error that
    // start(true) threw, also when its own call throws that same instance, as a model that keeps one Error for every
    // failure does
    @Test
    void a_pause_whose_own_call_throws_the_error_it_gave_up_throws_that_error() {
        Fixture fixture = new Fixture(Initially.STOPPED, true);
        Error shared = new AssertionError("One Error for every failure");
        try {
            Gate backingOff = fixture.handOverAStartOfN1(() -> fixture.wrapped.errorFromStarting.set(shared));
            assertThat(fixture.thrownByStart).as("what start(true) threw").isSameAs(shared);
            fixture.wrapped.errorFromIsPausedOfN1.set(shared);

            Throwable thrownByPause = catchThrowable(() -> fixture.model.pauseSubscription("n1"));
            fixture.wrapped.errorFromIsPausedOfN1.set(null);
            fixture.wrapped.errorFromStarting.set(null);
            backingOff.open();

            assertThat(thrownByPause).as("what the pause of n1 threw").isSameAs(shared);
        } finally {
            fixture.model.shutdown();
        }
    }

    // The wrapped model, which a start(true) given up for a subscription that does not compete still has started, stays
    // stopped once stop() is called, and nothing is left trying to start it
    @Test
    void a_stop_ends_the_start_of_the_wrapped_model_for_a_start_given_up() {
        Fixture fixture = new Fixture(Initially.STOPPED, true);
        try {
            Gate backingOff = fixture.handOverAStartOfN1WhileTheWrappedModelFailsToStart(Integer.MAX_VALUE);
            fixture.model.cancelSubscription("n1");
            backingOff.open();

            fixture.model.stop();
            fixture.wrapped.failuresFromStarting.set(0);

            await().atMost(EVENTUALLY).pollInterval(5, MILLISECONDS).until(() -> noThreadNamed("occurrent-competing-consumer-wrapped-model-start"));
            assertThat(fixture.wrapped.isRunning()).as("whether the wrapped model runs after stop()").isFalse();
        } finally {
            fixture.model.shutdown();
        }
    }

    // An Error from a stop() handed over for s1, which a cancel of s1 gives up, is thrown once the cancel is made
    @Test
    void an_error_from_a_stop_handed_over_that_a_cancel_gives_up_is_thrown_once_the_cancel_is_made() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        Gate tryBeforeItsBackoff = new Gate();
        Gate backingOff = fixture.handedOverThreadAboutToBackOff();
        try {
            // Holds the try that the failure on the thread s1 is handed to starts, so the stop() is left for the cancel
            fixture.model.runBeforeATryWaitsForItsBackoff(tryBeforeItsBackoff::pass);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.wrapped.errorsFromIsRunningOfS1OnALifecycleThread.set(1);
            fixture.model.stop();
            grantHoldingTheLock.open();
            assertThat(backingOff.awaitEntered()).as("the thread s1 was handed to failed and let go of the lock").isTrue();
            assertThat(tryBeforeItsBackoff.awaitEntered()).as("the try of s1 waits before its backoff").isTrue();
            fixture.strategy.errorsFromUnregisteringS1.set(1);

            Throwable thrownByCancel = catchThrowable(() -> fixture.model.cancelSubscription("s1"));

            assertThat(thrownByCancel).as("what the cancel of s1 threw").isInstanceOf(AssertionError.class).hasMessageStartingWith("Unregistering s1 failed");
            assertThat(fixture.wrapped.holdsPaused("s1") || fixture.wrapped.isRunning("s1")).as("whether the wrapped model still holds s1").isFalse();
            assertThat(fixture.strategy.candidates).as("the subscriptions this node competes for").doesNotContain("s1");
        } finally {
            tryBeforeItsBackoff.open();
            backingOff.open();
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
    // start(false), so the wrapped model ends as it does when the cancel returns before start(false) begins
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

    // A stop() that waits for a resume of n1 under way in the wrapped model is taken back once start(false) waits behind
    // it, and stops nothing then. A grant of s1 that takes the lock of s1 while that stop() waits leaves s1 to it, so s1
    // ends where start(false) alone leaves it.
    @Test
    void a_stop_taken_back_for_a_start_waiting_behind_it_leaves_a_subscription_a_grant_took_meanwhile_as_the_start_decides() {
        State expected = withTheLockFree(Initially.RUNNING, model -> model.start(false));
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.subscribeN1();
            fixture.model.pauseSubscription("n1");
            Gate resumeOfN1InTheWrappedModel = new Gate();
            fixture.wrapped.nextResumeOfN1OnTheTestThread.set(resumeOfN1InTheWrappedModel);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("n1"));
            assertThat(resumeOfN1InTheWrappedModel.awaitEntered()).as("the resume of n1 waits in the wrapped model").isTrue();
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            Thread stopping = startOnALifecycleCallThread(fixture.model::stop, stopped);
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> stopped.isDone() || waitsOnAMonitor(stopping));
            assertThat(stopped).as("stop() waits for the resume of n1").isNotDone();

            fixture.model.onConsumeGranted("s1", NODE);
            CompletableFuture<Void> started = new CompletableFuture<>();
            startOnALifecycleCallThread(() -> fixture.model.start(false), started);
            assertThat(stopped).as("stop() once start(false) waits behind it").succeedsWithin(EVENTUALLY);
            assertThat(started).as("start(false)").succeedsWithin(EVENTUALLY);
            resumeOfN1InTheWrappedModel.open();
            assertThat(resume).as("the resume of n1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("[s1 once a grant and start(false) are applied, with stop() taken back]").isEqualTo(expected);
        } finally {
            fixture.model.shutdown();
        }
    }

    // A stop() whose pause of s1 does not take, applied to s1 by a resume that began after it and took the lock of s1
    // first, throws what failed for s1, as it does with the lock free
    @Test
    void a_stop_applied_to_a_subscription_by_a_later_resume_throws_what_failed_for_that_subscription() {
        Throwable expected;
        Fixture free = new Fixture(Initially.RUNNING);
        try {
            free.wrapped.s1RunsOnWhateverPausesIt.set(true);
            expected = catchThrowable(free.model::stop);
            catchThrowable(() -> free.model.resumeSubscription("s1"));
        } finally {
            free.model.shutdown();
        }
        assertThat(expected).as("what stop() throws with the lock free").isInstanceOf(IllegalStateException.class);

        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.wrapped.s1RunsOnWhateverPausesIt.set(true);
            Gate begun = new Gate();
            AtomicBoolean first = new AtomicBoolean(true);
            fixture.model.runOnceAStartOrStopHasBegun(() -> {
                if (first.getAndSet(false)) {
                    begun.pass();
                }
            });
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            startOnALifecycleCallThread(fixture.model::stop, stopped);
            assertThat(begun.awaitEntered()).as("stop() has begun").isTrue();
            CompletableFuture<Void> resumed = new CompletableFuture<>();
            Thread resuming = startOnTheTestThread(() -> fixture.model.resumeSubscription("s1"), resumed);
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> resumed.isDone() || waitsOnAMonitor(resuming));
            begun.open();
            await().atMost(EVENTUALLY).until(() -> stopped.isDone() && resumed.isDone());

            assertThat(stopped).as("[stop() applied to s1 by the resume]").failsWithin(EVENTUALLY)
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(expected.getClass()).withMessage(expected.getMessage());
        } finally {
            fixture.model.shutdown();
        }
    }

    // A stop() that waits for a resume of n1 under way in the wrapped model has not stopped the wrapped model, which still
    // runs s1. A pause or cancel of s1 that began after that stop() applies it to s1 first, and the pause of s1 in the
    // wrapped model fails there. With the lock free, stop() stops the wrapped model first and then has nothing to pause
    // there, so stop() does not throw that failure here either.
    @TestFactory
    Stream<DynamicTest> a_stop_that_a_later_call_applied_before_the_wrapped_model_was_stopped_does_not_throw_what_its_own_thread_would_not_have_met() {
        return Stream.of(
                DynamicTest.dynamicTest("applied by a pause", () -> aStopAppliedBeforeTheWrappedModelWasStoppedBy(model -> model.pauseSubscription("s1"))),
                DynamicTest.dynamicTest("applied by a cancel", () -> aStopAppliedBeforeTheWrappedModelWasStoppedBy(model -> model.cancelSubscription("s1"))));
    }

    private static void aStopAppliedBeforeTheWrappedModelWasStoppedBy(Consumer<CompetingConsumerSubscriptionModel> laterCall) {
        Fixture free = new Fixture(Initially.RUNNING);
        try {
            free.wrapped.failuresFromPausingS1.set(1);
            assertThat(catchThrowable(free.model::stop)).as("what stop() throws with the lock free").isNull();
        } finally {
            free.model.shutdown();
        }

        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            StopWaitingForAResumeOfN1 stop = stopWaitingForAResumeOfN1(fixture);

            fixture.wrapped.failuresFromPausingS1.set(1);
            CompletableFuture<Void> later = runOnTheTestThread(() -> laterCall.accept(fixture.model));
            await().atMost(EVENTUALLY).until(later::isDone);
            assertThat(fixture.wrapped.failuresFromPausingS1).as("failures left for the pause of s1 in the wrapped model").hasValue(0);
            assertThat(stop.stopped()).as("stop() while the resume of n1 waits").isNotDone();
            stop.resumeOfN1InTheWrappedModel().open();
            assertThat(stop.resume()).as("the resume of n1").succeedsWithin(EVENTUALLY);

            assertThat(stop.stopped()).as("[stop() applied to s1 by the later call before the wrapped model was stopped]").succeedsWithin(EVENTUALLY);
        } finally {
            fixture.model.shutdown();
        }
    }

    // As above, with a pause of s1 applying the stop(), and then a resume of s1 that began after the stop() too. The
    // resume waits for the wrapped model to be stopped and then runs s1 there, and the thread of stop() gets to s1 only
    // after that. stop() does not pause a subscription that a resume begun after it let run, so it does not throw what
    // failed for the pause either.
    @Test
    void a_stop_that_a_later_pause_applied_before_the_wrapped_model_was_stopped_does_not_throw_for_a_subscription_a_later_resume_runs() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            StopWaitingForAResumeOfN1 stop = stopWaitingForAResumeOfN1(fixture);
            fixture.wrapped.failuresFromPausingS1.set(1);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("s1"));
            assertThat(pause).as("the pause of s1").succeedsWithin(EVENTUALLY);
            assertThat(fixture.wrapped.failuresFromPausingS1).as("failures left for the pause of s1 in the wrapped model").hasValue(0);
            CompletableFuture<Void> resumed = new CompletableFuture<>();
            Thread resuming = startOnTheTestThread(() -> fixture.model.resumeSubscription("s1"), resumed);
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> resumed.isDone() || waitsOnAMonitor(resuming));
            assertThat(resumed).as("the resume of s1 waits for the wrapped model to be stopped").isNotDone();

            Gate stopGetsToS1 = new Gate();
            fixture.wrapped.nextIsRunningOfS1OnAHandedOverThread.set(stopGetsToS1);
            stop.resumeOfN1InTheWrappedModel().open();
            assertThat(stop.resume()).as("the resume of n1").succeedsWithin(EVENTUALLY);
            assertThat(resumed).as("the resume of s1").succeedsWithin(EVENTUALLY);
            assertThat(fixture.wrapped.isRunning("s1")).as("s1 runs in the wrapped model").isTrue();
            assertThat(stopGetsToS1.awaitEntered()).as("stop() gets to s1").isTrue();
            stopGetsToS1.open();

            assertThat(stop.stopped()).as("[stop() applied to s1 by the pause, with s1 run by the later resume]").succeedsWithin(EVENTUALLY);
        } finally {
            fixture.model.shutdown();
        }
    }

    // As above, with the pause of s1 in the wrapped model succeeding, and the lease strategy failing to unregister s1
    // once or each time. With the lock free, the thread of stop() meets that failure itself, since it unregisters s1
    // whatever the wrapped model runs, so stop() throws it here too.
    @TestFactory
    Stream<DynamicTest> a_stop_that_a_later_pause_applied_before_the_wrapped_model_was_stopped_throws_what_failed_to_unregister_the_subscription() {
        return Stream.of(
                DynamicTest.dynamicTest("once", () -> aStopAppliedByALaterPauseWhileUnregisteringS1Fails(1)),
                DynamicTest.dynamicTest("each time", () -> aStopAppliedByALaterPauseWhileUnregisteringS1Fails(Integer.MAX_VALUE)));
    }

    private static void aStopAppliedByALaterPauseWhileUnregisteringS1Fails(int failures) {
        Throwable expected;
        Fixture free = new Fixture(Initially.RUNNING);
        try {
            free.strategy.failuresFromUnregisteringS1.set(failures);
            expected = catchThrowable(free.model::stop);
        } finally {
            free.strategy.failuresFromUnregisteringS1.set(0);
            free.model.shutdown();
        }
        assertThat(expected).as("what stop() throws with the lock free").isInstanceOf(IllegalStateException.class);

        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            StopWaitingForAResumeOfN1 stop = stopWaitingForAResumeOfN1(fixture);
            fixture.strategy.failuresFromUnregisteringS1.set(failures);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("s1"));
            await().atMost(EVENTUALLY).until(pause::isDone);
            assertThat(fixture.strategy.failuresFromUnregisteringS1).as("failures left for unregistering s1").hasValueLessThan(failures);
            assertThat(stop.stopped()).as("stop() while the resume of n1 waits").isNotDone();
            stop.resumeOfN1InTheWrappedModel().open();
            assertThat(stop.resume()).as("the resume of n1").succeedsWithin(EVENTUALLY);

            assertThat(stop.stopped()).as("[stop() applied to s1 by the pause, which failed to unregister s1]").failsWithin(EVENTUALLY)
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(expected.getClass()).withMessage(expected.getMessage());
        } finally {
            fixture.strategy.failuresFromUnregisteringS1.set(0);
            fixture.model.shutdown();
        }
    }

    // As above, with the pause of s1 in the wrapped model taking effect and then failing, or followed by an isRunning of
    // s1 that fails. That leaves s1 registered, and the wrapped model no longer runs it once the thread of stop() gets
    // to s1, as with the lock free. stop() returns only once this node has given up the lease of s1, as it does with the
    // lock free, while the try of s1 waits before its backoff.
    @TestFactory
    Stream<DynamicTest> a_stop_that_a_later_pause_applied_before_the_wrapped_model_was_stopped_gives_up_the_lease_of_a_subscription_the_wrapped_model_no_longer_runs() {
        return Stream.of(
                DynamicTest.dynamicTest("with the pause failing after it took effect", () -> aStopAppliedByALaterPauseThatLeftS1Paused(wrapped -> wrapped.failuresAfterPausingS1)),
                DynamicTest.dynamicTest("with isRunning failing after the pause", () -> aStopAppliedByALaterPauseThatLeftS1Paused(wrapped -> wrapped.failuresFromIsRunningOfS1AfterItsPause)));
    }

    private static void aStopAppliedByALaterPauseThatLeftS1Paused(Function<WrappedModel, AtomicInteger> failures) {
        Fixture free = new Fixture(Initially.RUNNING);
        try {
            failures.apply(free.wrapped).set(1);
            assertThat(catchThrowable(free.model::stop)).as("what stop() throws with the lock free").isNull();
            assertThat(free.strategy.holders).as("the leases this node holds once stop() returned with the lock free").doesNotContain("s1");
        } finally {
            failures.apply(free.wrapped).set(0);
            free.model.shutdown();
        }

        Fixture fixture = new Fixture(Initially.RUNNING);
        Gate tryBeforeItsBackoff = new Gate();
        try {
            fixture.model.runBeforeATryWaitsForItsBackoff(tryBeforeItsBackoff::pass);
            StopWaitingForAResumeOfN1 stop = stopWaitingForAResumeOfN1(fixture);
            failures.apply(fixture.wrapped).set(1);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("s1"));
            await().atMost(EVENTUALLY).until(pause::isDone);
            assertThat(failures.apply(fixture.wrapped)).as("failures left in the wrapped model").hasValue(0);
            assertThat(tryBeforeItsBackoff.awaitEntered()).as("the try of s1 waits before its backoff").isTrue();
            assertThat(fixture.strategy.holders).as("the leases this node holds once the pause of s1 returned").contains("s1");
            assertThat(stop.stopped()).as("stop() while the resume of n1 waits").isNotDone();
            stop.resumeOfN1InTheWrappedModel().open();
            assertThat(stop.resume()).as("the resume of n1").succeedsWithin(EVENTUALLY);
            assertThat(stop.stopped()).as("stop() applied to s1 by the pause").succeedsWithin(EVENTUALLY);

            assertThat(fixture.strategy.holders).as("[the leases this node holds once stop() returned, with the try of s1 waiting]").doesNotContain("s1");
        } finally {
            failures.apply(fixture.wrapped).set(0);
            tryBeforeItsBackoff.open();
            fixture.model.shutdown();
        }
    }

    // As above, with the pause of s1 in the wrapped model failing after it took effect, and the lease strategy failing to
    // unregister s1 once the pause returned. With the lock free, the thread of stop() meets that failure itself, so stop()
    // throws it here too.
    @Test
    void a_stop_that_a_later_pause_applied_before_the_wrapped_model_was_stopped_throws_what_failed_to_unregister_a_subscription_the_wrapped_model_no_longer_runs() {
        Throwable expected;
        Fixture free = new Fixture(Initially.RUNNING);
        try {
            free.wrapped.failuresAfterPausingS1.set(1);
            free.strategy.failuresFromUnregisteringS1.set(1);
            expected = catchThrowable(free.model::stop);
        } finally {
            free.wrapped.failuresAfterPausingS1.set(0);
            free.strategy.failuresFromUnregisteringS1.set(0);
            free.model.shutdown();
        }
        assertThat(expected).as("what stop() throws with the lock free").isInstanceOf(IllegalStateException.class).hasMessage("Unregistering s1 failed");

        Fixture fixture = new Fixture(Initially.RUNNING);
        Gate tryBeforeItsBackoff = new Gate();
        try {
            fixture.model.runBeforeATryWaitsForItsBackoff(tryBeforeItsBackoff::pass);
            StopWaitingForAResumeOfN1 stop = stopWaitingForAResumeOfN1(fixture);
            fixture.wrapped.failuresAfterPausingS1.set(1);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("s1"));
            await().atMost(EVENTUALLY).until(pause::isDone);
            assertThat(fixture.wrapped.failuresAfterPausingS1).as("failures left for the pause of s1 in the wrapped model").hasValue(0);
            fixture.strategy.failuresFromUnregisteringS1.set(1);
            stop.resumeOfN1InTheWrappedModel().open();
            assertThat(stop.resume()).as("the resume of n1").succeedsWithin(EVENTUALLY);

            assertThat(stop.stopped()).as("[stop() applied to s1 by the pause, with unregistering s1 failing after]").failsWithin(EVENTUALLY)
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(expected.getClass()).withMessage(expected.getMessage());
        } finally {
            fixture.wrapped.failuresAfterPausingS1.set(0);
            fixture.strategy.failuresFromUnregisteringS1.set(0);
            tryBeforeItsBackoff.open();
            fixture.model.shutdown();
        }
    }

    // A stop() that waits for a resume of n1 under way in the wrapped model, so it has not stopped the wrapped model,
    // until the test opens the gate
    private record StopWaitingForAResumeOfN1(Gate resumeOfN1InTheWrappedModel, CompletableFuture<Void> resume, CompletableFuture<Void> stopped) {
    }

    private static StopWaitingForAResumeOfN1 stopWaitingForAResumeOfN1(Fixture fixture) {
        fixture.subscribeN1();
        fixture.model.pauseSubscription("n1");
        Gate resumeOfN1InTheWrappedModel = new Gate();
        fixture.wrapped.nextResumeOfN1OnTheTestThread.set(resumeOfN1InTheWrappedModel);
        CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("n1"));
        assertThat(resumeOfN1InTheWrappedModel.awaitEntered()).as("the resume of n1 waits in the wrapped model").isTrue();
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        Thread stopping = startOnALifecycleCallThread(fixture.model::stop, stopped);
        await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> stopped.isDone() || waitsOnAMonitor(stopping));
        assertThat(stopped).as("stop() waits for the resume of n1").isNotDone();
        return new StopWaitingForAResumeOfN1(resumeOfN1InTheWrappedModel, resume, stopped);
    }

    private static Thread startOnALifecycleCallThread(Runnable call, CompletableFuture<Void> called) {
        Thread thread = new Thread(() -> {
            try {
                call.run();
                called.complete(null);
            } catch (Throwable e) {
                called.completeExceptionally(e);
            }
        }, "test-lifecycle-thread");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static boolean waitsOnAMonitor(Thread thread) {
        return thread.getState() == Thread.State.WAITING && Stream.of(thread.getStackTrace())
                .anyMatch(frame -> frame.getClassName().equals(Object.class.getName()) && frame.getMethodName().startsWith("wait"));
    }

    private static boolean waitsForALock(Thread thread) {
        return thread.getState() == Thread.State.WAITING && Stream.of(thread.getStackTrace())
                .anyMatch(frame -> frame.getClassName().equals(ReentrantLock.class.getName()) && frame.getMethodName().equals("lock"));
    }

    private static boolean waitsForItsTurn(Thread thread) {
        return thread.getState() == Thread.State.WAITING && Stream.of(thread.getStackTrace())
                .anyMatch(frame -> frame.getMethodName().equals("awaitUninterruptibly"));
    }

    private static CompletableFuture<Void> runOnTheTestThread(Runnable call) {
        CompletableFuture<Void> called = new CompletableFuture<>();
        startOnTheTestThread(call, called);
        return called;
    }

    // Returns once the call waits for a lock, or has returned or thrown
    private static CompletableFuture<Void> runOnTheTestThreadUntilItWaitsForALock(Runnable call) {
        CompletableFuture<Void> called = new CompletableFuture<>();
        Thread thread = startOnTheTestThread(call, called);
        await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> called.isDone() || waitsForALock(thread));
        return called;
    }

    private static Thread startOnTheTestThread(Runnable call, CompletableFuture<Void> called) {
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
        return thread;
    }

    // What the user call throws is left out, since a resume of s1 running asks the wrapped model, which a stop() that
    // began while the resume waited has stopped already
    private static State withTheLockFree(From from, UserCall userCall, Call later) {
        Fixture fixture = new Fixture(from.initially);
        try {
            from.prepare(fixture.model);
            catchThrowable(() -> userCall.apply(fixture.model));
            later.apply(fixture.model);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    // What the user call throws is left out, since only where s1 ends is compared
    private static State withTheLockFree(From from, Call earlier, UserCall userCall) {
        Fixture fixture = new Fixture(from.initially);
        try {
            from.prepare(fixture.model);
            earlier.apply(fixture.model);
            catchThrowable(() -> userCall.apply(fixture.model));
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    // The start(..) or stop() waits once it has begun, before it gets to s1, while the user call takes the lock of s1.
    // It goes on once the user call has returned, or once the user call waits for that stop() to stop the wrapped model.
    private static State beforeTheStartOrStopGetsToS1(From from, Call earlier, UserCall userCall) {
        Fixture fixture = new Fixture(from.initially);
        try {
            from.prepare(fixture.model);
            Gate begun = new Gate();
            AtomicBoolean first = new AtomicBoolean(true);
            fixture.model.runOnceAStartOrStopHasBegun(() -> {
                if (first.getAndSet(false)) {
                    begun.pass();
                }
            });
            CompletableFuture<Void> applied = CompletableFuture.runAsync(() -> earlier.apply(fixture.model), command -> {
                Thread thread = new Thread(command, "test-lifecycle-thread");
                thread.setDaemon(true);
                thread.start();
            });
            assertThat(begun.awaitEntered()).as("%s has begun", earlier.description).isTrue();
            CompletableFuture<Void> called = new CompletableFuture<>();
            Thread calling = startOnTheTestThread(() -> userCall.apply(fixture.model), called);
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> called.isDone() || waitsOnAMonitor(calling));
            begun.open();
            await().atMost(EVENTUALLY).until(() -> applied.isDone() && called.isDone());
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    // The grant holds the lock of s1 while the user call waits for it, and the later call begins then and is handed over
    private static State whileTheCallWaitsForTheLock(From from, UserCall userCall, Call later) {
        Fixture fixture = new Fixture(from.initially);
        try {
            from.prepare(fixture.model);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            CompletableFuture<Void> called = runOnTheTestThreadUntilItWaitsForALock(() -> userCall.apply(fixture.model));
            assertThat(called).as("%s waits for the lock of s1", userCall.description).isNotDone();
            later.apply(fixture.model);
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            await().atMost(EVENTUALLY).until(called::isDone);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
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
        return noThreadNamed("occurrent-competing-consumer-lifecycle-" + subscriptionId) && noThreadNamed("occurrent-competing-consumer-reconcile-" + subscriptionId);
    }

    private static boolean noThreadNamed(String name) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .noneMatch(name::equals);
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

    private enum From {
        RUNNING("a running model", Initially.RUNNING, null),
        RUNNING_WITH_S1_PAUSED("a running model with s1 paused by the user", Initially.RUNNING, UserCall.PAUSE),
        STOPPED("a stopped model", Initially.STOPPED, null),
        STOPPED_WITH_S1_RESUMED("a stopped model with s1 resumed by the user", Initially.STOPPED, UserCall.RESUME);

        private final String description;
        private final Initially initially;
        private final @Nullable UserCall preparedBy;

        From(String description, Initially initially, @Nullable UserCall preparedBy) {
            this.description = description;
            this.initially = initially;
            this.preparedBy = preparedBy;
        }

        private void prepare(CompetingConsumerSubscriptionModel model) {
            if (preparedBy != null) {
                preparedBy.apply(model);
            }
        }
    }

    private enum UserCall {
        PAUSE("a pause"),
        RESUME("a resume"),
        CANCEL("a cancel");

        private final String description;

        UserCall(String description) {
            this.description = description;
        }

        private void apply(CompetingConsumerSubscriptionModel model) {
            switch (this) {
                case PAUSE -> model.pauseSubscription("s1");
                case RESUME -> model.resumeSubscription("s1");
                case CANCEL -> model.cancelSubscription("s1");
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
        private @Nullable Throwable thrownByStart;

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
            Gate backingOff = handOverAStartOfN1(() -> wrapped.failuresFromResumingN1.set(failures));
            assertThat(thrownByStart).as("what start(true) threw").isNull();
            return backingOff;
        }

        // As handOverAStartOfN1ThatFails, but starting the wrapped model fails as often as given instead. The start(true)
        // tries first and throws that failure, and then the thread n1 is handed to tries. Another node holds the lease of
        // s1, so s1 does not start it.
        private Gate handOverAStartOfN1WhileTheWrappedModelFailsToStart(int failures) {
            Gate backingOff = handOverAStartOfN1(() -> wrapped.failuresFromStarting.set(failures));
            assertThat(thrownByStart).as("what start(true) threw").isInstanceOf(IllegalStateException.class).hasMessage("Starting the wrapped model failed");
            return backingOff;
        }

        private Gate handOverAStartOfN1(Runnable failing) {
            subscribeN1();
            Gate backingOff = handedOverThreadAboutToBackOff();
            Gate pauseHoldingTheLock = new Gate();
            wrapped.nextIsPausedOfN1OnTheTestThread.set(pauseHoldingTheLock);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> model.pauseSubscription("n1"));
            assertThat(pauseHoldingTheLock.awaitEntered()).as("the pause of n1 holds its lock").isTrue();
            failing.run();
            thrownByStart = catchThrowable(() -> model.start(true));
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
        private final AtomicInteger errorsFromUnregisteringS1 = new AtomicInteger();
        private final AtomicInteger failuresFromUnregisteringS1 = new AtomicInteger();
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
            if (subscriptionId.equals("s1") && errorsFromUnregisteringS1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new AssertionError("Unregistering s1 failed on " + Thread.currentThread().getName());
            }
            if (subscriptionId.equals("s1") && failuresFromUnregisteringS1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("Unregistering s1 failed");
            }
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
        // s1 runs on through a pause or a stop of this model, as with a model whose pause fails without throwing
        private final AtomicBoolean s1RunsOnWhateverPausesIt = new AtomicBoolean();
        private final AtomicInteger failuresFromPausingS1 = new AtomicInteger();
        private final AtomicInteger failuresAfterPausingS1 = new AtomicInteger();
        // Each pause of s1 that uses one up makes the next isRunning of s1 fail
        private final AtomicInteger failuresFromIsRunningOfS1AfterItsPause = new AtomicInteger();
        private final AtomicInteger errorsFromIsRunningOfS1OnALifecycleThread = new AtomicInteger();
        private final AtomicInteger failuresFromResumingN1 = new AtomicInteger();
        private final AtomicInteger failuresFromStarting = new AtomicInteger();
        private final AtomicReference<@Nullable Error> errorFromStarting = new AtomicReference<>();
        private final AtomicReference<@Nullable Error> errorFromIsPausedOfN1 = new AtomicReference<>();
        private final Counter resumeFailuresOfN1 = new Counter();
        private final List<String> callsAfterShutdown = new CopyOnWriteArrayList<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;
        private boolean shutDown;
        private boolean isRunningOfS1Fails;

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
            boolean s1RunsOn = s1RunsOnWhateverPausesIt.get() && runningIds.contains("s1");
            pausedIds.addAll(runningIds);
            runningIds.clear();
            if (s1RunsOn) {
                pausedIds.remove("s1");
                runningIds.add("s1");
            }
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            calledAfterShutdown("start");
            if (failuresFromStarting.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("Starting the wrapped model failed");
            }
            Error error = errorFromStarting.get();
            if (error != null) {
                throw error;
            }
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
                if (subscriptionId.equals("s1") && isRunningOfS1Fails) {
                    isRunningOfS1Fails = false;
                    throw new IllegalStateException("isRunning of s1 failed");
                }
                boolean runsOn = subscriptionId.equals("s1") && s1RunsOnWhateverPausesIt.get();
                return (running || runsOn) && runningIds.contains(subscriptionId);
            }
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            if (subscriptionId.equals("n1") && Thread.currentThread().getName().equals(TEST_THREAD)) {
                passIfSet(nextIsPausedOfN1OnTheTestThread);
            }
            Error error = subscriptionId.equals("n1") ? errorFromIsPausedOfN1.get() : null;
            if (error != null) {
                throw error;
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
            if (subscriptionId.equals("s1") && failuresFromPausingS1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("Pausing s1 failed");
            }
            synchronized (this) {
                if (subscriptionId.equals("s1")) {
                    isRunningOfS1Fails = failuresFromIsRunningOfS1AfterItsPause.getAndUpdate(left -> Math.max(0, left - 1)) > 0;
                }
                if (subscriptionId.equals("s1") && s1RunsOnWhateverPausesIt.get()) {
                    return;
                }
                if (runningIds.remove(subscriptionId)) {
                    pausedIds.add(subscriptionId);
                }
            }
            if (subscriptionId.equals("s1") && failuresAfterPausingS1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("Pausing s1 failed after it was paused");
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
