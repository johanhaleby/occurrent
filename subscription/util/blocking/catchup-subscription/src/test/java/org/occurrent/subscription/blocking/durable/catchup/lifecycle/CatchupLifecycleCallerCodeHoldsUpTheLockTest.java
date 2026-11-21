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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.jspecify.annotations.Nullable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A {@code StartAt.dynamic} function that a subscribe handed over by an action asks under the model lock, and that waits
 * for a lifecycle call made on another thread, does not deadlock with that call. The call gives up after
 * {@code LIFECYCLE_WAIT} and throws {@link IllegalStateException}, changing nothing, and a later lifecycle call completes.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class CatchupLifecycleCallerCodeHoldsUpTheLockTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    // The fix answers in about LIFECYCLE_WAIT (10 s), so a call that has not ended after this is a hang
    private static final Duration BOUND = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(10);
    // Far below the 30 s a held shutdown() lets go of its hold after, so a stop that waits for the held shutdown() cannot end within it by the hold expiring. A stop needs no LIFECYCLE_WAIT when the lock is free
    private static final Duration STOP_BOUND = Duration.ofSeconds(5);

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch functionReturned = new CountDownLatch(1);
    private final AtomicBoolean functionAsked = new AtomicBoolean();
    private final AtomicBoolean callEndedWhileTheFunctionWaited = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfX = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfZ = new AtomicReference<>();
    private final AtomicReference<@Nullable Call> callOfTheFunction = new AtomicReference<>();

    @AfterEach
    void shut_down() throws Exception {
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_from_another_thread_of_the_subscribe_being_asked_its_function_throws_instead_of_waiting_for_the_function_and_a_later_resume_completes() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z));

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the resume throws after the wait and changes nothing
        assertTheCallThrewBecauseOfCallersCode("resumeSubscription(" + Z + ")");
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_resume_at_a_given_position_from_another_thread_throws_instead_of_waiting_for_the_function() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z at a given position from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z, StartAt.now()));

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the resume throws after the wait and changes nothing
        assertTheCallThrewBecauseOfCallersCode("resumeSubscription(" + Z);
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_stop_from_another_thread_throws_instead_of_waiting_for_the_function_and_the_model_still_runs() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a stop from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::stop);

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the stop throws after the wait and the model still runs
        assertTheCallThrewBecauseOfCallersCode("stop()");
        assertThat(model.isRunning()).as("whether the model still runs after the stop that gave up").isTrue();
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_start_from_another_thread_throws_instead_of_waiting_for_the_function_and_the_model_still_runs() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a start from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.start(false));

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the start throws after the wait and the model still runs
        assertTheCallThrewBecauseOfCallersCode("start(..)");
        assertThat(model.isRunning()).as("whether the model still runs after the start that gave up").isTrue();
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_shutdown_that_gave_up_while_the_live_model_asks_the_function_leaves_no_subscription_in_either_model_once_the_subscribe_has_unwound() throws Exception {
        // Given a live model that asks the function before it registers a subscription, and an action that resumes x and then subscribes z paused,
        // whose function answers the catch-up model's own ask and waits for a shutdown from another thread when the live model asks
        live.asksTheStartAtAndRejectsOnceShutDown = true;
        actionResumesXThenSubscribesZ(StartAt.dynamic(context -> {
            if (context.subscriptionModelType() == ControllableLiveModel.AsksAsAWrapper.class) {
                waitForTheCall(model::shutdown);
            }
            return StartAt.now();
        }));

        // When the shutdown gave up on the lock and returned, the function returned, and the subscribe of z has unwound behind the handed calls
        awaitTheFunction();
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the shutdown returned within " + BOUND + " while the function waited for it").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what the shutdown threw").isNull();
        catchThrowable(() -> model.resumeSubscription("nobody"));

        // Then neither model holds z, which the live model registered after the shutdown had returned
        assertThat(live.subscriptions).as("the subscriptions the live model holds").doesNotContainKey(Z);
        assertThat(live.isRunning(Z)).as("whether the live model runs z").isFalse();
        assertThat(live.isPaused(Z)).as("whether the live model holds z paused").isFalse();
        assertThat(model.isRunning(Z)).as("whether the catch-up model runs z").isFalse();
        assertThat(model.isPaused(Z)).as("whether the catch-up model holds z paused").isFalse();
    }

    @Test
    void a_live_model_with_a_serialized_shutdown_held_in_the_shutdown_without_the_lock_is_not_waited_on_by_the_lock_holder_or_a_lifecycle_call_and_holds_no_subscription_once_released() throws Exception {
        // Given a live model whose shutdown() is serialized and held, only the first, inside it after it dropped the subscriptions,
        // and an action that resumes x and then subscribes z paused, whose function, asked by the live model, runs a shutdown from another thread
        // that gave up on the lock and returns only once that shutdown is held, so the lock holder, the lifecycle thread running the subscribe of z, unwinds
        CountDownLatch locklessShutdownHeld = new CountDownLatch(1);
        CountDownLatch releaseTheLocklessShutdown = new CountDownLatch(1);
        AtomicBoolean firstShutdown = new AtomicBoolean();
        AtomicBoolean heldWithinTheBound = new AtomicBoolean();
        live.asksTheStartAtAndRejectsOnceShutDown = true;
        live.serializesShutdown = true;
        live.afterShutdown = () -> {
            if (firstShutdown.compareAndSet(false, true)) {
                locklessShutdownHeld.countDown();
                awaitTheLatch(releaseTheLocklessShutdown);
            }
        };
        try {
            actionResumesXThenSubscribesZ(StartAt.dynamic(context -> {
                if (context.subscriptionModelType() == ControllableLiveModel.AsksAsAWrapper.class && functionAsked.compareAndSet(false, true)) {
                    callOfTheFunction.set(new Call("shutdown-without-the-lock", model::shutdown));
                    heldWithinTheBound.set(awaitTheLatch(locklessShutdownHeld));
                    functionReturned.countDown();
                }
                return StartAt.now();
            }));
            awaitTheFunction();
            assertThat(heldWithinTheBound.get()).as("whether the shutdown that gave up on the lock was held in the live model's shutdown() within " + BOUND + " after it dropped the subscriptions").isTrue();

            // When a lifecycle call from another thread runs while the shutdown is held, which needs the lock the holder let go
            assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("stop-while-the-shutdown-is-held"))).as("a stop while the shutdown without the lock is held in the live model's shutdown()").succeedsWithin(BOUND);

            // Then no thread but the shutdown's own has entered or waits to enter the live model's shutdown()
            assertThat(live.shutdownAttempts).as("the threads that entered or wait to enter the live model's shutdown() while the shutdown without the lock is held").containsExactly("shutdown-without-the-lock");
        } finally {
            releaseTheLocklessShutdown.countDown();
        }

        // And once the shutdown is released, neither model holds z with no further lifecycle call
        assertThat(callOfTheFunction.get().endedWithin(BOUND)).as("whether the shutdown returned within " + BOUND + " once its live shutdown() was released").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what the shutdown threw").isNull();
        Awaitility.await().atMost(BOUND).untilAsserted(() -> {
            assertThat(live.subscriptions).as("the subscriptions the live model holds").doesNotContainKey(Z);
            assertThat(live.isRunning(Z)).as("whether the live model runs z").isFalse();
            assertThat(live.isPaused(Z)).as("whether the live model holds z paused").isFalse();
            assertThat(model.isRunning(Z)).as("whether the catch-up model runs z").isFalse();
            assertThat(model.isPaused(Z)).as("whether the catch-up model holds z paused").isFalse();
        });
    }

    @Test
    void a_stop_from_another_thread_completes_while_a_second_shutdown_is_held_in_a_serialized_live_shutdown_after_a_first_one_returned_and_neither_model_holds_z_once_it_is_released() throws Exception {
        // Given a live model whose shutdown() is serialized, a shutdown that gave up on the lock and returned, and a second shutdown that gave up on it too and is held inside
        // the live shutdown() after it dropped the subscriptions, and a function, asked by the live model, that returns only then, so the lock holder, the lifecycle thread running the subscribe of z, unwinds
        CountDownLatch locklessShutdownHeld = new CountDownLatch(1);
        CountDownLatch releaseTheLocklessShutdown = new CountDownLatch(1);
        AtomicBoolean firstShutdownReturned = new AtomicBoolean();
        AtomicBoolean heldWithinTheBound = new AtomicBoolean();
        live.asksTheStartAtAndRejectsOnceShutDown = true;
        live.serializesShutdown = true;
        live.afterShutdown = () -> {
            if (Thread.currentThread().getName().equals("second-shutdown-without-the-lock")) {
                locklessShutdownHeld.countDown();
                awaitTheLatch(releaseTheLocklessShutdown);
            }
        };
        try {
            actionResumesXThenSubscribesZ(StartAt.dynamic(context -> {
                if (context.subscriptionModelType() == ControllableLiveModel.AsksAsAWrapper.class && functionAsked.compareAndSet(false, true)) {
                    Call first = new Call("first-shutdown-without-the-lock", model::shutdown);
                    firstShutdownReturned.set(first.endedWithin(BOUND) && first.thrown() == null);
                    callOfTheFunction.set(new Call("second-shutdown-without-the-lock", model::shutdown));
                    heldWithinTheBound.set(awaitTheLatch(locklessShutdownHeld));
                    functionReturned.countDown();
                }
                return StartAt.now();
            }));
            awaitTheFunction();
            assertThat(firstShutdownReturned.get()).as("whether the first shutdown that gave up on the lock returned without an exception within " + BOUND + " while the function waited for it").isTrue();
            assertThat(heldWithinTheBound.get()).as("whether the second shutdown that gave up on the lock was held in the live model's shutdown() within " + BOUND + " after it dropped the subscriptions").isTrue();

            // When a lifecycle call from another thread runs while the second shutdown is held, which needs the lock the lifecycle thread must not hold while it waits for the serialized live shutdown()
            assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("stop-while-the-second-shutdown-is-held"))).as("a stop while the second shutdown without the lock is held in the live model's shutdown()").succeedsWithin(STOP_BOUND);

            // Then no thread but the two shutdowns' own has entered or waits to enter the live model's shutdown()
            assertThat(live.shutdownAttempts).as("the threads that entered or wait to enter the live model's shutdown() while the second shutdown without the lock is held").containsExactlyInAnyOrder("first-shutdown-without-the-lock", "second-shutdown-without-the-lock");
        } finally {
            releaseTheLocklessShutdown.countDown();
        }

        // And once the second shutdown is released, neither model holds z with no further lifecycle call
        assertThat(callOfTheFunction.get().endedWithin(BOUND)).as("whether the second shutdown returned within " + BOUND + " once its live shutdown() was released").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what the second shutdown threw").isNull();
        Awaitility.await().atMost(BOUND).untilAsserted(() -> {
            assertThat(live.subscriptions).as("the subscriptions the live model holds").doesNotContainKey(Z);
            assertThat(live.isRunning(Z)).as("whether the live model runs z").isFalse();
            assertThat(live.isPaused(Z)).as("whether the live model holds z paused").isFalse();
            assertThat(model.isRunning(Z)).as("whether the catch-up model runs z").isFalse();
            assertThat(model.isPaused(Z)).as("whether the catch-up model holds z paused").isFalse();
        });
    }

    private static boolean awaitTheLatch(CountDownLatch latch) {
        try {
            return latch.await(BOUND.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // the lifecycle thread under the model lock and asks the function there. The function runs call on another thread
    // and waits for it, which is the wait that deadlocks without the fix
    private void actionResumesXThenSubscribesZWhoseFunctionWaitsFor(Runnable call) throws Exception {
        actionResumesXThenSubscribesZ(StartAt.dynamic(() -> {
            waitForTheCall(call);
            return StartAt.now();
        }));
    }

    // Runs on the thread that holds the model lock and asks the caller's code, and waits for the call once
    private void waitForTheCall(Runnable call) {
        if (functionAsked.compareAndSet(false, true)) {
            Call started = new Call("call-the-function-waits-for", call);
            callOfTheFunction.set(started);
            callEndedWhileTheFunctionWaited.set(started.endedWithin(BOUND));
            functionReturned.countDown();
        }
    }

    private void actionResumesXThenSubscribesZ(StartAt waits) throws Exception {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
        model.subscribe(Y, null, StartAt.now(), event -> {
            try {
                handleOfX.set(model.resumeSubscription(X));
                handleOfZ.set(model.subscribePaused(Z, null, waits, e -> {
                }));
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            } finally {
                actionReturned.countDown();
            }
        });
        daemonExecutor("live-delivery-to-y").execute(() -> live.publish(cloudEvent("live-1")));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        assertThat(thrownByTheAction.get()).as("what the action of y threw").isNull();
    }

    private void awaitTheFunction() throws InterruptedException {
        assertThat(functionReturned.await(BOUND.plusSeconds(15).toSeconds(), TimeUnit.SECONDS)).as("whether the caller's code that waited for the call returned").isTrue();
    }

    private void assertTheCallThrewBecauseOfCallersCode(String call) {
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the call ended within " + BOUND + " while the caller's code waited for it").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what " + call + " threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot run " + call)
                .hasMessageContaining("because code the caller supplied")
                .hasMessageEndingWith("Nothing was changed.");
    }

    private void assertTheHandedCallsRanAndZIsStillPaused() throws Exception {
        // A resume of a subscription nobody made runs behind the subscribe of z, which the lifecycle thread runs once the function returned
        assertThat(catchThrowable(() -> model.resumeSubscription("nobody"))).as("what a resume of a subscription nobody made threw").isInstanceOf(UnknownSubscriptionException.class);
        assertThat(handleOfZ.get()).as("the handle the subscribe of z that waited for the call returned to the action").isNotNull();
        assertThat(handleOfX.get().waitUntilStarted(WAIT)).as("whether the handle of the resume of x that the action handed over completes started").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume the action handed over").isTrue();
        assertThat(model.isPaused(Z)).as("whether z is still paused, as the call that gave up changed nothing").isTrue();
    }

    private void assertLaterLifecycleCallsComplete() {
        assertThat(CompletableFuture.runAsync(() -> model.resumeSubscription(Z), daemonExecutor("later-resume-of-z"))).as("a later resume of z").succeedsWithin(BOUND);
        assertThat(model.isRunning(Z)).as("whether z runs after the later resume").isTrue();
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("later-stop"))).as("a later stop").succeedsWithin(BOUND);
        assertThat(model.isRunning()).as("whether the model runs after the later stop").isFalse();
    }

    // One call on a thread of its own, which records what it threw
    private static final class Call {
        private final CountDownLatch ended = new CountDownLatch(1);
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        Call(String name, Runnable body) {
            Thread thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                } finally {
                    ended.countDown();
                }
            }, name);
            thread.setDaemon(true);
            thread.start();
        }

        boolean endedWithin(Duration bound) {
            try {
                return ended.await(bound.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Nullable Throwable thrown() {
            return thrown.get();
        }
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }
}
