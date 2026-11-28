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
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A {@code StartAt.dynamic} function that a subscribe handed over by an action asks runs on a thread of the handed call,
 * without the model lock. When it waits for a lifecycle call made on another thread, that call does not deadlock with it:
 * {@code stop()}, {@code start(..)} and {@code shutdown()} complete without waiting for the function, and a resume of the
 * subscription being subscribed waits for its claim up to {@code LIFECYCLE_WAIT} and then throws
 * {@link IllegalStateException}, changing nothing. A later lifecycle call completes. The wrapped model is shut down once,
 * whichever other {@code shutdown()} is made before or after that one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class CatchupLifecycleCallerCodeHoldsUpTheLockTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    private static final String W = "w";
    // The fix answers in about LIFECYCLE_WAIT (10 s), so a call that has not ended after this is a hang
    private static final Duration BOUND = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch functionReturned = new CountDownLatch(1);
    // The function waits for this once the call it waited for has ended, so a test can look at the wrapped model while the caller's code is still running
    private volatile CountDownLatch functionMayReturn = new CountDownLatch(0);
    private final CountDownLatch wrappedShutdownEntered = new CountDownLatch(1);
    private final CountDownLatch wrappedShutdownMayReturn = new CountDownLatch(1);
    private final CountDownLatch functionHoldsTheLock = new CountDownLatch(1);
    private final CountDownLatch liveActionHandedACallOver = new CountDownLatch(1);
    private final CountDownLatch liveActionReturned = new CountDownLatch(1);
    private final AtomicBoolean liveActionSawTheWrappedShutdown = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrownByTheLiveAction = new AtomicReference<>();
    private final AtomicBoolean functionAsked = new AtomicBoolean();
    private final AtomicBoolean callEndedWhileTheFunctionWaited = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfX = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfZ = new AtomicReference<>();
    private final AtomicReference<@Nullable Call> callOfTheFunction = new AtomicReference<>();

    @AfterEach
    void shut_down() throws Exception {
        functionMayReturn.countDown();
        wrappedShutdownMayReturn.countDown();
        liveActionHandedACallOver.countDown();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_from_another_thread_of_the_subscribe_being_asked_its_function_throws_when_the_claim_of_the_subscription_is_not_free_within_the_wait_and_a_later_resume_completes() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the resume throws after the wait for the claim of z and changes nothing
        assertTheCallThrewBecauseTheClaimOfTheSubscriptionWasNotFree("resumeSubscription(" + Z + ")");
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_resume_at_a_given_position_from_another_thread_throws_when_the_claim_of_the_subscription_is_not_free_within_the_wait() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z at a given position from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z, StartAt.now()));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the resume throws after the wait for the claim of z and changes nothing
        assertTheCallThrewBecauseTheClaimOfTheSubscriptionWasNotFree("resumeSubscription(" + Z);
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_stop_from_another_thread_completes_without_waiting_for_the_function_of_the_handed_subscribe() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a stop from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::stop);

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the stop completed at once, as the function does not hold the model lock, and the model is stopped
        assertTheCallCompletedWithoutWaitingForTheFunction("stop()");
        assertThat(model.isRunning()).as("whether the model runs after the stop that completed").isFalse();
        assertTheSubscribeOfZRanAndZIsPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_start_from_another_thread_completes_without_waiting_for_the_function_of_the_handed_subscribe_and_the_model_still_runs() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a start from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.start(false));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the start completed at once, as the function does not hold the model lock, and the model runs
        assertTheCallCompletedWithoutWaitingForTheFunction("start(..)");
        assertThat(model.isRunning()).as("whether the model runs after the start that completed").isTrue();
        assertTheHandedCallsRanAndZIsStillPaused();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_shutdown_from_another_thread_completes_without_waiting_for_the_function_of_the_handed_subscribe_and_shuts_the_wrapped_model_down_once() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a shutdown from another thread and then waits to be let go
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);

        // When the shutdown has returned and the function is still running
        awaitTheFunction();

        // Then the shutdown completed at once and the wrapped model is shut down once and y stops in it
        assertTheCallCompletedWithoutWaitingForTheFunction("shutdown()");
        assertTheWrappedModelIsShutDownOnce();
        assertThat(live.isRunning(Y)).as("whether y still runs in the wrapped model after the shutdown").isFalse();

        // And the wrapped model is still shut down once when the function returns
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_second_shutdown_while_the_function_is_still_running_completes_too_and_the_wrapped_model_is_shut_down_once() throws Exception {
        // Given a shutdown that completed while the function was still running
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        assertTheCallCompletedWithoutWaitingForTheFunction("shutdown()");

        // When a second shutdown is made from another thread while the function is still running
        Call second = new Call("second-shutdown", model::shutdown);

        // Then it completes without error, and the wrapped model has got one shutdown
        assertThat(second.endedWithin(BOUND)).as("whether the second shutdown ended within " + BOUND + " while the function was still running").isTrue();
        assertThat(second.thrown()).as("what the second shutdown threw").isNull();
        assertTheWrappedModelIsShutDownOnce();

        // And once the function returns, the wrapped model has still been shut down once
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_shutdown_made_after_the_first_one_has_completed_does_not_shut_the_wrapped_model_down_again() throws Exception {
        // Given a shutdown that completed while the function waited for it, and the function has returned
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();

        // When a later shutdown is made from another thread
        assertThat(CompletableFuture.runAsync(model::shutdown, daemonExecutor("later-shutdown"))).as("a shutdown made after the first one has completed").succeedsWithin(BOUND);

        // Then the wrapped model is still shut down once in total
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_shutdown_made_while_the_first_one_is_shutting_the_wrapped_model_down_waits_for_it_and_the_wrapped_model_is_shut_down_once() throws Exception {
        // Given a wrapped model whose shutdown waits to be let go, and a shutdown the function makes that reaches it
        live.beforeShutdown = () -> {
            wrappedShutdownEntered.countDown();
            awaitLatch(wrappedShutdownMayReturn);
        };
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        assertThat(wrappedShutdownEntered.await(20, TimeUnit.SECONDS)).as("whether the shutdown of the function reached the wrapped model").isTrue();

        // When a second shutdown is made while the first is inside the wrapped model under the model lock
        Call second = new Call("second-shutdown", model::shutdown);
        await().atMost(BOUND).untilAsserted(() -> assertThat(second.state()).as("the state of the thread of the second shutdown, which waits for the model lock").isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING));
        wrappedShutdownMayReturn.countDown();

        // Then the second shutdown returns without error, and the wrapped model is shut down once
        assertThat(second.endedWithin(BOUND)).as("whether the second shutdown returned once the first had shut the wrapped model down").isTrue();
        assertThat(second.thrown()).as("what the second shutdown threw").isNull();
        awaitTheFunction();
        assertTheCallCompletedWithoutWaitingForTheFunction("shutdown()");
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_stop_handed_over_from_a_live_action_does_not_keep_a_shutdown_made_while_the_function_runs_from_shutting_the_wrapped_model_down() throws Exception {
        // Given a function that runs on the thread of the handed subscribe, and waits for a shutdown once a live action of w has handed a stop over
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        live.beforeShutdown = wrappedShutdownEntered::countDown;
        functionMayReturn = new CountDownLatch(1);
        model.subscribe(W, null, StartAt.now(), event -> {
            if (!event.getId().equals("live-2")) {
                return;
            }
            try {
                model.stop();
                liveActionHandedACallOver.countDown();
                liveActionSawTheWrappedShutdown.set(wrappedShutdownEntered.await(30, TimeUnit.SECONDS));
            } catch (Throwable e) {
                thrownByTheLiveAction.set(e);
            } finally {
                liveActionReturned.countDown();
            }
        });
        StartAt waits = StartAt.dynamic(() -> {
            if (functionAsked.compareAndSet(false, true)) {
                functionHoldsTheLock.countDown();
                awaitLatch(liveActionHandedACallOver);
                Call started = new Call("call-the-function-waits-for", model::shutdown);
                callOfTheFunction.set(started);
                callEndedWhileTheFunctionWaited.set(started.endedWithin(BOUND));
                functionReturned.countDown();
                awaitLatch(functionMayReturn);
            }
            return StartAt.now();
        });
        model.subscribe(Y, null, StartAt.now(), event -> {
            if (!event.getId().equals("live-1")) {
                return;
            }
            try {
                // The resume is handed over, so the subscribe of z runs behind it on a thread of its own, occurrent-catchup-handed-call, and not on this one
                handleOfX.set(model.resumeSubscription(X));
                handleOfZ.set(model.subscribePaused(Z, null, waits, e -> {
                }));
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            } finally {
                actionReturned.countDown();
            }
        });
        daemonExecutor("live-delivery-1").execute(() -> live.publish(cloudEvent("live-1")));
        assertThat(functionHoldsTheLock.await(10, TimeUnit.SECONDS)).as("whether the function of z is running on the thread of the handed subscribe").isTrue();
        // Delivered to w alone and without the monitor of the live model, which publish holds while the action runs, since the
        // subscribe of z the handed call finishes once the function returns needs it
        daemonExecutor("live-delivery-2").execute(() -> live.subscriptions.get(W).receive(cloudEvent("live-2")));

        // When the shutdown has completed while the function is still running, and then the function returns
        awaitTheFunction();
        assertTheCallCompletedWithoutWaitingForTheFunction("shutdown()");
        functionMayReturn.countDown();

        // Then the wrapped model is shut down once, whatever the stop the live action handed over does, and the live action returns
        assertTheWrappedModelIsShutDownOnce();
        assertThat(liveActionReturned.await(10, TimeUnit.SECONDS)).as("whether the live action of w returned once the wrapped model was shut down").isTrue();
        assertThat(liveActionSawTheWrappedShutdown.get()).as("whether the live action of w saw the wrapped model shut down before it gave up waiting").isTrue();
        assertThat(thrownByTheLiveAction.get()).as("what the live action of w threw").isNull();
        assertThat(thrownByTheAction.get()).as("what the action of y threw").isNull();
    }

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // a thread of its own, without the model lock, and asks the function there. The function runs call on another thread
    // and waits for it, which is the wait that deadlocks without the fix
    private void actionResumesXThenSubscribesZWhoseFunctionWaitsFor(Runnable call) throws Exception {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
        StartAt waits = StartAt.dynamic(() -> {
            if (functionAsked.compareAndSet(false, true)) {
                Call started = new Call("call-the-function-waits-for", call);
                callOfTheFunction.set(started);
                callEndedWhileTheFunctionWaited.set(started.endedWithin(BOUND));
                functionReturned.countDown();
                awaitLatch(functionMayReturn);
            }
            return StartAt.now();
        });
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

    // Another call for the id waits for the claim the handed subscribe holds while the function runs, and gives up after the wait
    private void assertTheCallThrewBecauseTheClaimOfTheSubscriptionWasNotFree(String call) {
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether " + call + " ended within " + BOUND + " while the function waited for it").isTrue();
        Call made = callOfTheFunction.get();
        assertThat(made.elapsed()).as("how long " + call + " waited for the claim of the subscription before giving up").isBetween(WAIT.minusSeconds(1), WAIT.plusSeconds(5));
        assertThat(made.thrown()).as("what " + call + " threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot change subscription " + Z)
                .hasMessageContaining("another call for it didn't return within 10000 ms");
    }

    // A stop, start or shutdown does not wait for the function, which runs on the thread of the handed subscribe without the model lock
    private void assertTheCallCompletedWithoutWaitingForTheFunction(String call) {
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether " + call + " ended within " + BOUND + " while the function waited for it").isTrue();
        Call made = callOfTheFunction.get();
        assertThat(made.thrown()).as("what " + call + " threw").isNull();
        assertThat(made.elapsed()).as("how long " + call + " took, while the function waited for it").isLessThan(WAIT);
    }

    // Nothing shuts the wrapped model down a second time
    private void assertTheWrappedModelIsShutDownOnce() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got once the function returned").isEqualTo(1));
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got after the first one").isEqualTo(1));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void assertTheHandedCallsRanAndZIsStillPaused() throws Exception {
        assertTheSubscribeOfZRanAndZIsPaused();
        assertThat(handleOfX.get().waitUntilStarted(WAIT)).as("whether the handle of the resume of x that the action handed over completes started").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume the action handed over").isTrue();
    }

    // The subscribe of z, handed over by the action, completes once the function returned and z reads as paused
    private void assertTheSubscribeOfZRanAndZIsPaused() {
        await().atMost(WAIT).untilAsserted(() -> assertThat(model.isPaused(Z)).as("whether z is paused once the handed subscribe returned").isTrue());
        assertThat(handleOfZ.get()).as("the handle the subscribe of z that waited for the call returned to the action").isNotNull();
    }

    private void assertLaterLifecycleCallsComplete() {
        assertThat(CompletableFuture.runAsync(() -> model.resumeSubscription(Z), daemonExecutor("later-resume-of-z"))).as("a later resume of z").succeedsWithin(BOUND);
        assertThat(model.isRunning(Z)).as("whether z runs after the later resume").isTrue();
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("later-stop"))).as("a later stop").succeedsWithin(BOUND);
        assertThat(model.isRunning()).as("whether the model runs after the later stop").isFalse();
    }

    // One call on a thread of its own, which records what it threw
    private static final class Call {
        private final Thread thread;
        private final long startedAt = System.nanoTime();
        private volatile long endedAt;
        private final CountDownLatch ended = new CountDownLatch(1);
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        Call(String name, Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                } finally {
                    endedAt = System.nanoTime();
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

        Thread.State state() {
            return thread.getState();
        }

        Duration elapsed() {
            return Duration.ofNanos(endedAt - startedAt);
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
