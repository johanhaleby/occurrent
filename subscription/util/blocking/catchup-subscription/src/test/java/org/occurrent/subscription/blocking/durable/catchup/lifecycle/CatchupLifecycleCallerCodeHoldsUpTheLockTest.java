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
 * A {@code StartAt.dynamic} function that a subscribe handed over by an action asks under the model lock, and that waits
 * for a lifecycle call made on another thread, does not deadlock with that call. The call gives up after
 * {@code LIFECYCLE_WAIT} and throws {@link IllegalStateException}, changing nothing, and a later lifecycle call completes.
 * A {@code shutdown()} throws too, and hands the lifecycle thread one shutdown of the wrapped model that runs once the
 * function has returned, with no further call needed. The wrapped model is shut down once, whichever other
 * {@code shutdown()} is made before or after that one.
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

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch functionReturned = new CountDownLatch(1);
    // The function waits for this once the call it waited for has ended, so a test can look at the wrapped model while the caller's code still holds the lock
    private volatile CountDownLatch functionMayReturn = new CountDownLatch(0);
    private final CountDownLatch wrappedShutdownEntered = new CountDownLatch(1);
    private final CountDownLatch wrappedShutdownMayReturn = new CountDownLatch(1);
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
    void a_shutdown_from_another_thread_throws_instead_of_waiting_for_the_function_and_the_wrapped_model_is_shut_down_once_the_function_returns_with_no_further_call() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a shutdown from another thread and then waits to be let go
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);

        // When the shutdown has thrown and the function still holds the model lock
        awaitTheFunction();

        // Then the shutdown threw after the wait, and the wrapped model is not shut down and y still runs in it
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got from the shutdown that gave up").isZero();
        assertThat(live.isRunning(Y)).as("whether y still runs in the wrapped model after the shutdown that gave up").isTrue();

        // And once the function returns, with no further call, the wrapped model is shut down once and y stops
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
        assertThat(live.isRunning(Y)).as("whether y still runs in the wrapped model once the handed shutdown has run").isFalse();
    }

    @Test
    void a_second_shutdown_while_the_function_still_holds_the_lock_throws_too_and_the_wrapped_model_is_shut_down_once_when_the_function_returns() throws Exception {
        // Given a shutdown that threw because the function, which still holds the model lock, waited for it
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());

        // When a second shutdown is made from another thread while the function still holds the lock
        Call second = new Call("second-shutdown", model::shutdown);

        // Then it throws after the wait too, and the wrapped model has got no shutdown
        assertThat(second.endedWithin(BOUND)).as("whether the second shutdown ended within " + BOUND + " while the function held the lock").isTrue();
        assertTheShutdownThrewAfterTheWait(second);
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got while the function held the lock").isZero();

        // And once the function returns, the wrapped model is shut down once, as only one shutdown was handed over
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_shutdown_made_after_the_handed_one_has_run_does_not_shut_the_wrapped_model_down_again() throws Exception {
        // Given a shutdown that threw because the function waited for it, and the shutdown it handed over has run
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();

        // When a later shutdown is made from another thread
        assertThat(CompletableFuture.runAsync(model::shutdown, daemonExecutor("later-shutdown"))).as("a shutdown made after the handed one has run").succeedsWithin(BOUND);

        // Then the wrapped model is still shut down once in total
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_shutdown_made_while_the_handed_one_is_shutting_the_wrapped_model_down_waits_for_it_and_the_wrapped_model_is_shut_down_once() throws Exception {
        // Given a wrapped model whose shutdown waits to be let go, and a shutdown that threw because the function waited for it
        live.beforeShutdown = () -> {
            wrappedShutdownEntered.countDown();
            awaitLatch(wrappedShutdownMayReturn);
        };
        functionMayReturn = new CountDownLatch(1);
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());

        // When the function returns and, with the handed shutdown inside the wrapped model under the model lock, a second shutdown is made
        functionMayReturn.countDown();
        assertThat(wrappedShutdownEntered.await(20, TimeUnit.SECONDS)).as("whether the handed shutdown reached the wrapped model once the function returned").isTrue();
        Call second = new Call("second-shutdown", model::shutdown);
        await().atMost(BOUND).untilAsserted(() -> assertThat(second.state()).as("the state of the thread of the second shutdown, which waits for the model lock").isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING));
        wrappedShutdownMayReturn.countDown();

        // Then the second shutdown returns without error, and the wrapped model is shut down once
        assertThat(second.endedWithin(BOUND)).as("whether the second shutdown returned once the handed one had shut the wrapped model down").isTrue();
        assertThat(second.thrown()).as("what the second shutdown threw").isNull();
        assertTheWrappedModelIsShutDownOnce();
    }

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // the lifecycle thread under the model lock and asks the function there. The function runs call on another thread
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

    private void assertTheCallThrewBecauseOfCallersCode(String call) {
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the call ended within " + BOUND + " while the caller's code waited for it").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what " + call + " threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot run " + call)
                .hasMessageContaining("because code the caller supplied")
                .hasMessageEndingWith("Nothing was changed.");
    }

    private void assertTheShutdownThrewAfterTheWait(Call shutdown) {
        assertThat(shutdown.elapsed()).as("how long the shutdown took to give up").isLessThan(WAIT.plusSeconds(5));
        assertThat(shutdown.thrown()).as("what shutdown() threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot run shutdown()")
                .hasMessageContaining("because code the caller supplied")
                .hasMessageContaining("The wrapped subscription model isn't shut down yet")
                .hasMessageEndingWith("Then the wrapped model is shut down under the model lock, with no further call needed.");
    }

    // The handed shutdown runs once the function returned, and nothing then shuts the wrapped model down a second time
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
