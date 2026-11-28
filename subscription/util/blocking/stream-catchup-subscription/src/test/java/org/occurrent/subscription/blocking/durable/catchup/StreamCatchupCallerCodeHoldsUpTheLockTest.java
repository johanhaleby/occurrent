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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code StartAt.dynamic} function that a subscribe handed over by an action asks runs on a thread of the handed
 * call, without the model lock. {@code stop()}, {@code start(..)} and {@code shutdown()} made on another thread complete
 * without waiting for it. A {@link CatchupListener} still runs under the model lock, and a lifecycle call that waits for
 * it gives up after {@code LIFECYCLE_WAIT} and throws {@link IllegalStateException}, changing nothing, and a later
 * lifecycle call completes. A {@code shutdown()} that threw leaves the wrapped model running, since it has ended the
 * catch-up replays and marked the model as shutting down before it waited, and hands the lifecycle thread one shutdown
 * of the wrapped model that runs once the code has returned, with no further call needed. The wrapped model is shut
 * down once, whichever other {@code shutdown()} is made before or after that one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class StreamCatchupCallerCodeHoldsUpTheLockTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    // The fix answers in about LIFECYCLE_WAIT (10 s), so a call that has not ended after this is a hang
    private static final Duration BOUND = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch functionReturned = new CountDownLatch(1);
    // The function waits for this once the call it waited for has ended, so a test can look at the wrapped model while the caller's code is still running
    private volatile CountDownLatch functionMayReturn = new CountDownLatch(0);
    private final CountDownLatch wrappedShutdownEntered = new CountDownLatch(1);
    private final CountDownLatch wrappedShutdownMayReturn = new CountDownLatch(1);
    private final CountDownLatch listenerHoldsTheLock = new CountDownLatch(1);
    private final CountDownLatch actionHandedACallOver = new CountDownLatch(1);
    private final AtomicBoolean actionSawTheWrappedShutdown = new AtomicBoolean();
    // Holds the thread of the one call under test between the handed calls and the model lock, see givenACallIsHeldBeforeTheModelLock
    private final AtomicBoolean holdTheNextCallBeforeTheLock = new AtomicBoolean();
    private final CountDownLatch callIsHeldBeforeTheLock = new CountDownLatch(1);
    private final CountDownLatch callMayTakeTheLock = new CountDownLatch(1);
    private final AtomicReference<@Nullable Call> heldCall = new AtomicReference<>();
    // Runs on the thread that asks the function, a handed-call thread without the model lock, once the call the function waited for has ended
    private volatile Runnable onTheThreadHoldingTheLock = () -> {
    };
    private final AtomicReference<@Nullable Throwable> thrownUnderTheLock = new AtomicReference<>();
    private final AtomicInteger stopsSeenUnderTheLock = new AtomicInteger(-1);
    private final AtomicBoolean functionAsked = new AtomicBoolean();
    private final AtomicBoolean callEndedWhileTheFunctionWaited = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfX = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfZ = new AtomicReference<>();
    private final AtomicReference<@Nullable Call> callOfTheFunction = new AtomicReference<>();
    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel live;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model() {
        inMemory = new InMemorySubscriptionModel();
        live = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shut_down() throws Exception {
        functionMayReturn.countDown();
        wrappedShutdownMayReturn.countDown();
        actionHandedACallOver.countDown();
        callMayTakeTheLock.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_of_the_subscribe_being_asked_its_function_from_another_thread_throws_when_the_claim_of_the_subscription_is_not_free_within_the_wait_and_the_subscribe_then_completes() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the resume throws after the wait for the claim of z and changes nothing
        assertTheCallThrewBecauseTheClaimOfTheSubscriptionWasNotFree("resumeSubscription(" + Z + ")");
        assertTheHandedCallsRanAndZIsStillHeld();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_resume_at_a_given_position_from_another_thread_does_not_wait_for_the_function_and_the_wrapped_model_that_cannot_reposition_refuses_it_and_nothing_is_changed() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z at a given position from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z, StartAt.now()));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the resume is refused at once by the wrapped model, which cannot resume at a position, and changes nothing
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the resume ended within " + BOUND + " while the function waited for it").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what resumeSubscription(" + Z + ", startAt) threw").isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("not repositionable");
        assertTheHandedCallsRanAndZIsStillHeld();
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
        assertTheSubscribeOfZRanAndZIsHeld();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_start_from_another_thread_completes_without_waiting_for_the_function_of_the_handed_subscribe_and_starts_the_wrapped_model_once() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a start from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.start(false));

        // When the action returns and the handed subscribe of z asks the function on its own thread
        awaitTheFunction();

        // Then the start completed at once, as the function does not hold the model lock, and started the wrapped model once
        assertTheCallCompletedWithoutWaitingForTheFunction("start(..)");
        assertThat(live.starts.get()).as("the starts the wrapped model got from the start").isEqualTo(1);
        assertTheHandedCallsRanAndZIsStillHeld();
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
            HookedSubscriptionModel.awaitLatch(wrappedShutdownMayReturn);
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
    void a_stop_from_another_thread_throws_when_a_catchup_listener_that_runs_under_the_lock_without_a_handed_call_waits_for_it_and_a_later_stop_completes() throws Exception {
        // Given z held paused with a parked replay, and a listener for z that waits for a stop from another thread
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.listenForCatchup(Z, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (functionAsked.compareAndSet(false, true)) {
                    waitForTheCall(model::stop);
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribePaused(Z, null, StartAtTime.beginningOfTime(), event -> {
        });

        // When a plain thread resumes z, which relaunches the parked replay and tells the listener under the model lock, with no call handed over
        Call resume = new Call("resume-of-z", () -> handleOfZ.set(model.resumeSubscription(Z)));
        awaitTheFunction();
        assertThat(resume.endedWithin(BOUND)).as("whether the resume of z returned within " + BOUND).isTrue();

        // Then the stop throws after the wait, and the resume went on
        assertTheCallThrewBecauseOfCallersCode("stop()");
        assertThat(resume.thrown()).as("what the resume of z threw").isNull();
        assertThat(model.isRunning()).as("whether the model still runs after the stop that gave up").isTrue();
        assertThat(live.stops.get()).as("the stops the wrapped model got from the stop that gave up").isZero();
        assertThat(handleOfZ.get()).as("the handle the resume of z returned").isNotNull();
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("later-stop"))).as("a later stop").succeedsWithin(BOUND);
        assertThat(model.isRunning()).as("whether the model runs after the later stop").isFalse();
    }

    @Test
    void a_stop_handed_over_from_a_live_action_does_not_keep_a_held_up_shutdown_from_shutting_the_wrapped_model_down_when_the_listener_returns() throws Exception {
        // Given a listener that holds the model lock and waits for a shutdown, and a live action that hands a stop over and then waits for the wrapped model to be shut down
        givenAListenerHoldsTheLockWhileALiveActionHandsACallOverAndWaitsForTheWrappedShutdown(model::stop);

        // When the shutdown has thrown after the wait, and then the listener returns
        // Then the wrapped model is shut down once, with no further call, and the action returns
        assertTheWrappedModelIsShutDownOnceTheListenerReturnsAlthoughTheActionWaitsForIt();
    }

    // Picks the resume of a subscription, not a pause or a cancel, since x can be held paused and resumed from the action
    @Test
    void a_resume_handed_over_from_a_live_action_does_not_keep_a_held_up_shutdown_from_shutting_the_wrapped_model_down_when_the_listener_returns() throws Exception {
        // Given x held paused, a listener that holds the model lock and waits for a shutdown, and a live action that hands a resume of x over and then waits for the wrapped model to be shut down
        givenXPaused();
        givenAListenerHoldsTheLockWhileALiveActionHandsACallOverAndWaitsForTheWrappedShutdown(() -> handleOfX.set(model.resumeSubscription(X)));

        // When the shutdown has thrown after the wait, and then the listener returns
        // Then the wrapped model is shut down once, with no further call, the action returns, and the resume it handed over left x held
        assertTheWrappedModelIsShutDownOnceTheListenerReturnsAlthoughTheActionWaitsForIt();
        assertThat(model.isRunning(X)).as("whether x runs, which it does not as the resume the action handed over ran after a shutdown had begun").isFalse();
    }

    @Test
    void a_shutdown_handed_over_from_a_live_action_does_not_keep_a_held_up_shutdown_from_shutting_the_wrapped_model_down_when_the_listener_returns() throws Exception {
        // Given a listener that holds the model lock and waits for a shutdown, and a live action that hands a shutdown over and then waits for the wrapped model to be shut down
        givenAListenerHoldsTheLockWhileALiveActionHandsACallOverAndWaitsForTheWrappedShutdown(model::shutdown);

        // When the shutdown has thrown after the wait, and then the listener returns
        // Then the wrapped model is shut down once, with no further call, and the action returns
        assertTheWrappedModelIsShutDownOnceTheListenerReturnsAlthoughTheActionWaitsForIt();
    }

    @Test
    void a_stop_made_from_another_thread_while_the_handed_shutdown_is_in_the_wrapped_model_does_not_stop_the_wrapped_model() throws Exception {
        // Given a wrapped model whose shutdown waits to be let go, and a shutdown that threw because a listener under the model lock waited for it
        live.beforeShutdown = () -> {
            wrappedShutdownEntered.countDown();
            HookedSubscriptionModel.awaitLatch(wrappedShutdownMayReturn);
        };
        functionMayReturn = new CountDownLatch(1);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        aListenerOfZHoldsTheLockAndWaitsForAShutdown(() -> {
        });
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());

        // When the listener returns and, with the handed shutdown inside the wrapped model under the model lock, a stop is made from another thread
        functionMayReturn.countDown();
        assertThat(wrappedShutdownEntered.await(20, TimeUnit.SECONDS)).as("whether the handed shutdown reached the wrapped model once the listener returned").isTrue();
        Call stop = new Call("stop-during-the-handed-shutdown", model::stop);
        await().atMost(BOUND).untilAsserted(() -> assertThat(stop.state()).as("the state of the thread of the stop, which waits for the handed shutdown").isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING));
        wrappedShutdownMayReturn.countDown();

        // Then the stop returns without error once the handed shutdown has returned, and the wrapped model got no stop
        assertThat(stop.endedWithin(BOUND)).as("whether the stop returned once the handed shutdown had shut the wrapped model down").isTrue();
        assertThat(stop.thrown()).as("what the stop threw").isNull();
        assertThat(live.stops.get()).as("the stops the wrapped model got from the stop made while the handed shutdown was in it").isZero();
    }

    @Test
    void a_start_that_found_no_handed_call_pending_before_a_held_up_shutdown_handed_its_shutdown_over_starts_nothing_once_that_shutdown_has_run() throws Exception {
        // Given a start made from another thread that read no handed call pending and is held before the model lock, and a shutdown that then threw because a listener under the lock waited for it
        givenACallIsHeldBeforeTheModelLock(() -> model.start(false));

        // When the listener returns, the handed shutdown has shut the wrapped model down, and the start takes the lock
        theShutdownThrewAndTheHandedOneHasReturnedAndThenTheHeldCallTakesTheLock();

        // Then the start returns without error, the wrapped model got no start from it, and was shut down once
        assertThat(heldCall.get().thrown()).as("what the start threw").isNull();
        assertThat(live.starts.get()).as("the starts the wrapped model got from the start made before the handed shutdown returned").isZero();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_stop_that_found_no_handed_call_pending_before_a_held_up_shutdown_handed_its_shutdown_over_stops_nothing_once_that_shutdown_has_run() throws Exception {
        // Given a stop made from another thread that read no handed call pending and is held before the model lock, and a shutdown that then threw because a listener under the lock waited for it
        givenACallIsHeldBeforeTheModelLock(model::stop);

        // When the listener returns, the handed shutdown has shut the wrapped model down, and the stop takes the lock
        theShutdownThrewAndTheHandedOneHasReturnedAndThenTheHeldCallTakesTheLock();

        // Then the stop returns without error, the wrapped model got no stop from it, and was shut down once
        assertThat(heldCall.get().thrown()).as("what the stop threw").isNull();
        assertThat(live.stops.get()).as("the stops the wrapped model got from the stop made before the handed shutdown returned").isZero();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_resume_of_a_paused_subscription_that_found_no_handed_call_pending_before_a_held_up_shutdown_handed_its_shutdown_over_leaves_it_held_once_that_shutdown_has_run() throws Exception {
        // Given x held paused, a resume of x made from another thread that read no handed call pending and is held before the model lock, and a shutdown that then threw because a listener under the lock waited for it
        givenXPaused();
        givenACallIsHeldBeforeTheModelLock(() -> handleOfX.set(model.resumeSubscription(X)));

        // When the listener returns, the handed shutdown has shut the wrapped model down, and the resume takes the lock
        theShutdownThrewAndTheHandedOneHasReturnedAndThenTheHeldCallTakesTheLock();

        // Then the resume returns without error, its handle never reports x started, and x does not run
        assertThat(heldCall.get().thrown()).as("what the resume of x threw").isNull();
        assertThat(handleOfX.get()).as("the handle the resume of x returned").isNotNull();
        assertThat(handleOfX.get().waitUntilStarted(Duration.ofSeconds(1))).as("whether the handle of the resume made before the handed shutdown returned completes started").isFalse();
        assertThat(model.isRunning(X)).as("whether x runs after the resume made before the handed shutdown returned").isFalse();
        assertTheWrappedModelIsShutDownOnce();
    }

    @Test
    void a_stop_made_by_the_function_of_a_handed_subscribe_after_the_shutdown_it_asked_has_completed_stops_nothing() throws Exception {
        // Given a function, running on the thread of a handed subscribe, that waits for a shutdown on another thread, which completed and shut the wrapped model down, and then makes a stop itself
        givenTheFunctionMakesACallAfterTheShutdownItAskedCompleted(model::stop);

        // When the function returns
        // Then the stop returned without error, the wrapped model got no stop from it, and it is shut down once
        assertThat(thrownUnderTheLock.get()).as("what the stop made by the function threw").isNull();
        assertThat(stopsSeenUnderTheLock.get()).as("the stops the wrapped model had got once the stop made by the function returned").isZero();
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
        assertThat(live.stops.get()).as("the stops the wrapped model got in all").isZero();
    }

    @Test
    void a_start_made_after_the_handed_shutdown_has_returned_with_no_handed_call_left_starts_the_wrapped_model_again() throws Exception {
        // Given a shutdown that threw because a listener under the model lock waited for it, whose handed shutdown has shut the wrapped model down and returned
        functionMayReturn = new CountDownLatch(1);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        aListenerOfZHoldsTheLockAndWaitsForAShutdown(() -> {
        });
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
        awaitTheHandedShutdownThreadHasEnded();
        assertThat(live.starts.get()).as("the starts the wrapped model got before the start made after the handed shutdown returned").isZero();

        // When a start is made from another thread
        assertThat(CompletableFuture.runAsync(() -> model.start(false), daemonExecutor("later-start"))).as("a start made after the handed shutdown returned").succeedsWithin(BOUND);

        // Then the wrapped model is started once
        assertThat(live.starts.get()).as("the starts the wrapped model got from the start made after the handed shutdown returned").isEqualTo(1);
    }

    // A listener of z holds the model lock and waits for a shutdown, which throws once it has handed its wrapped model's
    // shutdown over. Before it asks that shutdown, the listener has call made from another thread, which reads that no handed
    // call is pending, and the hook given to runBetweenTheHandedCallsAndTheLock holds it before it waits for the model lock.
    // So the call is held ahead of the handover
    private void givenACallIsHeldBeforeTheModelLock(Runnable call) throws InterruptedException {
        model.runBetweenTheHandedCallsAndTheLock(() -> {
            if (holdTheNextCallBeforeTheLock.compareAndSet(true, false)) {
                callIsHeldBeforeTheLock.countDown();
                HookedSubscriptionModel.awaitLatch(callMayTakeTheLock);
            }
        });
        functionMayReturn = new CountDownLatch(1);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        aListenerOfZHoldsTheLockAndWaitsForAShutdown(() -> {
            holdTheNextCallBeforeTheLock.set(true);
            heldCall.set(new Call("call-held-before-the-lock", call));
            HookedSubscriptionModel.awaitLatch(callIsHeldBeforeTheLock);
        });
        assertThat(callIsHeldBeforeTheLock.await(20, TimeUnit.SECONDS)).as("whether the call under test is held before the model lock").isTrue();
    }

    private void theShutdownThrewAndTheHandedOneHasReturnedAndThenTheHeldCallTakesTheLock() throws Exception {
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got while the listener held the lock").isZero();
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
        awaitTheHandedShutdownThreadHasEnded();
        callMayTakeTheLock.countDown();
        assertThat(heldCall.get().endedWithin(BOUND)).as("whether the held call returned once it could take the model lock").isTrue();
    }

    // The handed shutdown runs on a thread of its own, which has returned once that thread has ended
    private static void awaitTheHandedShutdownThreadHasEnded() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(Thread.getAllStackTraces().keySet())
                .as("the handed shutdown threads still alive")
                .noneMatch(thread -> thread.getName().equals("occurrent-catchup-handed-shutdown") && thread.isAlive()));
    }

    // The function of z asks a shutdown from another thread on the thread of the handed subscribe of z, without the model lock.
    // Once that shutdown has completed and shut the wrapped model down, the function itself makes call
    private void givenTheFunctionMakesACallAfterTheShutdownItAskedCompleted(Runnable call) throws Exception {
        functionMayReturn = new CountDownLatch(1);
        onTheThreadHoldingTheLock = () -> {
            try {
                call.run();
            } catch (Throwable e) {
                thrownUnderTheLock.set(e);
            }
            stopsSeenUnderTheLock.set(live.stops.get());
        };
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);
        awaitTheFunction();
        assertTheCallCompletedWithoutWaitingForTheFunction("shutdown()");
    }

    // A listener of z holds the model lock on the thread that resumes z, and asks a shutdown from another thread, which
    // throws after LIFECYCLE_WAIT, once beforeTheShutdown has run. Returns once the listener holds the lock. z has history
    // by then
    private void aListenerOfZHoldsTheLockAndWaitsForAShutdown(Runnable beforeTheShutdown) throws InterruptedException {
        model.listenForCatchup(Z, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (functionAsked.compareAndSet(false, true)) {
                    listenerHoldsTheLock.countDown();
                    beforeTheShutdown.run();
                    waitForTheCall(model::shutdown);
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribePaused(Z, null, StartAtTime.beginningOfTime(), event -> {
        });
        new Call("resume-of-z", () -> handleOfZ.set(model.resumeSubscription(Z)));
        assertThat(listenerHoldsTheLock.await(20, TimeUnit.SECONDS)).as("whether the listener of z holds the model lock").isTrue();
    }

    // The action of y, a live action, runs once the listener holds the lock. It hands handOver over, which a thread that
    // runs no action of the model can't have queued ahead of the shutdown the listener waits for, and then waits for the
    // wrapped model's shutdown. Only that shutdown can end the wait, as what the action handed over waits for the action
    private void givenAListenerHoldsTheLockWhileALiveActionHandsACallOverAndWaitsForTheWrappedShutdown(Runnable handOver) throws Exception {
        live.beforeShutdown = wrappedShutdownEntered::countDown;
        functionMayReturn = new CountDownLatch(1);
        // The history of z, written before y subscribes, so no live call of y runs before the listener holds the lock
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.subscribe(Y, null, StartAt.now(), event -> {
            try {
                handOver.run();
                actionHandedACallOver.countDown();
                actionSawTheWrappedShutdown.set(wrappedShutdownEntered.await(30, TimeUnit.SECONDS));
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            } finally {
                actionReturned.countDown();
            }
        });
        aListenerOfZHoldsTheLockAndWaitsForAShutdown(() -> HookedSubscriptionModel.awaitLatch(actionHandedACallOver));
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
    }

    // The action still waits when the shutdown that threw hands the wrapped model's shutdown over, which waits only for the lock
    private void assertTheWrappedModelIsShutDownOnceTheListenerReturnsAlthoughTheActionWaitsForIt() throws Exception {
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got while the listener held the lock").isZero();
        assertThat(actionReturned.getCount()).as("whether the action of y still waits for the wrapped model's shutdown").isEqualTo(1);

        functionMayReturn.countDown();
        await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got once the listener returned, while the action still waited for it").isEqualTo(1));
        assertTheWrappedModelIsShutDownOnce();
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned once the wrapped model was shut down").isTrue();
        assertThat(actionSawTheWrappedShutdown.get()).as("whether the action of y saw the wrapped model shut down before it gave up waiting").isTrue();
        assertThat(thrownByTheAction.get()).as("what the action of y threw").isNull();
    }

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // a thread of its own, without the model lock, and asks the function there. The function runs call on another thread
    // and waits for it, which is the wait that deadlocks without the fix
    private void actionResumesXThenSubscribesZWhoseFunctionWaitsFor(Runnable call) throws Exception {
        givenXPaused();
        StartAt waits = StartAt.dynamic(() -> {
            if (functionAsked.compareAndSet(false, true)) {
                waitForTheCall(call);
            }
            return StartAtTime.beginningOfTime();
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
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        assertThat(thrownByTheAction.get()).as("what the action of y threw").isNull();
    }

    // Runs on the handed-call thread that asks the caller's code
    private void waitForTheCall(Runnable call) {
        Call started = new Call("call-the-function-waits-for", call);
        callOfTheFunction.set(started);
        callEndedWhileTheFunctionWaited.set(started.endedWithin(BOUND));
        onTheThreadHoldingTheLock.run();
        functionReturned.countDown();
        HookedSubscriptionModel.awaitLatch(functionMayReturn);
    }

    private void awaitTheFunction() throws InterruptedException {
        assertThat(functionReturned.await(BOUND.plusSeconds(15).toSeconds(), TimeUnit.SECONDS)).as("whether the caller's code that waited for the call returned").isTrue();
    }

    // A listener the relaunch of a replay tells, and a function asked in a start's or resume's turn, still run under the model lock, as the callers of this helper do
    private void assertTheCallThrewBecauseOfCallersCode(String call) {
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the call ended within " + BOUND + " while the caller's code waited for it").isTrue();
        assertThat(callOfTheFunction.get().thrown()).as("what " + call + " threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot run " + call)
                .hasMessageContaining("because code the caller supplied")
                .hasMessageEndingWith("Nothing was changed.");
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

    private void assertTheHandedCallsRanAndZIsStillHeld() throws Exception {
        assertTheSubscribeOfZRanAndZIsHeld();
        assertThat(handleOfX.get().waitUntilStarted(WAIT)).as("whether the handle of the resume of x that the action handed over completes started").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume the action handed over").isTrue();
        assertThat(model.isRunning(Z)).as("whether z runs, which it does not as the call that gave up changed nothing and the subscribe left z held").isFalse();
    }

    // The subscribe of z, handed over by the action, completes once the function returned, and z is held
    private void assertTheSubscribeOfZRanAndZIsHeld() {
        await().atMost(WAIT).untilAsserted(() -> assertThat(model.isPaused(Z)).as("whether z is paused once the handed subscribe returned").isTrue());
        assertThat(handleOfZ.get()).as("the handle the subscribe of z that waited for the call returned to the action").isNotNull();
    }

    private void assertLaterLifecycleCallsComplete() {
        assertThat(CompletableFuture.runAsync(() -> model.resumeSubscription(Z), daemonExecutor("later-resume-of-z"))).as("a later resume of z").succeedsWithin(BOUND);
        assertThat(model.isRunning(Z)).as("whether z runs after the later resume").isTrue();
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("later-stop"))).as("a later stop").succeedsWithin(BOUND);
        assertThat(model.isRunning()).as("whether the model runs after the later stop").isFalse();
    }

    private void givenXPaused() {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
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

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
