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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Code a caller supplied, a {@code StartAt.dynamic} function or a {@link CatchupListener}, that runs under the model
 * lock and waits for a lifecycle call made on another thread does not deadlock with that call. The call gives up after
 * {@code LIFECYCLE_WAIT} and throws {@link IllegalStateException}, changing nothing, and a later lifecycle call
 * completes. A {@code shutdown()} throws too, and leaves the wrapped model running, since it has ended the catch-up
 * replays and marked the model as shutting down before it waited. A {@code shutdown()} made once the code has returned
 * shuts the wrapped model down.
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
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_of_the_subscribe_being_asked_its_function_from_another_thread_throws_instead_of_waiting_for_the_function_and_the_subscribe_then_completes() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a resume of z from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.resumeSubscription(Z));

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the resume throws after the wait and changes nothing
        assertTheCallThrewBecauseOfCallersCode("resumeSubscription(" + Z + ")");
        assertTheHandedCallsRanAndZIsStillHeld();
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
        assertTheHandedCallsRanAndZIsStillHeld();
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
        assertTheHandedCallsRanAndZIsStillHeld();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_start_from_another_thread_throws_instead_of_waiting_for_the_function() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a start from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(() -> model.start(false));

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the start throws after the wait and starts nothing
        assertTheCallThrewBecauseOfCallersCode("start(..)");
        assertThat(live.starts.get()).as("the starts the wrapped model got from the start that gave up").isZero();
        assertTheHandedCallsRanAndZIsStillHeld();
        assertLaterLifecycleCallsComplete();
    }

    @Test
    void a_shutdown_from_another_thread_throws_instead_of_waiting_for_the_function_and_leaves_the_wrapped_model_running_and_a_later_shutdown_shuts_it_down() throws Exception {
        // Given an action that resumes x and then subscribes z paused, whose function waits for a shutdown from another thread
        actionResumesXThenSubscribesZWhoseFunctionWaitsFor(model::shutdown);

        // When the action returns and the lifecycle thread asks the function under the model lock
        awaitTheFunction();

        // Then the shutdown throws after the wait, and the wrapped model is not shut down and y still runs in it
        assertThat(callEndedWhileTheFunctionWaited.get()).as("whether the shutdown ended within " + BOUND + " while the function waited for it").isTrue();
        assertThat(callOfTheFunction.get().elapsed()).as("how long the shutdown took to give up").isLessThan(WAIT.plusSeconds(5));
        assertThat(callOfTheFunction.get().thrown()).as("what shutdown() threw")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot run shutdown()")
                .hasMessageContaining("because code the caller supplied")
                .hasMessageContaining("the wrapped subscription model isn't shut down")
                .hasMessageEndingWith("Call shutdown() again once that code has returned.");
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got from the shutdown that gave up").isZero();
        assertThat(live.isRunning(Y)).as("whether y still runs in the wrapped model after the shutdown that gave up").isTrue();

        // And once the caller's code has returned, a later shutdown shuts the wrapped model down
        model.awaitTheHandedCalls("the test");
        assertThat(CompletableFuture.runAsync(model::shutdown, daemonExecutor("later-shutdown"))).as("a later shutdown").succeedsWithin(BOUND);
        assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got once the later shutdown completed").isEqualTo(1);
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

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // the lifecycle thread under the model lock and asks the function there. The function runs call on another thread
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

    // Runs on the thread that holds the model lock and asks the caller's code
    private void waitForTheCall(Runnable call) {
        Call started = new Call("call-the-function-waits-for", call);
        callOfTheFunction.set(started);
        callEndedWhileTheFunctionWaited.set(started.endedWithin(BOUND));
        functionReturned.countDown();
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

    private void assertTheHandedCallsRanAndZIsStillHeld() throws Exception {
        // Behind the subscribe of z, which the lifecycle thread runs once the function returned
        model.awaitTheHandedCalls("the test");
        assertThat(handleOfZ.get()).as("the handle the subscribe of z that waited for the call returned to the action").isNotNull();
        assertThat(handleOfX.get().waitUntilStarted(WAIT)).as("whether the handle of the resume of x that the action handed over completes started").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume the action handed over").isTrue();
        assertThat(model.isRunning(Z)).as("whether z runs, which it does not as the call that gave up changed nothing and the subscribe left z held").isFalse();
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
