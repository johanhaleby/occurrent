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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
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
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code start(..)}, {@code stop()} or {@code resumeSubscription(id)} of a {@link CatchupSubscriptionModel} that was made
 * before the wrapped model's shutdown, which a {@code shutdown()} held up by code a caller supplied handed to a thread of
 * its own, had returned changes nothing, also when the call read that no handed call was pending before the shutdown handed
 * its own over, and took the model lock after the wrapped model was shut down. A {@code start(..)} made once the handed
 * shutdown has returned starts the wrapped model again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupSubscriptionModelHandedShutdownOvertakeTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    private static final Duration BOUND = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch functionReturned = new CountDownLatch(1);
    // The caller's code waits for this once the call it waited for has ended, so a test can look at the wrapped model while that code still holds the lock
    private volatile CountDownLatch functionMayReturn = new CountDownLatch(0);
    private final CountDownLatch listenerHoldsTheLock = new CountDownLatch(1);
    private final AtomicBoolean holdTheNextCallBeforeTheLock = new AtomicBoolean();
    private final CountDownLatch callIsHeldBeforeTheLock = new CountDownLatch(1);
    private final CountDownLatch callMayTakeTheLock = new CountDownLatch(1);
    private final AtomicReference<@Nullable Call> heldCall = new AtomicReference<>();
    // Runs on the thread that asks the function, under the model lock, once the call the function waited for has ended
    private volatile Runnable onTheThreadHoldingTheLock = () -> {
    };
    private final AtomicReference<@Nullable Throwable> thrownUnderTheLock = new AtomicReference<>();
    private final AtomicInteger stopsSeenUnderTheLock = new AtomicInteger(-1);
    private final AtomicBoolean functionAsked = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> handleOfX = new AtomicReference<>();
    private final AtomicReference<@Nullable Call> callOfTheFunction = new AtomicReference<>();
    private InMemorySubscriptionModel inMemory;
    private CountingLiveModel live;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_over_an_in_memory_live_model() {
        inMemory = new InMemorySubscriptionModel();
        live = new CountingLiveModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new CatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shut_down() throws Exception {
        functionMayReturn.countDown();
        callMayTakeTheLock.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
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

    @Test
    void a_stop_made_by_the_function_under_the_model_lock_after_a_shutdown_handed_its_shutdown_over_stops_nothing() throws Exception {
        // Given a function that waits for a shutdown on another thread, which threw and handed its wrapped model's shutdown over, and then makes a stop itself while it still holds the model lock
        givenTheFunctionMakesACallUnderTheLockAfterTheShutdownHandedItsShutdownOver(model::stop);

        // When the function returns
        // Then the stop returned without error, the wrapped model got no stop from it, and it is shut down once
        assertThat(thrownUnderTheLock.get()).as("what the stop made under the model lock threw").isNull();
        assertThat(stopsSeenUnderTheLock.get()).as("the stops the wrapped model had got once the stop made under the model lock returned").isZero();
        functionMayReturn.countDown();
        assertTheWrappedModelIsShutDownOnce();
        assertThat(live.stops.get()).as("the stops the wrapped model got in all").isZero();
    }

    // A listener of z holds the model lock and waits for a shutdown, which throws once it has handed its wrapped model's
    // shutdown over. Before it asks that shutdown, the listener has call made from another thread, which reads that no handed
    // call is pending, and the hook given to runBetweenTheHandedCallsAndTheLock holds it before it waits for the model lock.
    // So the call is held ahead of the handover
    private void givenACallIsHeldBeforeTheModelLock(Runnable call) throws InterruptedException {
        model.runBetweenTheHandedCallsAndTheLock(() -> {
            if (holdTheNextCallBeforeTheLock.compareAndSet(true, false)) {
                callIsHeldBeforeTheLock.countDown();
                awaitLatch(callMayTakeTheLock);
            }
        });
        functionMayReturn = new CountDownLatch(1);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        aListenerOfZHoldsTheLockAndWaitsForAShutdown(() -> {
            holdTheNextCallBeforeTheLock.set(true);
            heldCall.set(new Call("call-held-before-the-lock", call));
            awaitLatch(callIsHeldBeforeTheLock);
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

    // The function of z asks a shutdown from another thread under the model lock, on the lifecycle thread, which holds no claim
    // for z while it asks. Once that shutdown has thrown and handed its wrapped model's shutdown over, which waits for the lock
    // the function holds, the function itself makes call, so call finds the lock held by its own thread
    private void givenTheFunctionMakesACallUnderTheLockAfterTheShutdownHandedItsShutdownOver(Runnable call) throws Exception {
        functionMayReturn = new CountDownLatch(1);
        onTheThreadHoldingTheLock = () -> {
            try {
                call.run();
            } catch (Throwable e) {
                thrownUnderTheLock.set(e);
            }
            stopsSeenUnderTheLock.set(live.stops.get());
        };
        actionResumesXThenSubscribesZWhoseFunctionWaitsForAShutdown();
        awaitTheFunction();
        assertTheShutdownThrewAfterTheWait(callOfTheFunction.get());
    }

    // The action of y, a live action, hands the resume of x over and then subscribes z paused behind it, so z subscribes on
    // the lifecycle thread under the model lock and asks the function there. The function runs a shutdown on another thread
    // and waits for it
    private void actionResumesXThenSubscribesZWhoseFunctionWaitsForAShutdown() throws Exception {
        givenXPaused();
        StartAt waits = StartAt.dynamic(() -> {
            if (functionAsked.compareAndSet(false, true)) {
                waitForTheShutdown();
            }
            return StartAtTime.beginningOfTime();
        });
        model.subscribe(Y, null, StartAt.now(), event -> {
            try {
                handleOfX.set(model.resumeSubscription(X));
                model.subscribePaused(Z, null, waits, e -> {
                });
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
                    waitForTheShutdown();
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribePaused(Z, null, StartAtTime.beginningOfTime(), event -> {
        });
        new Call("resume-of-z", () -> model.resumeSubscription(Z));
        assertThat(listenerHoldsTheLock.await(20, TimeUnit.SECONDS)).as("whether the listener of z holds the model lock").isTrue();
    }

    // Runs on the thread that holds the model lock and asks the caller's code
    private void waitForTheShutdown() {
        Call started = new Call("call-the-function-waits-for", model::shutdown);
        callOfTheFunction.set(started);
        started.endedWithin(BOUND);
        onTheThreadHoldingTheLock.run();
        functionReturned.countDown();
        awaitLatch(functionMayReturn);
    }

    private void awaitTheFunction() throws InterruptedException {
        assertThat(functionReturned.await(BOUND.plusSeconds(15).toSeconds(), TimeUnit.SECONDS)).as("whether the caller's code that waited for the shutdown returned").isTrue();
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

    // The handed shutdown runs once the caller's code returned, and nothing then shuts the wrapped model down a second time
    private void assertTheWrappedModelIsShutDownOnce() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got once the caller's code returned").isEqualTo(1));
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(live.shutdowns.get()).as("the shutdowns the wrapped model got after the first one").isEqualTo(1));
    }

    // The handed shutdown runs on a thread of its own, which has returned once that thread has ended
    private static void awaitTheHandedShutdownThreadHasEnded() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(Thread.getAllStackTraces().keySet())
                .as("the handed shutdown threads still alive")
                .noneMatch(thread -> thread.getName().equals("occurrent-catchup-handed-shutdown") && thread.isAlive()));
    }

    private void givenXPaused() {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
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

    // One call on a thread of its own, which records what it threw
    private static final class Call {
        private final long startedAt = System.nanoTime();
        private volatile long endedAt;
        private final CountDownLatch ended = new CountDownLatch(1);
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        Call(String name, Runnable body) {
            Thread thread = new Thread(() -> {
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

    // An in-memory live model that counts the starts, stops and shutdowns it is asked for
    private static final class CountingLiveModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        final AtomicInteger starts = new AtomicInteger();
        final AtomicInteger stops = new AtomicInteger();
        final AtomicInteger shutdowns = new AtomicInteger();

        CountingLiveModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            starts.incrementAndGet();
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            shutdowns.incrementAndGet();
            delegate.shutdown();
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
