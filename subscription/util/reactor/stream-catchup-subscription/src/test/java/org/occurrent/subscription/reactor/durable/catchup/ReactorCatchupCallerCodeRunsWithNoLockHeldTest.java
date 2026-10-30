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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The catch-up model runs the caller's code from a cancel, a stop, a shutdown and a launch of a replay. That code is the
 * cancel hook of an action, a waiter for the start of a subscription, or an action a replay runs on the thread that
 * launched it. It may call the catch-up model from another thread and wait for that call, so none of these holds a lock
 * of the catch-up model while the code runs. A thread that never returns fails its test at the deadline rather than
 * hanging the build.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupCallerCodeRunsWithNoLockHeldTest {

    private static final String ID = "sub";
    private static final String LIVE_EVENT_ID = "live";
    private static final Duration DEADLINE = Duration.ofSeconds(5);
    private static final Duration WAIT_IN_CALLER_CODE = Duration.ofSeconds(2);

    @Test
    void two_cancels_return_when_each_cancels_the_other_subscription_from_the_cancel_hook_of_the_live_action_the_wrapped_model_cancels() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.deliversALiveEvent = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        CountDownLatch bothInCallerCode = new CountDownLatch(2);
        List<Throwable> callerCodeFailures = new CopyOnWriteArrayList<>();
        List<String> received = new CopyOnWriteArrayList<>();
        for (String[] ids : new String[][]{{"x", "y"}, {"y", "x"}}) {
            String other = ids[1];
            AtomicBoolean ran = new AtomicBoolean();
            Runnable cancelOther = () -> {
                if (!ran.compareAndSet(false, true)) {
                    return;
                }
                try {
                    bothInCallerCode.countDown();
                    if (!bothInCallerCode.await(DEADLINE.toMillis(), TimeUnit.MILLISECONDS)) {
                        throw new AssertionError("The cancel of " + other + " never ran its caller code");
                    }
                    catchup.cancelSubscription(other).block(DEADLINE);
                } catch (Throwable t) {
                    callerCodeFailures.add(t);
                }
            };
            catchup.subscribe(ids[0], StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
                received.add(ids[0] + ":" + cloudEvent.getId());
                return LIVE_EVENT_ID.equals(cloudEvent.getId()) ? Mono.<Void>never().doOnCancel(cancelOther) : Mono.empty();
            });
        }
        assertThat(received).as("events the actions were given").contains("x:" + LIVE_EVENT_ID, "y:" + LIVE_EVENT_ID);

        CompletableFuture<Void> cancelOfX = new CompletableFuture<>();
        CompletableFuture<Void> cancelOfY = new CompletableFuture<>();
        Thread cancellingX = daemon("cancel-x", cancelOfX, () -> catchup.cancelSubscription("x").block(DEADLINE));
        Thread cancellingY = daemon("cancel-y", cancelOfY, () -> catchup.cancelSubscription("y").block(DEADLINE));
        cancellingX.start();
        cancellingY.start();

        await(cancelOfX, "The cancel of x", cancellingX);
        await(cancelOfY, "The cancel of y", cancellingY);
        assertThat(bothInCallerCode.getCount()).as("cancels that ran caller code").isZero();
        assertThat(callerCodeFailures).as("failures in the caller code").isEmpty();
        assertThat(wrapped.cancelCalls).as("cancels of the wrapped model").contains("x", "y");
    }

    @Test
    void a_stop_returns_when_the_cancel_hook_its_dispose_runs_waits_for_a_cancel_on_another_thread() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        AtomicReference<@Nullable Boolean> cancelReturnedToTheHook = new AtomicReference<>();
        Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.<Void>never().doOnCancel(() -> cancelReturnedToTheHook.set(cancelsOnAnotherThread(catchup))));

        CompletableFuture<Void> stopReturned = new CompletableFuture<>();
        Thread stopping = daemon("stop", stopReturned, catchup::stop);
        stopping.start();

        await(stopReturned, "The stop", stopping);
        assertThat(cancelReturnedToTheHook).as("whether the cancel the hook waited for returned").hasValue(true);
        assertThat(wrapped.cancelCalls).as("cancels of the wrapped model").containsExactly(ID);
        assertCancelledBeforeItStarted(subscription);
    }

    @Test
    void a_shutdown_returns_when_a_waiter_it_fails_waits_for_a_replay_on_another_thread() {
        WrappedModel wrapped = new WrappedModel();
        BlockingReader reader = new BlockingReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        catchup.stop();
        Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        CompletableFuture<Void> replayReturned = new CompletableFuture<>();
        AtomicReference<@Nullable Boolean> replayReturnedToTheWaiter = new AtomicReference<>();
        AtomicReference<@Nullable Throwable> startFailure = new AtomicReference<>();
        subscription.waitUntilStarted().subscribe(unused -> {
        }, failure -> {
            startFailure.set(failure);
            reader.release();
            replayReturnedToTheWaiter.set(returnsWithin(replayReturned));
        });
        // start(true) launches the replay on this thread, which waits in the read until the waiter releases it
        Thread replaying = daemon("replay", replayReturned, () -> catchup.start(true));
        replaying.start();
        reader.awaitReading();

        CompletableFuture<Void> shutdownReturned = new CompletableFuture<>();
        Thread shuttingDown = daemon("shutdown", shutdownReturned, catchup::shutdown);
        shuttingDown.start();

        await(shutdownReturned, "The shutdown", shuttingDown);
        await(replayReturned, "The replay", replaying);
        assertThat(startFailure.get()).as("what the waiter was told").isInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(replayReturnedToTheWaiter).as("whether the replay the waiter waited for returned").hasValue(true);
        assertThat(wrapped.subscribeCalls).as("handovers").isEmpty();
        assertThat(wrapped.shutdownCalls).as("shutdowns of the wrapped model").hasValue(1);
    }

    @Test
    void a_waiter_that_throws_when_a_shutdown_fails_it_leaves_the_shutdown_to_fail_every_other_waiter_and_shut_the_wrapped_model_down() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        catchup.stop();
        Map<String, Throwable> startFailures = new ConcurrentHashMap<>();
        for (String id : List.of("x", "y", "z")) {
            catchup.subscribe(id, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty())
                    .waitUntilStarted()
                    .subscribe(unused -> {
                    }, failure -> {
                        startFailures.put(id, failure);
                        throw new IllegalArgumentException("thrown by the waiter for " + id);
                    });
        }

        Throwable shutdownFailure = catchThrowable(catchup::shutdown);

        assertThat(shutdownFailure).as("what the shutdown threw").isNull();
        assertThat(startFailures).as("what each waiter was told").containsOnlyKeys("x", "y", "z")
                .allSatisfy((id, failure) -> assertThat(failure).isInstanceOf(SubscriptionModelShutdownException.class));
        assertThat(wrapped.shutdownCalls).as("shutdowns of the wrapped model").hasValue(1);
    }

    @Test
    void a_subscribe_returns_when_an_action_its_replay_runs_on_the_subscribing_thread_waits_for_a_cancel_on_another_thread() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        AtomicReference<@Nullable Boolean> cancelReturnedToTheAction = new AtomicReference<>();
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<@Nullable Subscription> subscription = new AtomicReference<>();

        CompletableFuture<Void> subscribeReturned = new CompletableFuture<>();
        Thread subscribing = daemon("subscribe", subscribeReturned, () -> subscription.set(
                catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
                    received.add(cloudEvent.getId());
                    if (cancelReturnedToTheAction.get() == null) {
                        cancelReturnedToTheAction.set(cancelsOnAnotherThread(catchup));
                    }
                    return Mono.empty();
                })));
        subscribing.start();

        await(subscribeReturned, "The subscribe", subscribing);
        assertThat(cancelReturnedToTheAction).as("whether the cancel the action waited for returned").hasValue(true);
        assertThat(received).as("events the action was given").containsExactly("e1");
        assertThat(wrapped.subscribeCalls).as("handovers").isEmpty();
        assertThat(wrapped.cancelCalls).as("cancels of the wrapped model").containsExactly(ID);
        assertCancelledBeforeItStarted(subscription.get());
    }

    @Test
    void a_subscribe_of_an_id_whose_wrapped_cancel_has_not_completed_is_refused_and_goes_through_once_it_has() {
        WrappedModel wrapped = new WrappedModel();
        Sinks.Empty<Void> wrappedCancel = Sinks.empty();
        wrapped.cancelling = wrappedCancel.asMono();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.cancelSubscription(ID);

        assertThatThrownBy(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()))
                .isInstanceOf(DuplicateSubscriptionIdException.class);
        wrappedCancel.tryEmitEmpty();
        wrapped.cancelling = Mono.empty();
        catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());

        assertThat(wrapped.subscribeCalls).as("handovers").containsExactly(ID, ID);
        assertThat(catchup.isRunning(ID)).as("the subscription made once the cancel completed runs").isTrue();
    }

    // Cancels ID on a thread of its own and waits for that cancel, as long as caller code would. Whether it returned.
    private static boolean cancelsOnAnotherThread(ReactorStreamCatchupSubscriptionModel catchup) {
        CompletableFuture<Void> cancelReturned = new CompletableFuture<>();
        daemon("cancel-from-caller-code", cancelReturned, () -> catchup.cancelSubscription(ID).block(DEADLINE)).start();
        return returnsWithin(cancelReturned);
    }

    private static boolean returnsWithin(CompletableFuture<Void> returned) {
        try {
            returned.get(WAIT_IN_CALLER_CODE.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static void assertCancelledBeforeItStarted(@Nullable Subscription subscription) {
        assertThat(subscription).isNotNull();
        assertThatThrownBy(() -> subscription.waitUntilStarted().block(DEADLINE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Subscription " + ID + " was cancelled before it started.");
    }

    private static Thread daemon(String name, CompletableFuture<Void> returned, Runnable work) {
        Thread thread = new Thread(() -> {
            try {
                work.run();
                returned.complete(null);
            } catch (Throwable t) {
                returned.completeExceptionally(t);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void await(CompletableFuture<Void> returned, String what, Thread thread) {
        try {
            returned.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(what + " did not return within " + DEADLINE.toSeconds() + " seconds, thread " + thread.getName() + " is " + thread.getState()
                    + " at " + Arrays.stream(thread.getStackTrace()).limit(6).toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("type")
                .build();
    }

    private static Flux<CloudEvent> eventsIn(PositionRange range) {
        long from = range.afterPosition().orElse(0L) + 1;
        long to = Math.min(range.upToPosition().orElse(3L), 3L);
        return Flux.fromStream(LongStream.rangeClosed(from, to).boxed().map(position -> event("e" + position)));
    }

    // Three events at positions 1 to 3, read on the thread that subscribes to the read
    private static final class ImmediateReader implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return eventsIn(range);
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(3L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // As ImmediateReader, but a read blocks the thread that subscribes to it until release()
    private static final class BlockingReader implements UnscopedPositionOrderedReader {
        private final CountDownLatch reading = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void release() {
            released.countDown();
        }

        void awaitReading() {
            awaitLatch(reading, "The replay never read");
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.defer(() -> {
                reading.countDown();
                awaitLatch(released, "The read was never released");
                return eventsIn(range);
            });
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(3L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }

        private static void awaitLatch(CountDownLatch latch, String failure) {
            try {
                if (!latch.await(DEADLINE.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new AssertionError(failure);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    // A named subscription model that, like ReactorMongoSubscriptionModel, cancels a subscription by cancelling the Mono
    // its action returned. When deliversALiveEvent is set, its subscribe gives the action one event on a thread of its
    // own, and returns once that thread has the Mono.
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();
        final AtomicInteger shutdownCalls = new AtomicInteger();
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        volatile boolean deliversALiveEvent;
        volatile Mono<Void> cancelling = Mono.empty();
        private final Map<String, Disposable> running = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(TOKEN);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(TOKEN);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribeCalls.add(subscriptionId);
            subscribed.add(subscriptionId);
            if (deliversALiveEvent) {
                CountDownLatch delivered = new CountDownLatch(1);
                Thread delivery = new Thread(() -> {
                    running.put(subscriptionId, action.apply(OccurrentCloudEventExtension.withPosition(event(LIVE_EVENT_ID), 4)).subscribe());
                    delivered.countDown();
                }, "delivery");
                delivery.setDaemon(true);
                delivery.start();
                BlockingReader.awaitLatch(delivered, "The delivery thread never gave the action the live event");
            }
            return handle(subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            cancelCalls.add(subscriptionId);
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
            Disposable inFlight = running.remove(subscriptionId);
            if (inFlight != null) {
                inFlight.dispose();
            }
            return cancelling;
        }

        @Override
        public void stop() {
            paused.addAll(subscribed);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (resumeSubscriptionsAutomatically) {
                paused.clear();
            }
        }

        @Override
        public void shutdown() {
            shutdownCalls.incrementAndGet();
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            paused.remove(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        private static Subscription handle(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }
    }
}
