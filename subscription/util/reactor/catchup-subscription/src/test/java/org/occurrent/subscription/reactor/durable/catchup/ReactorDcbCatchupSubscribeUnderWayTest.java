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
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReads;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subscribe that goes straight to live calls the wrapped model's {@code subscribe(..)}, which can call the caller's
 * dynamic {@link StartAt} again on the calling thread. That call holds no lock of the catch-up model, so the function
 * can cancel another subscription through the catch-up, and a cancel of an id whose subscribe is under way returns
 * without waiting for it. On the calling thread a cancel does not wait, and a subscribe gets the other checks. A thread
 * that never returns fails its test at the deadline rather than hanging the build.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDcbCatchupSubscribeUnderWayTest {

    private static final Duration STARTUP = Duration.ofSeconds(60);
    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration BLOCKED = Duration.ofSeconds(5);
    private static final DcbSubscriptionFilter FILTER = DcbSubscriptionFilter.filter(DcbCriteria.all());

    @Test
    void two_subscribes_return_when_the_dynamic_start_of_each_cancels_the_other_subscription_without_waiting_for_the_cancel() {
        WrappedModel wrapped = new WrappedModel();
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        loadClassesOfASubscribeAndACancel(catchup);
        CountDownLatch bothInsideTheWrappedSubscribe = new CountDownLatch(2);
        Map<String, CompletableFuture<Void>> cancels = new ConcurrentHashMap<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        CompletableFuture<Void> subscribeOfY = new CompletableFuture<>();
        Thread subscribingX = daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER,
                cancellingFromTheWrappedModel(catchup, "y", bothInsideTheWrappedSubscribe, cancels, "x"), cloudEvent -> Mono.empty()));
        Thread subscribingY = daemon("subscribe-y", subscribeOfY, () -> catchup.subscribe("y", FILTER,
                cancellingFromTheWrappedModel(catchup, "x", bothInsideTheWrappedSubscribe, cancels, "y"), cloudEvent -> Mono.empty()));
        subscribingX.start();
        subscribingY.start();
        awaitLatch(bothInsideTheWrappedSubscribe, STARTUP, "The subscribes of x and y were not both inside the wrapped subscribe");

        await(CompletableFuture.allOf(subscribeOfX, subscribeOfY), "The subscribes of x and y did not return. x returned: " + subscribeOfX.isDone()
                + ", " + describe(subscribingX) + ". y returned: " + subscribeOfY.isDone() + ", " + describe(subscribingY));
        await(CompletableFuture.allOf(cancels.get("x"), cancels.get("y")), "A cancel did not complete");

        assertThat(bothInsideTheWrappedSubscribe.getCount()).as("subscribes that were inside the wrapped subscribe").isZero();
        assertThat(wrapped.isRunning("x") || wrapped.isPaused("x")).as("whether the wrapped model holds x").isFalse();
        assertThat(wrapped.isRunning("y") || wrapped.isPaused("y")).as("whether the wrapped model holds y").isFalse();
        assertThat(wrapped.events).as("what the wrapped model was told").containsSubsequence("subscribed:x", "cancelled:x").containsSubsequence("subscribed:y", "cancelled:y");
    }

    @Test
    void a_cancel_of_a_subscription_whose_subscribe_is_under_way_returns_at_once_and_completes_once_the_subscribe_returned_and_the_wrapped_model_cancelled_what_it_made() {
        WrappedModel wrapped = new WrappedModel();
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        StartAt held = StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorDcbCatchupSubscriptionModel.class)) {
                return null;
            }
            inside.countDown();
            awaitLatch(released, DEADLINE.multipliedBy(2), "The wrapped subscribe was never released");
            return StartAt.now();
        });
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held, cloudEvent -> Mono.empty())).start();
        awaitLatch(inside, STARTUP, "The wrapped model never evaluated the start");

        CompletableFuture<Mono<Void>> cancelling = new CompletableFuture<>();
        CountDownLatch cancelThreadRunning = new CountDownLatch(1);
        Thread cancelThread = daemon("cancel-x", new CompletableFuture<>(), () -> {
            cancelThreadRunning.countDown();
            cancelling.complete(catchup.cancelSubscription("x"));
        });
        cancelThread.start();
        awaitLatch(cancelThreadRunning, STARTUP, "The thread cancel-x never started");
        Mono<Void> cancel = awaitValue(cancelling, "The cancel of x did not return while the wrapped subscribe of x was under way, " + describe(cancelThread));
        CompletableFuture<Void> cancelled = cancel.toFuture();
        assertThat(cancelled).as("a cancel of x issued while the wrapped subscribe of x is under way").isNotDone();
        assertThat(wrapped.events).as("what the wrapped model was told while the subscribe was under way").isEmpty();
        released.countDown();

        await(subscribeOfX, "The subscribe of x did not return");
        await(cancelled, "The cancel of x did not complete");
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isFalse();
        assertThat(wrapped.isPaused("x")).as("whether the wrapped model holds x paused").isFalse();
    }

    @Test
    void a_dynamic_start_that_blocks_on_a_cancel_of_its_own_id_returns_since_the_wrapped_model_is_cancelled_at_once() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        AtomicReference<Throwable> blockFailure = new AtomicReference<>();
        AtomicReference<List<String>> toldWhenTheBlockReturned = new AtomicReference<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorDcbCatchupSubscriptionModel.class)) {
                return null;
            }
            try {
                catchup.cancelSubscription("x").block(BLOCKED);
                toldWhenTheBlockReturned.set(List.copyOf(wrapped.events));
            } catch (Throwable t) {
                blockFailure.set(t);
            }
            return StartAt.now();
        }), cloudEvent -> Mono.empty())).start();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(blockFailure.get()).as("what the block on a cancel of x inside the start of x threw").isNull();
        assertThat(toldWhenTheBlockReturned.get()).as("what the wrapped model had been told when the block returned").containsExactly("subscribed:x", "cancelled:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isFalse();
    }

    @Test
    void an_event_the_wrapped_model_delivers_while_the_wrapped_subscribe_is_under_way_reaches_the_action_when_the_id_was_not_cancelled() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        HeldStart held = new HeldStart();
        List<String> reached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> reached.add(cloudEvent.getId())))).start();
        held.awaitInside();

        wrapped.deliver("x", anEvent("e1"));
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(reached).as("events that reached the action of x").containsExactly("e1");
    }

    @Test
    void an_event_the_wrapped_model_delivers_after_a_cancel_from_another_thread_returned_does_not_reach_the_action_though_the_wrapped_subscribe_is_still_under_way() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        HeldStart held = new HeldStart();
        List<String> reached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> reached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        CompletableFuture<Void> cancel = cancelFromAnotherThread(catchup, "x");
        assertThat(cancel).as("a cancel of x issued while the wrapped subscribe of x is under way").isNotDone();

        wrapped.deliver("x", anEvent("e1"));
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        await(cancel, "The cancel of x did not complete");
        assertThat(reached).as("events that reached the action of x after its cancel returned").isEmpty();
    }

    // Keeps the time the threads of a test take to load these classes out of what the deadline measures
    private static void loadClassesOfASubscribeAndACancel(ReactorDcbCatchupSubscriptionModel catchup) {
        catchup.subscribe("warm-up", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("warm-up").block(STARTUP);
    }

    private static CloudEvent anEvent(String id) {
        return DcbCloudEvents.withTags(CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build(), Set.of());
    }

    // The call itself runs on a thread of its own, so a call that blocks fails at the deadline
    private static CompletableFuture<Void> cancelFromAnotherThread(ReactorDcbCatchupSubscriptionModel catchup, String subscriptionId) {
        CompletableFuture<Mono<Void>> cancelling = new CompletableFuture<>();
        CountDownLatch running = new CountDownLatch(1);
        Thread thread = daemon("cancel-" + subscriptionId, new CompletableFuture<>(), () -> {
            running.countDown();
            cancelling.complete(catchup.cancelSubscription(subscriptionId));
        });
        thread.start();
        awaitLatch(running, STARTUP, "The thread cancel-" + subscriptionId + " never started");
        return awaitValue(cancelling, "The cancel of " + subscriptionId + " did not return, " + describe(thread)).toFuture();
    }

    // A start that answers null to the catch-up and then waits in the call the wrapped model makes, until released
    private static final class HeldStart {
        private final CountDownLatch inside = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        StartAt startAt() {
            return StartAt.dynamic(context -> {
                if (context.hasSubscriptionModelType(ReactorDcbCatchupSubscriptionModel.class)) {
                    return null;
                }
                inside.countDown();
                awaitLatch(released, DEADLINE.multipliedBy(2), "The wrapped subscribe was never released");
                return StartAt.now();
            });
        }

        void awaitInside() {
            awaitLatch(inside, STARTUP, "The wrapped model never evaluated the start");
        }

        void release() {
            released.countDown();
        }
    }

    // Answers null to the catch-up, which sends the subscribe straight to live, and cancels the other id from the call
    // the wrapped model makes once both subscribes are inside it. Fires the cancel without blocking on it.
    private static StartAt cancellingFromTheWrappedModel(ReactorDcbCatchupSubscriptionModel catchup, String other, CountDownLatch bothInside,
                                                         Map<String, CompletableFuture<Void>> cancels, String own) {
        return StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorDcbCatchupSubscriptionModel.class)) {
                return null;
            }
            bothInside.countDown();
            awaitLatch(bothInside, STARTUP, "The other subscribe never reached the wrapped model");
            cancels.put(own, catchup.cancelSubscription(other).toFuture());
            return StartAt.now();
        });
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

    private static void await(CompletableFuture<?> future, String failure) {
        awaitValue(future, failure);
    }

    private static <T> T awaitValue(CompletableFuture<T> future, String failure) {
        try {
            return future.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(failure + " within " + DEADLINE.toSeconds() + " seconds");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(failure + ", it failed with " + e.getCause(), e.getCause());
        }
    }

    private static void awaitLatch(CountDownLatch latch, Duration timeout, String failure) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError(failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static String describe(Thread thread) {
        return "thread " + thread.getName() + " is " + thread.getState() + " at " + Arrays.stream(thread.getStackTrace()).limit(6).toList();
    }

    // A named subscription model that, like ReactorMongoSubscriptionModel, evaluates the start on the calling thread
    // inside subscribe(..), against its own class, and holds the subscription once that returned
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> events = new CopyOnWriteArrayList<>();
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        // Holds the id from before it evaluates the start, as ReactorMongoSubscriptionModel does
        volatile boolean registersBeforeTheStart;
        final Map<String, List<Function<CloudEvent, Mono<Void>>>> actions = new ConcurrentHashMap<>();

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
            actions.computeIfAbsent(subscriptionId, id -> new CopyOnWriteArrayList<>()).add(action);
            if (registersBeforeTheStart) {
                register(subscriptionId);
            }
            startAt.get(new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
            if (!registersBeforeTheStart) {
                register(subscriptionId);
            }
            return handle(subscriptionId);
        }

        // Hands the event to each action of the id, the way the wrapped model does for an event it reads
        void deliver(String subscriptionId, CloudEvent event) {
            actions.getOrDefault(subscriptionId, List.of()).forEach(action -> action.apply(event).block(BLOCKED));
        }

        private void register(String subscriptionId) {
            subscribed.add(subscriptionId);
            events.add("subscribed:" + subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
            events.add("cancelled:" + subscriptionId);
            return Mono.empty();
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

    // Nothing is read here, since the subscribes under test go straight to live
    private static final class UnusedDcbEventStore implements DcbEventStore, DcbCatchupReads {
        @Override
        public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("Nothing must be read"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("append must not be called"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("append must not be called"));
        }
    }
}
