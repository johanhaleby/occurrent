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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subscribe that goes straight to live calls the wrapped model's {@code subscribe(..)}, which can call the caller's
 * dynamic {@link StartAt} again on the calling thread. That call holds no lock of the catch-up model, so the function
 * can cancel another subscription through the catch-up. A cancel of an id whose subscribe is under way returns without
 * waiting for it, and its Mono completes once the subscribe returned and the wrapped model cancelled what it made. A
 * second subscribe of that id is refused meanwhile. Code on the thread of the wrapped subscribe is neither refused nor
 * made to wait for it. A thread that never returns fails its test at the deadline rather
 * than hanging the build.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscribeUnderWayTest {

    private static final Duration STARTUP = Duration.ofSeconds(60);
    private static final Duration DEADLINE = Duration.ofSeconds(15);
    private static final Duration BLOCKED = Duration.ofSeconds(5);
    private static final StreamSubscriptionFilter FILTER = StreamSubscriptionFilter.filter(Filter.all());

    @Test
    void two_subscribes_return_when_the_dynamic_start_of_each_cancels_the_other_subscription_without_waiting_for_the_cancel() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
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
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();

        CompletableFuture<Void> cancel = returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture());
        assertThat(cancel).as("a cancel of x issued while the wrapped subscribe of x is under way").isNotDone();
        assertThat(wrapped.events).as("what the wrapped model was told while the subscribe was under way").isEmpty();
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        await(cancel, "The cancel of x did not complete");
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isFalse();
        assertThat(wrapped.isPaused("x")).as("whether the wrapped model holds x paused").isFalse();
    }

    @Test
    void a_second_straight_to_live_subscribe_of_an_id_is_refused_while_the_wrapped_subscribe_of_it_is_under_way() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();

        Throwable refusal = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(refusal).as("what the second subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x");
    }

    @Test
    void a_second_subscribe_with_catch_up_of_an_id_is_refused_while_the_wrapped_subscribe_of_it_is_under_way() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();

        Throwable refusal = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()));
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(refusal).as("what the second subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x");
    }

    @Test
    void a_subscribe_of_an_id_goes_through_once_the_wrapped_subscribe_of_it_returned_and_it_was_cancelled() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        held.release();
        await(subscribeOfX, "The subscribe of x did not return");
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");

        Throwable failure = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertThat(failure).as("what the subscribe after the cancel threw").isNull();
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x");
    }

    @Test
    void a_dynamic_start_that_blocks_on_a_cancel_of_its_own_id_returns_since_the_wrapped_model_is_cancelled_at_once() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        AtomicReference<Throwable> blockFailure = new AtomicReference<>();
        AtomicReference<List<String>> toldWhenTheBlockReturned = new AtomicReference<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
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
    void a_subscribe_returns_when_its_dynamic_start_cancels_another_subscription_whose_cancel_blocks_on_a_cancel_of_the_subscription_under_way() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        catchup.subscribe("b", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        wrapped.onCancel.put("b", () -> {
            try {
                catchup.cancelSubscription("a").block(BLOCKED);
            } catch (Throwable t) {
                hookFailure.set(t);
            }
        });
        CompletableFuture<Void> subscribeOfA = new CompletableFuture<>();
        daemon("subscribe-a", subscribeOfA, () -> catchup.subscribe("a", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
                return null;
            }
            catchup.cancelSubscription("b").subscribe();
            return StartAt.now();
        }), cloudEvent -> Mono.empty())).start();

        await(subscribeOfA, "The subscribe of a did not return");
        assertThat(hookFailure.get()).as("what the block on a cancel of a inside the cancel of b threw").isNull();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:b", "cancelled:b", "cancelled:a", "subscribed:a");
    }

    @Test
    void a_dynamic_start_that_cancels_its_own_id_without_blocking_can_subscribe_the_id_again() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> cancel = new AtomicReference<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
                return null;
            }
            cancel.set(catchup.cancelSubscription("x").toFuture());
            try {
                catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
            } catch (Throwable t) {
                nestedFailure.set(t);
            }
            return StartAt.now();
        }), cloudEvent -> Mono.empty())).start();

        await(subscribeOfX, "The subscribe of x did not return");
        await(cancel.get(), "The cancel of x did not complete");
        assertThat(nestedFailure.get()).as("what the subscribe of x from inside the start of x threw").isNull();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
    }

    @Test
    void a_subscribe_from_the_continuation_of_a_cancel_that_waited_for_the_wrapped_subscribe_goes_through() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        AtomicReference<Throwable> continuationFailure = new AtomicReference<>();
        CompletableFuture<Void> cancel = returnedWithin("cancel-x", () -> catchup.cancelSubscription("x")
                .doOnSuccess(done -> continuationFailure.set(failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()))))
                .toFuture());
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        await(cancel, "The cancel of x did not complete");
        assertThat(continuationFailure.get()).as("what the subscribe of x from the continuation of the cancel threw").isNull();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
    }

    @Test
    void both_cancels_that_waited_for_a_wrapped_subscribe_that_threw_complete_and_the_id_can_be_subscribed_again() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.failingSubscribes.add("x");
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        CompletableFuture<Void> firstCancel = returnedWithin("cancel-x-1", () -> catchup.cancelSubscription("x").toFuture());
        CompletableFuture<Void> secondCancel = returnedWithin("cancel-x-2", () -> catchup.cancelSubscription("x").toFuture());
        held.release();

        assertThat(subscribeOfX).as("the subscribe of x").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(IllegalStateException.class);
        await(firstCancel, "The first cancel of x did not complete");
        await(secondCancel, "The second cancel of x did not complete");
        wrapped.failingSubscribes.clear();
        Throwable failure = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertThat(failure).as("what the subscribe after the cancels threw").isNull();
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
        assertThat(wrapped.events).as("what the wrapped model was told").containsOnlyOnce("subscribed:x");
    }

    // Keeps the time the threads of a test take to load these classes out of what the deadline measures
    private static void loadClassesOfASubscribeAndACancel(ReactorStreamCatchupSubscriptionModel catchup) {
        catchup.subscribe("warm-up", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("warm-up").block(STARTUP);
    }

    // Answers null to the catch-up, which sends the subscribe straight to live, and cancels the other id from the call
    // the wrapped model makes once both subscribes are inside it. Fires the cancel without blocking on it.
    private static StartAt cancellingFromTheWrappedModel(ReactorStreamCatchupSubscriptionModel catchup, String other, CountDownLatch bothInside,
                                                         Map<String, CompletableFuture<Void>> cancels, String own) {
        return StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
                return null;
            }
            bothInside.countDown();
            awaitLatch(bothInside, STARTUP, "The other subscribe never reached the wrapped model");
            cancels.put(own, catchup.cancelSubscription(other).toFuture());
            return StartAt.now();
        });
    }

    // A start that answers null to the catch-up and then waits in the call the wrapped model makes, until released
    private static final class HeldStart {
        private final CountDownLatch inside = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        StartAt startAt() {
            return StartAt.dynamic(context -> {
                if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
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

    // The call itself runs on a thread of its own, so a call that blocks fails at the deadline
    private static <T> CompletableFuture<T> returnedWithin(String name, Supplier<CompletableFuture<T>> call) {
        CompletableFuture<CompletableFuture<T>> returned = new CompletableFuture<>();
        CountDownLatch running = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            running.countDown();
            try {
                returned.complete(call.get());
            } catch (Throwable t) {
                returned.completeExceptionally(t);
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        awaitLatch(running, STARTUP, "The thread " + name + " never started");
        try {
            return returned.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("The call on " + name + " did not return within " + DEADLINE.toSeconds() + " seconds, " + describe(thread));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    // What the call threw, or null. The call runs on a thread of its own, so a call that blocks fails at the deadline
    private static @Nullable Throwable failureOf(Runnable call) {
        CompletableFuture<Void> returned = new CompletableFuture<>();
        CountDownLatch running = new CountDownLatch(1);
        Thread thread = daemon("second-subscribe", returned, () -> {
            running.countDown();
            call.run();
        });
        thread.start();
        awaitLatch(running, STARTUP, "The thread second-subscribe never started");
        try {
            returned.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        } catch (TimeoutException e) {
            throw new AssertionError("The second subscribe did not return within " + DEADLINE.toSeconds() + " seconds, " + describe(thread));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static void await(CompletableFuture<?> future, String failure) {
        try {
            future.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
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

    // Nothing is replayed here, since the subscribes under test go straight to live or are refused first
    private static final class UnusedReader implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("Nothing must be replayed"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(0L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // A named subscription model that, like ReactorMongoSubscriptionModel, evaluates the start on the calling thread
    // inside subscribe(..), against its own class, and holds the subscription once that returned
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> events = new CopyOnWriteArrayList<>();
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final Set<String> failingSubscribes = ConcurrentHashMap.newKeySet();
        // Run on the thread that cancels the id, as the cancel of a Mongo subscription runs its cancel hooks
        final Map<String, Runnable> onCancel = new ConcurrentHashMap<>();
        // Holds the id from before it evaluates the start, as ReactorMongoSubscriptionModel does
        volatile boolean registersBeforeTheStart;

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
            if (registersBeforeTheStart) {
                register(subscriptionId);
            }
            startAt.get(new SubscriptionModelContext(ReactorMongoSubscriptionModel.class));
            if (failingSubscribes.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped subscribe of " + subscriptionId + " failed");
            }
            if (!registersBeforeTheStart) {
                register(subscriptionId);
            }
            return handle(subscriptionId);
        }

        private void register(String subscriptionId) {
            subscribed.add(subscriptionId);
            events.add("subscribed:" + subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            boolean held = subscribed.remove(subscriptionId);
            paused.remove(subscriptionId);
            events.add("cancelled:" + subscriptionId);
            Runnable hook = onCancel.remove(subscriptionId);
            if (held && hook != null) {
                hook.run();
            }
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
}
