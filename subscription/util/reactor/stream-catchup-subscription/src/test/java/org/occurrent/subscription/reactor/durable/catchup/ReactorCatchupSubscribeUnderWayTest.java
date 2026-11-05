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
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe that goes straight to live calls the wrapped model's {@code subscribe(..)}, which can call the caller's
 * dynamic {@link StartAt} again on the calling thread. That call holds no lock of the catch-up model, so the function
 * can cancel another subscription through the catch-up. A cancel of an id whose subscribe is under way cancels in the
 * wrapped model at once, and its Mono completes once that cancel did. From then on the action of that subscribe gets no
 * event, and once the subscribe returned the wrapped model is told to cancel again if it holds the id, unless a later
 * subscribe owns it. A second subscribe of the id is refused while the subscribe under way was not cancelled. A thread
 * that never returns fails its test at the deadline rather than hanging the build.
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
    void a_cancel_of_a_subscription_whose_subscribe_is_under_way_cancels_in_the_wrapped_model_at_once_and_again_once_the_subscribe_returned() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();

        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete while the subscribe of x was under way");
        List<String> toldWhileUnderWay = List.copyOf(wrapped.events);
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(toldWhileUnderWay).as("what the wrapped model was told while the subscribe was under way").containsExactly("cancelled:x");
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x");
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
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:b", "cancelled:b", "cancelled:a", "subscribed:a", "cancelled:a");
        assertThat(wrapped.isRunning("a")).as("whether the wrapped model runs a").isFalse();
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
    void a_subscribe_from_another_thread_goes_through_once_a_nested_subscribe_of_the_id_was_cancelled_and_the_outer_subscribe_leaves_it_running_when_it_returns() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        HeldStart held = new HeldStart();
        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
                return null;
            }
            try {
                catchup.cancelSubscription("x").subscribe();
                catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
                catchup.cancelSubscription("x").subscribe();
            } catch (Throwable t) {
                nestedFailure.set(t);
            }
            return held.startAt().get(context);
        }), cloudEvent -> Mono.empty())).start();
        held.awaitInside();

        Throwable failureWhileUnderWay = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(nestedFailure.get()).as("what the nested subscribe of x threw").isNull();
        assertThat(failureWhileUnderWay).as("what the subscribe of x from another thread threw while the outer subscribe was under way").isNull();
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x", "cancelled:x", "subscribed:x");
    }

    @Test
    void a_subscribe_from_the_continuation_of_a_cancel_of_a_subscription_whose_subscribe_is_under_way_goes_through_and_gets_the_events() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        List<String> outerReached = new CopyOnWriteArrayList<>();
        List<String> continuationReached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> outerReached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        AtomicReference<Throwable> continuationFailure = new AtomicReference<>();
        CompletableFuture<Void> cancel = returnedWithin("cancel-x", () -> catchup.cancelSubscription("x")
                .doOnSuccess(done -> continuationFailure.set(failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(),
                        cloudEvent -> Mono.fromRunnable(() -> continuationReached.add(cloudEvent.getId()))))))
                .toFuture());
        await(cancel, "The cancel of x did not complete");
        held.release();
        await(subscribeOfX, "The subscribe of x did not return");

        wrapped.deliver("x", anEvent("e1"));

        assertThat(continuationFailure.get()).as("what the subscribe of x from the continuation of the cancel threw").isNull();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "subscribed:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
        assertThat(continuationReached).as("events that reached the action of the subscribe from the continuation").containsExactly("e1");
        assertThat(outerReached).as("events that reached the action of the cancelled subscribe").isEmpty();
    }

    @Test
    void two_cancels_of_an_id_whose_wrapped_subscribe_throws_complete_and_the_id_can_be_subscribed_again() {
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

    @Test
    void an_event_the_wrapped_model_delivers_while_the_wrapped_subscribe_is_under_way_reaches_the_action_when_the_id_was_not_cancelled() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
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
    void an_event_the_wrapped_model_delivers_after_a_cancel_from_another_thread_completed_does_not_reach_the_action_though_the_wrapped_subscribe_is_still_under_way() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        List<String> reached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> reached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete while the subscribe of x was under way");

        wrapped.deliver("x", anEvent("e1"));
        boolean runningWhileUnderWay = catchup.isRunning("x");
        boolean pausedWhileUnderWay = catchup.isPaused("x");
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(reached).as("events that reached the action of x after its cancel completed").isEmpty();
        assertThat(runningWhileUnderWay).as("whether the catch-up reports x running while the subscribe is under way").isFalse();
        assertThat(pausedWhileUnderWay).as("whether the catch-up reports x paused while the subscribe is under way").isFalse();
    }

    @Test
    void a_first_cancel_that_completes_after_the_second_cancel_started_leaves_x_not_running_while_the_second_is_held_and_after_it_completes() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.holdsCancels = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        CompletableFuture<Void> firstCancel = returnedWithin("cancel-x-1", () -> catchup.cancelSubscription("x").toFuture());
        held.release();
        await(subscribeOfX, "The subscribe of x did not return");
        int cancelsAsked = wrapped.cancelsHeld();

        wrapped.completeCancel(0);
        await(firstCancel, "The first cancel of x did not complete");
        boolean runningWhileTheSecondCancelIsHeld = catchup.isRunning("x");
        wrapped.completeCancel(1);

        assertThat(cancelsAsked).as("cancels the wrapped model was asked for once the subscribe returned").isEqualTo(2);
        assertThat(runningWhileTheSecondCancelIsHeld).as("whether the catch-up reports x running while the second cancel is held").isFalse();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancel-asked:x", "subscribed:x", "cancel-asked:x", "cancelled:x", "cancelled:x");
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isFalse();
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isFalse();
    }

    @Test
    void a_second_cancel_that_throws_leaves_the_catch_up_reporting_x_not_running_while_the_wrapped_model_runs_it_and_a_later_cancel_stops_x() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        List<String> reached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> reached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        await(returnedWithin("cancel-x-1", () -> catchup.cancelSubscription("x").toFuture()), "The first cancel of x did not complete while the subscribe of x was under way");
        wrapped.failCancelsFromNowOn();
        held.release();
        await(subscribeOfX, "The subscribe of x did not return");

        boolean wrappedRunsX = wrapped.isRunning("x");
        boolean catchupRunsX = catchup.isRunning("x");
        wrapped.deliver("x", anEvent("e1"));
        wrapped.succeedCancels();
        await(returnedWithin("cancel-x-3", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x after the failed one did not complete");
        Throwable failure = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertThat(wrappedRunsX).as("whether the wrapped model runs x after the second cancel threw").isTrue();
        assertThat(catchupRunsX).as("whether the catch-up reports x running after the second cancel threw").isFalse();
        assertThat(reached).as("events that reached the action of the cancelled subscribe").isEmpty();
        assertThat(failure).as("what the subscribe after the later cancel threw").isNull();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x", "subscribed:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
    }

    @Test
    void a_pause_or_resume_of_x_is_refused_as_unknown_while_the_wrapped_model_runs_what_a_cancelled_subscribe_left_after_its_second_cancel_threw() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        await(returnedWithin("cancel-x-1", () -> catchup.cancelSubscription("x").toFuture()), "The first cancel of x did not complete while the subscribe of x was under way");
        wrapped.failCancelsFromNowOn();
        held.release();
        await(subscribeOfX, "The subscribe of x did not return");

        Throwable pause = failureOf(() -> catchup.pauseSubscription("x"));
        boolean wrappedPausedX = wrapped.isPaused("x");
        Throwable resume = failureOf(() -> catchup.resumeSubscription("x"));

        assertAll(
                () -> assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue(),
                () -> assertThat(pause).as("what a pause of x throws").isInstanceOf(UnknownSubscriptionException.class),
                () -> assertThat(wrappedPausedX).as("whether the pause reached the wrapped model").isFalse(),
                () -> assertThat(resume).as("what a resume of x throws").isInstanceOf(UnknownSubscriptionException.class),
                () -> assertThat(catchup.isPaused("x")).as("whether the catch-up reports x paused").isFalse());
    }

    @Test
    void the_action_of_a_subscribe_cancelled_while_under_way_gets_no_event_while_a_nested_subscribe_of_the_id_gets_them() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.registersBeforeTheStart = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        loadClassesOfASubscribeAndACancel(catchup);
        wrapped.events.clear();
        HeldStart held = new HeldStart();
        List<String> outerReached = new CopyOnWriteArrayList<>();
        List<String> innerReached = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(ReactorStreamCatchupSubscriptionModel.class)) {
                return null;
            }
            try {
                catchup.cancelSubscription("x").subscribe();
                catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> innerReached.add(cloudEvent.getId())));
            } catch (Throwable t) {
                nestedFailure.set(t);
            }
            return held.startAt().get(context);
        }), cloudEvent -> Mono.fromRunnable(() -> outerReached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        wrapped.deliver("x", anEvent("e1"));
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        held.release();

        await(subscribeOfX, "The subscribe of x did not return");
        assertThat(nestedFailure.get()).as("what the nested subscribe of x threw").isNull();
        assertThat(innerReached).as("events that reached the action of the nested subscribe").containsExactly("e1");
        assertThat(outerReached).as("events that reached the action of the outer subscribe").isEmpty();
        assertThat(wrapped.isRunning("x") || wrapped.isPaused("x")).as("whether the wrapped model holds x").isFalse();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x", "cancelled:x");
    }

    @Test
    void a_cancelled_subscribe_that_returns_while_the_wrapped_model_moves_the_id_between_running_and_paused_cancels_it_there_again() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart cancelledStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        wrapped.movingBetweenRunningAndPaused = true;

        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");
        wrapped.movingBetweenRunningAndPaused = false;

        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x");
        assertThat(wrapped.subscriptionIds()).as("the ids the wrapped model holds").isEmpty();
    }

    @Test
    void a_straight_to_live_subscribe_that_the_wrapped_model_refuses_since_a_cancelled_subscribe_took_the_id_after_it_cancels_what_that_subscribe_left() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart cancelledStart = new HeldStart();
        HeldStart laterStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        CompletableFuture<Void> laterSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        daemon("subscribe-x-again", laterSubscribe, () -> catchup.subscribe("x", FILTER, laterStart.startAt(), cloudEvent -> Mono.empty())).start();
        laterStart.awaitInside();

        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");
        laterStart.release();

        assertThat(laterSubscribe).as("the subscribe of x made after the cancel").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "refused:x", "cancelled:x");
        assertThat(wrapped.isRunning("x") || wrapped.isPaused("x")).as("whether the wrapped model holds x").isFalse();
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isFalse();
        assertThat(failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()))).as("what a subscribe of x after that threw").isNull();
    }

    @Test
    void a_subscribe_with_catch_up_whose_handover_the_wrapped_model_refuses_since_a_cancelled_subscribe_took_the_id_during_the_replay_cancels_what_that_subscribe_left() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart cancelledStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        Subscription replaying = catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();

        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");
        reader.release();

        assertThat(replaying.waitUntilStarted().toFuture()).as("the start of the subscribe with catch-up").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "refused:x", "cancelled:x");
        assertThat(wrapped.isRunning("x") || wrapped.isPaused("x")).as("whether the wrapped model holds x").isFalse();
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isFalse();
    }

    @Test
    void a_subscribe_with_catch_up_made_after_a_cancel_runs_when_the_wrapped_model_took_the_id_for_the_cancelled_subscribe_before_its_start() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        wrapped.registersBeforeTheStart = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart cancelledStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        Subscription replaying = catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();

        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");
        reader.release();

        await(replaying.waitUntilStarted().toFuture(), "The subscribe with catch-up did not start");
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("subscribed:x", "cancelled:x", "subscribed:x");
        assertThat(wrapped.isRunning("x")).as("whether the wrapped model runs x").isTrue();
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isTrue();
    }

    @Test
    void a_subscribe_the_wrapped_model_refuses_after_a_cancelled_catch_up_replaced_a_cancelled_subscribe_cancels_what_that_subscribe_left() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart cancelledStart = new HeldStart();
        HeldStart thirdStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        CompletableFuture<Void> thirdSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        await(returnedWithin("cancel-x-2", () -> catchup.cancelSubscription("x").toFuture()), "c2");
        daemon("subscribe-x-3", thirdSubscribe, () -> catchup.subscribe("x", FILTER, thirdStart.startAt(), cloudEvent -> Mono.empty())).start();
        thirdStart.awaitInside();
        cancelledStart.release();
        await(cancelledSubscribe, "s1");
        thirdStart.release();
        assertThat(thirdSubscribe).failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class);
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));
        assertAll(
                () -> assertThat(wrapped.events).containsExactly("cancelled:x", "cancelled:x", "subscribed:x", "refused:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(later).as("what a subscribe of x once the cancelled one returned throws").isNull());
    }

    @Test
    void a_cancel_of_a_catch_up_that_replaced_a_cancelled_subscribe_completing_after_that_subscribe_took_the_id_cancels_it_there() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart cancelledStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        wrapped.holdsCancels = true;
        CompletableFuture<Void> secondCancel = catchup.cancelSubscription("x").toFuture();
        cancelledStart.release();
        await(cancelledSubscribe, "s1");
        wrapped.holdsCancels = false;
        wrapped.completeCancel(0);
        await(secondCancel, "c2");
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));
        assertAll(
                () -> assertThat(wrapped.events).containsExactly("cancelled:x", "cancel-asked:x", "subscribed:x", "cancelled:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(later).as("what a subscribe of x once the cancels completed throws").isNull());
    }

    @Test
    void a_subscribe_the_wrapped_model_refuses_after_two_cancelled_subscribes_and_a_cancelled_catch_up_replaced_one_another_cancels_what_the_first_left() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart h1 = new HeldStart();
        HeldStart h2 = new HeldStart();
        HeldStart h4 = new HeldStart();
        CompletableFuture<Void> s1 = new CompletableFuture<>();
        CompletableFuture<Void> s2 = new CompletableFuture<>();
        CompletableFuture<Void> s4 = new CompletableFuture<>();
        daemon("s1", s1, () -> catchup.subscribe("x", FILTER, h1.startAt(), cloudEvent -> Mono.empty())).start();
        h1.awaitInside();
        await(returnedWithin("c1", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        daemon("s2", s2, () -> catchup.subscribe("x", FILTER, h2.startAt(), cloudEvent -> Mono.empty())).start();
        h2.awaitInside();
        await(returnedWithin("c2", () -> catchup.cancelSubscription("x").toFuture()), "c2");
        catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        h2.release();
        await(s2, "s2");
        await(returnedWithin("c3", () -> catchup.cancelSubscription("x").toFuture()), "c3");
        daemon("s4", s4, () -> catchup.subscribe("x", FILTER, h4.startAt(), cloudEvent -> Mono.empty())).start();
        h4.awaitInside();
        h1.release();
        await(s1, "s1");
        h4.release();
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertAll(
                () -> assertThat(s4).as("what the last subscribe under way throws").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(wrapped.events).as("what the wrapped model was told").containsExactly(
                        "cancelled:x", "cancelled:x", "subscribed:x", "cancelled:x", "subscribed:x", "refused:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(later).as("what a subscribe of x once every subscribe returned throws").isNull());
    }

    @Test
    void a_subscribe_made_as_a_cancel_completes_and_refused_by_what_a_cancelled_subscribe_took_cancels_it() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart h1 = new HeldStart();
        HeldStart h5 = new HeldStart();
        CompletableFuture<Void> s1 = new CompletableFuture<>();
        CompletableFuture<Void> s5 = new CompletableFuture<>();
        daemon("s1", s1, () -> catchup.subscribe("x", FILTER, h1.startAt(), cloudEvent -> Mono.empty())).start();
        h1.awaitInside();
        await(returnedWithin("c1", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        wrapped.holdsCancels = true;
        Mono<Void> c3 = catchup.cancelSubscription("x");
        wrapped.holdsCancels = false;
        c3.subscribe(null, null, () -> {
            try {
                catchup.subscribe("x", FILTER, h5.startAt(), cloudEvent -> Mono.empty());
                s5.complete(null);
            } catch (Throwable t) {
                s5.completeExceptionally(t);
            }
        });
        CompletableFuture<Void> completer = new CompletableFuture<>();
        daemon("complete", completer, () -> wrapped.completeCancel(0)).start();
        h5.awaitInside();
        h1.release();
        await(s1, "s1");
        h5.release();
        await(completer, "completer");
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertAll(
                () -> assertThat(s5).as("what the subscribe made as the cancel completed throws").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(wrapped.events).as("what the wrapped model was told").containsExactly(
                        "cancelled:x", "cancel-asked:x", "cancelled:x", "subscribed:x", "refused:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(later).as("what a subscribe of x once every subscribe returned throws").isNull());
    }

    @Test
    void a_cancelled_subscribe_that_returns_after_a_shutdown_returns_its_subscription() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.empty())).start();
        held.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete while the subscribe of x was under way");

        catchup.shutdown();
        held.release();

        assertThat(subscribeOfX).as("the subscribe of x that returned after the shutdown").succeedsWithin(DEADLINE);
    }

    @Test
    void a_cancel_that_throws_while_a_cancelled_subscribe_returns_leaves_the_catch_up_reporting_x_not_running_while_the_wrapped_model_runs_it() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart held = new HeldStart();
        List<String> reached = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> s1 = new CompletableFuture<>();
        daemon("s1", s1, () -> catchup.subscribe("x", FILTER, held.startAt(), cloudEvent -> Mono.fromRunnable(() -> reached.add(cloudEvent.getId())))).start();
        held.awaitInside();
        await(returnedWithin("c1", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        wrapped.failCancelsFromNowOn();
        // Inside the wrapped cancel of c2, before it throws, the cancelled subscribe returns holding x
        wrapped.beforeNextCancel.set(() -> {
            held.release();
            await(s1, "s1");
        });
        Throwable c2 = failureOf(() -> catchup.cancelSubscription("x"));
        boolean wrappedRunsX = wrapped.isRunning("x");
        boolean catchupRunsX = catchup.isRunning("x");
        wrapped.deliver("x", anEvent("e1"));

        assertAll(
                () -> assertThat(c2).as("what c2 threw").isInstanceOf(IllegalStateException.class),
                () -> assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x"),
                () -> assertThat(wrappedRunsX).as("whether the wrapped model runs x").isTrue(),
                () -> assertThat(catchupRunsX).as("whether the catch-up reports x running, after every cancel threw").isFalse(),
                () -> assertThat(reached).as("events that reached the action of the cancelled subscribe").isEmpty());
    }

    @Test
    void a_cancel_of_a_replaying_catch_up_that_throws_still_cancels_what_a_cancelled_subscribe_took() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart h1 = new HeldStart();
        CompletableFuture<Void> s1 = new CompletableFuture<>();
        daemon("s1", s1, () -> catchup.subscribe("x", FILTER, h1.startAt(), cloudEvent -> Mono.empty())).start();
        h1.awaitInside();
        await(returnedWithin("c1", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        Subscription k2 = catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        wrapped.failCancelsFromNowOn();
        Throwable c2 = failureOf(() -> catchup.cancelSubscription("x"));
        wrapped.succeedCancels();
        h1.release();
        await(s1, "s1");
        Set<String> heldOnceAllReturned = wrapped.subscriptionIds();
        boolean catchupRunsX = catchup.isRunning("x");
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertAll(
                () -> assertThat(c2).as("what c2 threw").isInstanceOf(IllegalStateException.class),
                () -> assertThat(k2.waitUntilStarted().toFuture()).as("the start of k2").failsWithin(DEADLINE),
                () -> assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(heldOnceAllReturned).as("ids the wrapped model holds once s1 returned, with no live subscription of x").isEmpty(),
                () -> assertThat(catchupRunsX).as("whether the catch-up reports x running").isFalse(),
                () -> assertThat(later).as("what a subscribe of x once every subscribe returned throws").isNull());
    }

    @Test
    void a_cancel_of_a_replaying_catch_up_that_throws_after_a_cancelled_subscribe_took_the_id_cancels_what_that_subscribe_left() {
        WrappedModel wrapped = new WrappedModel();
        HeldReplay reader = new HeldReplay();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        HeldStart h1 = new HeldStart();
        CompletableFuture<Void> s1 = new CompletableFuture<>();
        daemon("s1", s1, () -> catchup.subscribe("x", FILTER, h1.startAt(), cloudEvent -> Mono.empty())).start();
        h1.awaitInside();
        await(returnedWithin("c1", () -> catchup.cancelSubscription("x").toFuture()), "c1");
        Subscription k2 = catchup.subscribe("x", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        reader.awaitReading();
        h1.release();
        await(s1, "s1");
        // Only the cancel of c2 throws
        wrapped.failCancelsFromNowOn();
        wrapped.beforeNextCancel.set(() -> wrapped.beforeNextCancel.set(wrapped::succeedCancels));

        Throwable c2 = failureOf(() -> catchup.cancelSubscription("x"));
        Set<String> heldOnceC2Threw = wrapped.subscriptionIds();
        boolean catchupRunsX = catchup.isRunning("x");
        Throwable later = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));

        assertAll(
                () -> assertThat(c2).as("what c2 threw").isInstanceOf(IllegalStateException.class),
                () -> assertThat(k2.waitUntilStarted().toFuture()).as("the start of k2").failsWithin(DEADLINE),
                () -> assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x", "subscribed:x"),
                () -> assertThat(heldOnceC2Threw).as("ids the wrapped model holds once c2 threw, with no live subscription of x").isEmpty(),
                () -> assertThat(catchupRunsX).as("whether the catch-up reports x running").isFalse(),
                () -> assertThat(later).as("what a subscribe of x after that throws").isNull());
    }

    @Test
    void the_catch_up_reports_x_not_running_while_the_wrapped_model_cancels_what_a_cancelled_subscribe_left_after_it_refused_a_later_subscribe() {
        WrappedModel wrapped = new WrappedModel();
        wrapped.refusesDuplicates = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart cancelledStart = new HeldStart();
        HeldStart laterStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        CompletableFuture<Void> laterSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        daemon("subscribe-x-again", laterSubscribe, () -> catchup.subscribe("x", FILTER, laterStart.startAt(), cloudEvent -> Mono.empty())).start();
        laterStart.awaitInside();
        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");
        wrapped.holdsCancels = true;

        laterStart.release();
        assertThat(laterSubscribe).as("the subscribe of x made after the cancel").failsWithin(DEADLINE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class);
        int cancelsAsked = wrapped.cancelsHeld();
        boolean runningWhileTheCancelIsHeld = catchup.isRunning("x");
        wrapped.completeCancel(0);

        assertThat(cancelsAsked).as("cancels the wrapped model was asked for once the later subscribe was refused").isEqualTo(1);
        assertThat(runningWhileTheCancelIsHeld).as("whether the catch-up reports x running while the cancel of what the cancelled subscribe left is held").isFalse();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "refused:x", "cancel-asked:x", "cancelled:x");
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isFalse();
        assertThat(failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()))).as("what a subscribe of x after that threw").isNull();
    }

    @Test
    void the_catch_up_reports_x_not_running_while_a_cancelled_subscribe_takes_it_after_a_later_subscribe_failed_in_the_wrapped_model() {
        WrappedModel wrapped = new WrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedReader());
        HeldStart cancelledStart = new HeldStart();
        CompletableFuture<Void> cancelledSubscribe = new CompletableFuture<>();
        daemon("subscribe-x", cancelledSubscribe, () -> catchup.subscribe("x", FILTER, cancelledStart.startAt(), cloudEvent -> Mono.empty())).start();
        cancelledStart.awaitInside();
        await(returnedWithin("cancel-x", () -> catchup.cancelSubscription("x").toFuture()), "The cancel of x did not complete");
        wrapped.failingSubscribes.add("x");
        Throwable laterFailure = failureOf(() -> catchup.subscribe("x", FILTER, StartAt.now(), cloudEvent -> Mono.empty()));
        wrapped.failingSubscribes.clear();
        AtomicReference<@Nullable Boolean> runningOnceTaken = new AtomicReference<>();
        wrapped.onRegister.put("x", () -> runningOnceTaken.set(catchup.isRunning("x")));

        cancelledStart.release();
        await(cancelledSubscribe, "The cancelled subscribe of x did not return");

        assertThat(laterFailure).as("what the subscribe of x made after the cancel threw").isInstanceOf(IllegalStateException.class);
        assertThat(runningOnceTaken.get()).as("whether the catch-up reports x running once the cancelled subscribe took it").isFalse();
        assertThat(wrapped.events).as("what the wrapped model was told").containsExactly("cancelled:x", "subscribed:x", "cancelled:x");
        assertThat(wrapped.isRunning("x") || wrapped.isPaused("x")).as("whether the wrapped model holds x").isFalse();
        assertThat(catchup.isRunning("x")).as("whether the catch-up reports x running").isFalse();
    }

    // Keeps the time the threads of a test take to load these classes out of what the deadline measures
    private static void loadClassesOfASubscribeAndACancel(ReactorStreamCatchupSubscriptionModel catchup) {
        catchup.subscribe("warm-up", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("warm-up").block(STARTUP);
    }

    private static CloudEvent anEvent(String id) {
        return OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build(), 1);
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

    // Replays nothing, and ends a read only once released, so a handover waits until then
    private static final class HeldReplay implements UnscopedPositionOrderedReader {
        private final CountDownLatch reading = new CountDownLatch(1);
        private final Sinks.Empty<Void> released = Sinks.empty();

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.defer(() -> {
                reading.countDown();
                return released.asMono().thenMany(Flux.empty());
            });
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(5L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }

        void awaitReading() {
            awaitLatch(reading, STARTUP, "The replay never read");
        }

        void release() {
            released.tryEmitEmpty().orThrow();
        }
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

    // A named subscription model that evaluates the start on the calling thread inside subscribe(..), against the class of
    // ReactorMongoSubscriptionModel, and holds the subscription once that returned. Only registersBeforeTheStart holds
    // it from before the evaluation, which is the order of ReactorMongoSubscriptionModel
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, IntrospectableSubscriptions {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> events = new CopyOnWriteArrayList<>();
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final Set<String> failingSubscribes = ConcurrentHashMap.newKeySet();
        // When on, a cancel completes only once completeCancel(..) is called for it, and removes the id only if it held it when asked
        volatile boolean holdsCancels;
        final List<Sinks.Empty<Void>> heldCancels = new CopyOnWriteArrayList<>();
        final AtomicInteger cancelCalls = new AtomicInteger();
        volatile int failingCancelsFrom = Integer.MAX_VALUE;
        // Run once at the start of the next cancelSubscription(..), before it can throw
        final AtomicReference<@Nullable Runnable> beforeNextCancel = new AtomicReference<>();
        // Run on the thread that cancels the id, as the cancel of a Mongo subscription runs its cancel hooks
        final Map<String, Runnable> onCancel = new ConcurrentHashMap<>();
        // Run on the subscribing thread once the id is registered, before the wrapped subscribe(..) returns
        final Map<String, Runnable> onRegister = new ConcurrentHashMap<>();
        // Holds the id from before it evaluates the start, as ReactorMongoSubscriptionModel does
        volatile boolean registersBeforeTheStart;
        // Refuses a subscribe of an id it holds, as ReactorMongoSubscriptionModel does
        volatile boolean refusesDuplicates;
        // isRunning(..) and isPaused(..) answer false while subscriptionIds() still lists the id, as they do when read while
        // ReactorMongoSubscriptionModel moves the id between its running and paused maps
        volatile boolean movingBetweenRunningAndPaused;
        // Every action each id was subscribed with, since a nested subscribe of an id adds one
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
            if (failingSubscribes.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped subscribe of " + subscriptionId + " failed");
            }
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
            if (refusesDuplicates && subscribed.contains(subscriptionId)) {
                events.add("refused:" + subscriptionId);
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            subscribed.add(subscriptionId);
            events.add("subscribed:" + subscriptionId);
            Runnable hook = onRegister.remove(subscriptionId);
            if (hook != null) {
                hook.run();
            }
        }

        int cancelsHeld() {
            return heldCancels.size();
        }

        void completeCancel(int index) {
            heldCancels.get(index).tryEmitEmpty().orThrow();
        }

        void failCancelsFromNowOn() {
            failingCancelsFrom = cancelCalls.get() + 1;
        }

        void succeedCancels() {
            failingCancelsFrom = Integer.MAX_VALUE;
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            Runnable before = beforeNextCancel.getAndSet(null);
            if (before != null) {
                before.run();
            }
            if (cancelCalls.incrementAndGet() >= failingCancelsFrom) {
                throw new IllegalStateException("The wrapped cancel of " + subscriptionId + " failed");
            }
            if (holdsCancels) {
                events.add("cancel-asked:" + subscriptionId);
                boolean heldWhenAsked = subscribed.contains(subscriptionId);
                Sinks.Empty<Void> completion = Sinks.empty();
                heldCancels.add(completion);
                return completion.asMono().doOnSuccess(done -> {
                    if (heldWhenAsked) {
                        subscribed.remove(subscriptionId);
                        paused.remove(subscriptionId);
                    }
                    events.add("cancelled:" + subscriptionId);
                });
            }
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
            return !movingBetweenRunningAndPaused && subscribed.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !movingBetweenRunningAndPaused && paused.contains(subscriptionId);
        }

        @Override
        public Set<String> subscriptionIds() {
            return Set.copyOf(subscribed);
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
