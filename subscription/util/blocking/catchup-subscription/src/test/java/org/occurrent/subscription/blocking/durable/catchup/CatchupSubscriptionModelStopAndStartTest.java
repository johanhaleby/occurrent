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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * What {@code start(..)} and {@code resumeSubscription(..)} do on a stopped blocking catch-up model, for both
 * {@link CatchupSubscriptionModel} and {@link StreamCatchupSubscriptionModel}, over a live model that holds what it
 * has not started and hands it the events published meanwhile once it starts.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelStopAndStartTest {

    private static final Duration STARTED_WITHIN = Duration.ofSeconds(5);

    static Stream<Named<BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel>>> models() {
        return Stream.of(
                Named.of("CatchupSubscriptionModel", (live, history) -> new CatchupSubscriptionModel(live, history)),
                Named.of("StreamCatchupSubscriptionModel", (live, history) -> new StreamCatchupSubscriptionModel(live, history, new CatchupSubscriptionModelConfig(1000))));
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_after_a_start_that_threw_starts_the_live_model_and_hands_the_replay_over(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        List<String> received = new CopyOnWriteArrayList<>();
        model.stop();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> received.add(e.getId()));
        live.failNextStart = FailMode.BEFORE_EFFECT;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);

        Subscription resumed = model.resumeSubscription("a");

        assertThat(resumed.waitUntilStarted(STARTED_WITHIN)).as("resumed subscription started").isTrue();
        assertThat(live.isRunning()).as("live model running").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(received).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_that_throws_on_a_running_model_leaves_it_running(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        List<String> received = new CopyOnWriteArrayList<>();
        live.failNextStart = FailMode.BEFORE_EFFECT;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);

        Subscription subscription = model.subscribe("a", StartAtTime.beginningOfTime(), e -> received.add(e.getId()));

        assertThat(subscription.waitUntilStarted(STARTED_WITHIN)).as("subscription started").isTrue();
        assertThat(received).containsExactly("1", "2");
    }

    @ParameterizedTest
    @MethodSource("models")
    void resuming_a_handed_over_subscription_on_a_stopped_model_starts_it_so_a_later_subscription_replays_at_once(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        Subscription a = model.subscribe("a", StartAtTime.beginningOfTime(), e -> {});
        assertThat(a.waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        model.resumeSubscription("a");
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started without a start or a resume of its own").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void resuming_a_handed_over_subscription_on_a_stopped_model_resumes_nothing_else(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        List<String> receivedByB = new CopyOnWriteArrayList<>();
        List<String> receivedByC = new CopyOnWriteArrayList<>();
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).isTrue();
        assertThat(model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId())).waitUntilStarted(STARTED_WITHIN)).isTrue();
        model.stop();
        model.subscribe("c", StartAtTime.beginningOfTime(), e -> receivedByC.add(e.getId()));

        model.resumeSubscription("a");
        live.publish(cloudEvent("3"));

        assertThat(model.isPaused("b")).as("b paused").isTrue();
        assertThat(model.isPaused("c")).as("c paused").isTrue();
        assertThat(receivedByB).containsExactly("1", "2");
        assertThat(receivedByC).isEmpty();
        model.resumeSubscription("b");
        // c replays the history, which this fake does not add event 3 to
        assertThat(model.resumeSubscription("c").waitUntilStarted(STARTED_WITHIN)).as("c started").isTrue();
        assertThat(receivedByB).containsExactly("1", "2", "3");
        assertThat(receivedByC).containsExactly("1", "2");
    }

    @ParameterizedTest
    @MethodSource("models")
    void resuming_an_unknown_subscription_on_a_stopped_model_leaves_it_stopped(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("unknown"));

        assertThat(thrown).isInstanceOf(UnknownSubscriptionException.class);
        assertThat(live.startCalls).isEmpty();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> {});
        assertThat(model.isPaused("a")).as("a made afterwards is paused").isTrue();
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_true_that_throws_after_starting_the_live_model_keeps_the_model_started(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        live.failNextStart = FailMode.AFTER_EFFECT;
        assertThat(catchThrowable(() -> model.start(true))).isInstanceOf(IllegalStateException.class);
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started on a model whose live model runs").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_false_that_throws_after_starting_the_live_model_keeps_the_model_started(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();
        live.failNextStart = FailMode.AFTER_EFFECT;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started on a model whose live model runs").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_that_throws_after_resuming_one_subscription_keeps_the_model_started(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        List<String> receivedByC = new CopyOnWriteArrayList<>();
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        assertThat(model.subscribe("c", StartAtTime.beginningOfTime(), e -> receivedByC.add(e.getId())).waitUntilStarted(STARTED_WITHIN)).as("c handed over").isTrue();
        model.stop();
        live.failNextStart = FailMode.AFTER_RESUMING_ONE;
        live.resumedBeforeFailing = "a";
        assertThat(catchThrowable(() -> model.start(true))).isInstanceOf(IllegalStateException.class);
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started on a model whose live model runs").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
        assertThat(model.isPaused("c")).as("c, which the failing start did not resume, paused").isTrue();
        assertThat(model.resumeSubscription("c").waitUntilStarted(STARTED_WITHIN)).as("c resumed").isTrue();
        assertThat(receivedByC).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_failing_start_keeps_the_model_started_when_a_start_true_returned_while_it_was_starting_the_live_model(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();
        Subscription a = model.subscribe("a", StartAtTime.beginningOfTime(), e -> {});
        CountDownLatch release = new CountDownLatch(1);
        live.blockNextStartUntil = release;
        live.failNextStart = FailMode.BEFORE_EFFECT;
        CompletableFuture<Throwable> failing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.start(false)));
        awaitWithin(live.startEntered);
        model.start(true);
        assertThat(a.waitUntilStarted(STARTED_WITHIN)).as("a started by the start(true)").isTrue();
        release.countDown();
        assertThat(failing.join()).isInstanceOf(IllegalStateException.class);
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started after a start(true) that returned").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_failing_start_keeps_the_model_started_when_a_resume_started_the_live_model_while_it_was_starting_it(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        CountDownLatch release = new CountDownLatch(1);
        live.blockNextStartUntil = release;
        live.failNextStart = FailMode.BEFORE_EFFECT;
        CompletableFuture<Throwable> failing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.start(false)));
        awaitWithin(live.startEntered);
        assertThat(model.resumeSubscription("a").waitUntilStarted(STARTED_WITHIN)).as("a resumed").isTrue();
        release.countDown();
        assertThat(failing.join()).isInstanceOf(IllegalStateException.class);
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started after a resume that started the live model").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_that_starts_the_live_model_after_a_failing_start_stopped_the_model_again_starts_the_model(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        CountDownLatch releaseResume = new CountDownLatch(1);
        live.blockNextResumeUntil = releaseResume;
        AtomicReference<CompletableFuture<Subscription>> resuming = new AtomicReference<>();
        failStartWhile(model, live, () -> {
            // The failing start already allows replays to run, so this resume goes to the live model without a start of its own
            resuming.set(CompletableFuture.supplyAsync(() -> model.resumeSubscription("a")));
            awaitWithin(live.resumeEntered);
        });
        releaseResume.countDown();
        assertThat(resuming.get().join().waitUntilStarted(STARTED_WITHIN)).as("a resumed").isTrue();
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started after a resume that started the live model").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_replay_that_a_resume_ran_again_while_a_failing_start_was_starting_the_live_model_waits_for_the_next_resume(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        GatedHistory history = history();
        SubscriptionModel model = modelOver.apply(live, history);
        model.stop();
        List<String> receivedByA = new CopyOnWriteArrayList<>();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> receivedByA.add(e.getId()));
        CountDownLatch gate = new CountDownLatch(1);
        history.gate = gate;
        failStartWhile(model, live, () -> {
            // The failing start already allows replays to run, so this resume runs a's replay without a start of its own
            model.resumeSubscription("a");
            awaitWithin(history.queried);
        });

        gate.countDown();

        await().atMost(STARTED_WITHIN).until(() -> model.isPaused("a"));
        assertThat(receivedByA).as("history replayed to a while the model is stopped").isEmpty();
        assertThat(model.resumeSubscription("a").waitUntilStarted(STARTED_WITHIN)).as("a resumed").isTrue();
        assertThat(live.startCalls).as("start calls on the live model").containsExactly(false, false);
        live.publish(cloudEvent("3"));
        assertThat(receivedByA).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_of_an_unknown_subscription_while_a_failing_start_was_starting_the_live_model_keeps_the_model_stopped(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();
        List<String> receivedByA = new CopyOnWriteArrayList<>();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> receivedByA.add(e.getId()));
        failStartWhile(model, live, () -> assertThat(catchThrowable(() -> model.resumeSubscription("unknown"))).isInstanceOf(UnknownSubscriptionException.class));

        Subscription resumed = model.resumeSubscription("a");

        assertThat(resumed.waitUntilStarted(STARTED_WITHIN)).as("a started by its resume").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByA).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_of_a_running_subscription_while_a_failing_start_was_starting_the_live_model_keeps_the_model_stopped(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        assertThat(model.subscribe("running", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("running handed over").isTrue();
        model.stop();
        // The live model is stopped but says the subscription named running still runs, so resuming it throws
        live.subscriptions.get("running").started = true;
        List<String> receivedByA = new CopyOnWriteArrayList<>();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> receivedByA.add(e.getId()));
        failStartWhile(model, live, () -> assertThat(catchThrowable(() -> model.resumeSubscription("running"))).isInstanceOf(SubscriptionAlreadyRunningException.class));

        Subscription resumed = model.resumeSubscription("a");

        assertThat(resumed.waitUntilStarted(STARTED_WITHIN)).as("a started by its resume").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByA).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_subscription_made_after_a_failing_start_that_a_resume_of_an_unknown_subscription_overlapped_stays_paused(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();
        failStartWhile(model, live, () -> assertThat(catchThrowable(() -> model.resumeSubscription("unknown"))).isInstanceOf(UnknownSubscriptionException.class));
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(model.isPaused("b")).as("b paused").isTrue();
        assertThat(b.waitUntilStarted(Duration.ofMillis(500))).as("b started").isFalse();
        assertThat(receivedByB).as("history replayed to b").isEmpty();
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_stop_while_a_failing_start_was_starting_the_live_model_keeps_the_model_stopped(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        model.stop();
        List<String> receivedByA = new CopyOnWriteArrayList<>();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> receivedByA.add(e.getId()));
        failStartWhile(model, live, model::stop);

        model.subscribe("b", StartAtTime.beginningOfTime(), e -> {});

        assertThat(model.isPaused("b")).as("b paused").isTrue();
        assertThat(model.resumeSubscription("a").waitUntilStarted(STARTED_WITHIN)).as("a resumed").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByA).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_failing_start_and_a_resume_whose_action_stops_the_model_both_return_when_the_live_model_answers_isRunning_under_the_lock_it_delivers_with(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) throws InterruptedException {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        AtomicReference<Thread> failingStart = new AtomicReference<>();
        CountDownLatch delivering = new CountDownLatch(1);
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {
            if (e.getId().equals("3")) {
                delivering.countDown();
                awaitBlocked(failingStart.get());
                model.stop();
            }
        }).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        live.publish(cloudEvent("3"));
        live.isRunningTakesTheMonitor = true;
        CountDownLatch release = new CountDownLatch(1);
        live.blockNextStartUntil = release;
        live.failNextStart = FailMode.BEFORE_EFFECT;
        Thread failing = Thread.ofPlatform().daemon().unstarted(() -> catchThrowable(() -> model.start(false)));
        failingStart.set(failing);
        failing.start();
        awaitWithin(live.startEntered);
        // The failing start already allows replays to run, so this resume goes to the live model, which hands it 3
        // while it holds its monitor
        Thread resuming = Thread.ofPlatform().daemon().start(() -> catchThrowable(() -> model.resumeSubscription("a")));
        awaitWithin(delivering);

        release.countDown();

        failing.join(STARTED_WITHIN.toMillis());
        resuming.join(STARTED_WITHIN.toMillis());
        assertThat(failing.isAlive()).as("failing start still waiting").isFalse();
        assertThat(resuming.isAlive()).as("resume still waiting").isFalse();
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_after_a_start_that_threw_starts_the_model_so_a_later_subscription_replays_at_once(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        live.failNextStart = FailMode.BEFORE_EFFECT;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);
        model.resumeSubscription("a");
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started after a resume").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @Test
    void a_resume_at_a_position_after_a_start_that_threw_starts_the_model_so_a_later_subscription_replays_at_once() {
        FakeLiveModel live = new FakeLiveModel();
        CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        live.failNextStart = FailMode.BEFORE_EFFECT;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);
        model.resumeSubscription("a", StartAt.now());
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(b.waitUntilStarted(STARTED_WITHIN)).as("b started after a resume at a position that started the live model").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByB).containsExactly("1", "2", "3");
    }

    @Test
    void a_subscription_made_after_a_stop_and_a_resume_at_a_position_stays_paused() {
        FakeLiveModel live = new FakeLiveModel();
        CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history());
        assertThat(model.subscribe("a", StartAtTime.beginningOfTime(), e -> {}).waitUntilStarted(STARTED_WITHIN)).as("a handed over").isTrue();
        model.stop();
        model.resumeSubscription("a", StartAt.now());
        List<String> receivedByB = new CopyOnWriteArrayList<>();

        Subscription b = model.subscribe("b", StartAtTime.beginningOfTime(), e -> receivedByB.add(e.getId()));

        assertThat(model.isPaused("b")).as("b paused").isTrue();
        assertThat(b.waitUntilStarted(Duration.ofMillis(500))).as("b started").isFalse();
        assertThat(receivedByB).as("history replayed to b").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void a_replay_in_flight_hands_over_when_a_start_returns_before_a_failing_start_parks_it(boolean resumeSubscriptionsAutomatically) throws InterruptedException {
        FakeLiveModel live = new FakeLiveModel();
        GatedHistory history = history();
        ParkWaitingStreamCatchupSubscriptionModel model = new ParkWaitingStreamCatchupSubscriptionModel(live, history);
        model.stop();
        CountDownLatch releaseFailingStart = new CountDownLatch(1);
        live.blockNextStartUntil = releaseFailingStart;
        live.failNextStart = FailMode.BEFORE_EFFECT;
        Thread failing = Thread.ofPlatform().daemon().unstarted(() -> catchThrowable(() -> model.start(false)));
        model.waitingThread = failing;
        failing.start();
        awaitWithin(live.startEntered);
        // The failing start already allows replays to run, so a's replay runs and waits in the history read
        CountDownLatch gate = new CountDownLatch(1);
        history.gate = gate;
        List<String> receivedByA = new CopyOnWriteArrayList<>();
        Subscription a = model.subscribe("a", StartAtTime.beginningOfTime(), e -> receivedByA.add(e.getId()));
        awaitWithin(history.queried);
        releaseFailingStart.countDown();
        awaitWithin(model.aboutToPark);

        model.start(resumeSubscriptionsAutomatically);
        model.park.countDown();
        failing.join(STARTED_WITHIN.toMillis());
        gate.countDown();

        assertThat(a.waitUntilStarted(STARTED_WITHIN)).as("a started").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(receivedByA).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_that_throws_while_the_live_model_cannot_tell_whether_it_runs_throws_its_own_failure_and_keeps_the_model_stopped(BiFunction<FakeLiveModel, EventStoreQueries, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        GatedHistory history = history();
        SubscriptionModel model = modelOver.apply(live, history);
        model.stop();
        live.failNextStart = FailMode.BEFORE_EFFECT;
        live.isRunningThrows = true;

        Throwable thrown = catchThrowable(() -> model.start(false));

        live.isRunningThrows = false;
        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("start failed before taking effect");
        assertThat(thrown.getSuppressed()).singleElement().isInstanceOf(IllegalArgumentException.class);
        // A replay that runs waits in the history read, so a reads as not paused until it is parked
        history.gate = new CountDownLatch(1);
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> {});
        assertThat(model.isPaused("a")).as("a paused").isTrue();
        history.gate.countDown();
    }

    @Test
    void a_start_that_throws_while_the_live_model_cannot_tell_whether_it_runs_keeps_every_catch_up_model_of_the_composite_stopped() {
        FakeLiveModel live = new FakeLiveModel();
        GatedHistory history = history();
        CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
        model.stop();
        live.failNextStart = FailMode.BEFORE_EFFECT;
        live.isRunningThrows = true;
        assertThat(catchThrowable(() -> model.start(false))).as("start failure").isNotNull();
        live.isRunningThrows = false;
        history.gate = new CountDownLatch(1);

        model.subscribe("stream", StartAtTime.beginningOfTime(), e -> {});
        model.subscribe("agnostic", AgnosticSubscriptionFilter.filter(Filter.all()), StartAtTime.beginningOfTime(), e -> {});

        assertThat(model.isPaused("stream")).as("stream paused").isTrue();
        assertThat(model.isPaused("agnostic")).as("agnostic paused").isTrue();
        history.gate.countDown();
    }

    // Waits up to the given time for thread to block on a monitor, and returns either way
    private static void awaitBlocked(Thread thread) {
        long until = System.nanoTime() + STARTED_WITHIN.toNanos();
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }

    // Holds the first handover lock that waitingThread asks for until park counts down, which for a failing start is
    // the lock it parks a replay in flight under
    private static final class ParkWaitingStreamCatchupSubscriptionModel extends StreamCatchupSubscriptionModel {
        volatile @Nullable Thread waitingThread;
        final CountDownLatch aboutToPark = new CountDownLatch(1);
        final CountDownLatch park = new CountDownLatch(1);

        private ParkWaitingStreamCatchupSubscriptionModel(FakeLiveModel live, EventStoreQueries history) {
            super(live, history, new CatchupSubscriptionModelConfig(1000));
        }

        @Override
        protected HandoverLock lockHandover(String subscriptionId) {
            if (Thread.currentThread() == waitingThread) {
                waitingThread = null;
                aboutToPark.countDown();
                awaitWithin(park);
            }
            return super.lockHandover(subscriptionId);
        }
    }

    // Runs start(false) on another thread with a live model whose start fails before it takes effect, and runs
    // whileStarting after that start has begun and before it throws
    private static void failStartWhile(SubscriptionModel model, FakeLiveModel live, Runnable whileStarting) {
        CountDownLatch release = new CountDownLatch(1);
        live.blockNextStartUntil = release;
        live.failNextStart = FailMode.BEFORE_EFFECT;
        CompletableFuture<Throwable> failing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.start(false)));
        awaitWithin(live.startEntered);
        try {
            whileStarting.run();
        } finally {
            release.countDown();
        }
        assertThat(failing.join()).as("the failing start threw").isInstanceOf(IllegalStateException.class);
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static GatedHistory history() {
        return new GatedHistory(List.of(cloudEvent("1"), cloudEvent("2")));
    }

    private static void awaitWithin(CountDownLatch latch) {
        try {
            assertThat(latch.await(STARTED_WITHIN.toMillis(), TimeUnit.MILLISECONDS)).as("latch counted down in time").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // A history read waits until the gate opens, which it is unless a test closes it, and counts down queried first
    private static final class GatedHistory implements EventStoreQueries {
        private final List<CloudEvent> events;
        volatile CountDownLatch gate = new CountDownLatch(0);
        final CountDownLatch queried = new CountDownLatch(1);

        private GatedHistory(List<CloudEvent> events) {
            this.events = events;
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            queried.countDown();
            awaitWithin(gate);
            return events.stream().skip(skip).limit(limit);
        }

        @Override
        public long count(Filter filter) {
            return events.size();
        }

        @Override
        public boolean exists(Filter filter) {
            return !events.isEmpty();
        }
    }

    enum FailMode {NONE, BEFORE_EFFECT, AFTER_EFFECT, AFTER_RESUMING_ONE}

    // Behaves as a change-stream model does. A subscription made while stopped is held, a resume starts the model and
    // that one subscription, start(true) starts every held subscription, start(false) none, and a held subscription
    // gets the events published meanwhile once it starts. A start can fail before it takes effect, or after it, as
    // ChangeStreamSubscriptions.start(..) marks the model running before a resume in it throws.
    static final class FakeLiveModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private volatile boolean running = true;
        // isRunning() takes the monitor that publish(..), stop() and a resume hold while they deliver
        volatile boolean isRunningTakesTheMonitor = false;
        volatile boolean isRunningThrows = false;
        volatile FailMode failNextStart = FailMode.NONE;
        // With FailMode.AFTER_RESUMING_ONE, the one subscription the failing start resumes
        volatile @Nullable String resumedBeforeFailing = null;
        // A start that finds this set waits for it before doing anything else, and counts down startEntered
        volatile @Nullable CountDownLatch blockNextStartUntil = null;
        final CountDownLatch startEntered = new CountDownLatch(1);
        // The same for a resume, which counts down resumeEntered
        volatile @Nullable CountDownLatch blockNextResumeUntil = null;
        final CountDownLatch resumeEntered = new CountDownLatch(1);
        final List<Boolean> startCalls = new CopyOnWriteArrayList<>();
        private final Map<String, FakeSubscription> subscriptions = new ConcurrentHashMap<>();

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return TimeBasedCheckpoint.beginningOfTime();
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            FakeSubscription subscription = new FakeSubscription(subscriptionId, action);
            subscription.started = running;
            subscriptions.put(subscriptionId, subscription);
            return subscription;
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            FakeSubscription subscription = new FakeSubscription(subscriptionId, action);
            subscriptions.put(subscriptionId, subscription);
            return subscription;
        }

        synchronized void publish(CloudEvent event) {
            subscriptions.values().forEach(subscription -> subscription.receive(event));
        }

        @Override
        public synchronized void stop() {
            running = false;
            subscriptions.values().forEach(subscription -> subscription.started = false);
        }

        // Not synchronized as a whole, so a start blocked by blockNextStartUntil does not hold up another start
        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            startCalls.add(resumeSubscriptionsAutomatically);
            FailMode failMode = failNextStart;
            failNextStart = FailMode.NONE;
            CountDownLatch block = blockNextStartUntil;
            if (block != null) {
                blockNextStartUntil = null;
                startEntered.countDown();
                awaitWithin(block);
            }
            if (failMode == FailMode.BEFORE_EFFECT) {
                throw new IllegalStateException("start failed before taking effect");
            }
            synchronized (this) {
                running = true;
                if (failMode == FailMode.AFTER_RESUMING_ONE) {
                    subscriptions.get(resumedBeforeFailing).go();
                } else if (resumeSubscriptionsAutomatically) {
                    subscriptions.values().forEach(FakeSubscription::go);
                }
            }
            if (failMode != FailMode.NONE) {
                throw new IllegalStateException("start failed after taking effect");
            }
        }

        @Override
        public boolean isRunning() {
            if (isRunningThrows) {
                throw new IllegalArgumentException("isRunning() failed");
            }
            if (isRunningTakesTheMonitor) {
                synchronized (this) {
                    return running;
                }
            }
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            FakeSubscription subscription = subscriptions.get(subscriptionId);
            return subscription != null && subscription.started;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            FakeSubscription subscription = subscriptions.get(subscriptionId);
            return subscription != null && !subscription.started;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            CountDownLatch block = blockNextResumeUntil;
            if (block != null) {
                blockNextResumeUntil = null;
                resumeEntered.countDown();
                awaitWithin(block);
            }
            synchronized (this) {
                FakeSubscription subscription = subscriptions.get(subscriptionId);
                if (subscription == null) {
                    throw new UnknownSubscriptionException(subscriptionId);
                }
                if (subscription.started) {
                    throw new SubscriptionAlreadyRunningException(subscriptionId);
                }
                running = true;
                subscription.go();
                return subscription;
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            FakeSubscription subscription = subscriptions.get(subscriptionId);
            if (subscription != null) {
                subscription.started = false;
            }
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }
    }

    private static final class FakeSubscription implements Subscription {
        private final String id;
        private final Consumer<CloudEvent> action;
        private final List<CloudEvent> held = new ArrayList<>();
        private volatile boolean started = false;

        private FakeSubscription(String id, Consumer<CloudEvent> action) {
            this.id = id;
            this.action = action;
        }

        private void receive(CloudEvent event) {
            if (started) {
                action.accept(event);
            } else {
                held.add(event);
            }
        }

        private void go() {
            if (!started) {
                started = true;
                held.forEach(action);
                held.clear();
            }
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            try {
                await().atMost(timeout).until(() -> started);
                return true;
            } catch (org.awaitility.core.ConditionTimeoutException e) {
                return false;
            }
        }
    }
}
