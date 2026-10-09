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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
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

    static Stream<Named<Function<FakeLiveModel, SubscriptionModel>>> models() {
        return Stream.of(
                Named.of("CatchupSubscriptionModel", live -> new CatchupSubscriptionModel(live, history())),
                Named.of("StreamCatchupSubscriptionModel", live -> new StreamCatchupSubscriptionModel(live, history(), new CatchupSubscriptionModelConfig(1000))));
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_resume_after_a_start_that_threw_starts_the_live_model_and_hands_the_replay_over(Function<FakeLiveModel, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live);
        List<String> received = new CopyOnWriteArrayList<>();
        model.stop();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> received.add(e.getId()));
        live.failNextStart = true;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);

        Subscription resumed = model.resumeSubscription("a");

        assertThat(resumed.waitUntilStarted(STARTED_WITHIN)).as("resumed subscription started").isTrue();
        assertThat(live.isRunning()).as("live model running").isTrue();
        live.publish(cloudEvent("3"));
        assertThat(received).containsExactly("1", "2", "3");
    }

    @ParameterizedTest
    @MethodSource("models")
    void a_start_that_throws_on_a_running_model_leaves_it_running(Function<FakeLiveModel, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live);
        List<String> received = new CopyOnWriteArrayList<>();
        live.failNextStart = true;
        assertThat(catchThrowable(() -> model.start(false))).isInstanceOf(IllegalStateException.class);

        Subscription subscription = model.subscribe("a", StartAtTime.beginningOfTime(), e -> received.add(e.getId()));

        assertThat(subscription.waitUntilStarted(STARTED_WITHIN)).as("subscription started").isTrue();
        assertThat(received).containsExactly("1", "2");
    }

    @ParameterizedTest
    @MethodSource("models")
    void resuming_a_handed_over_subscription_on_a_stopped_model_starts_it_so_a_later_subscription_replays_at_once(Function<FakeLiveModel, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live);
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
    void resuming_a_handed_over_subscription_on_a_stopped_model_resumes_nothing_else(Function<FakeLiveModel, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live);
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
    void resuming_an_unknown_subscription_on_a_stopped_model_leaves_it_stopped(Function<FakeLiveModel, SubscriptionModel> modelOver) {
        FakeLiveModel live = new FakeLiveModel();
        SubscriptionModel model = modelOver.apply(live);
        model.stop();

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("unknown"));

        assertThat(thrown).isInstanceOf(UnknownSubscriptionException.class);
        assertThat(live.startCalls).isEmpty();
        model.subscribe("a", StartAtTime.beginningOfTime(), e -> {});
        assertThat(model.isPaused("a")).as("a made afterwards is paused").isTrue();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static EventStoreQueries history() {
        return new HistoryOf(List.of(cloudEvent("1"), cloudEvent("2")));
    }

    private record HistoryOf(List<CloudEvent> events) implements EventStoreQueries {
        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
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

    // Behaves as a change-stream model does. A subscription made while stopped is held, a resume starts the model and
    // that one subscription, start(true) starts every held subscription, start(false) none, and a held subscription
    // gets the events published meanwhile once it starts.
    static final class FakeLiveModel implements CheckpointAwareSubscriptionModel {
        private volatile boolean running = true;
        volatile boolean failNextStart = false;
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

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            startCalls.add(resumeSubscriptionsAutomatically);
            if (failNextStart) {
                failNextStart = false;
                throw new IllegalStateException("start failed before taking effect");
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                subscriptions.values().forEach(FakeSubscription::go);
            }
        }

        @Override
        public boolean isRunning() {
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
        public synchronized Subscription resumeSubscription(String subscriptionId) {
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
