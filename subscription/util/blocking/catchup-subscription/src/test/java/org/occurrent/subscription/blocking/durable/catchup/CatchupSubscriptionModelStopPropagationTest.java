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
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * No-Mongo regression guard: {@link CatchupSubscriptionModel#stop()}/{@code start(boolean)}, {@code isRunning} and
 * {@code isCatchingUp} must reach the catch-up child running the in-flight replay, not just the shared live delegate.
 * Uses a real in-memory event store and a permissive fake delegate, since these tests need an actual replay to
 * observe mid-flight.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelStopPropagationTest {

    @Test
    void stop_interrupts_a_catchup_replay_that_is_started_after_stop_was_called() {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"), cloudEvent("2"), cloudEvent("3"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        catchupSubscriptionModel.stop();

        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        Subscription subscription = catchupSubscriptionModel.subscribe("someId", StartAtTime.beginningOfTime(), received::add);
        // The replay never ran a single iteration (stopped was already true), so this hands back a
        // CancelledSubscription rather than a live one, and that answers false, since nothing here is going to start it.
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isFalse();

        assertThat(received).isEmpty();
    }

    @Test
    void a_replay_started_after_start_following_stop_delivers_events_again() {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"), cloudEvent("2"), cloudEvent("3"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        catchupSubscriptionModel.stop();
        catchupSubscriptionModel.start(false);

        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        Subscription subscription = catchupSubscriptionModel.subscribe("someId", StartAtTime.beginningOfTime(), received::add);
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        assertThat(received).extracting(CloudEvent::getId).containsExactly("1", "2", "3");
    }

    @Test
    void isRunning_reports_true_while_a_catchup_replay_is_in_flight() throws InterruptedException {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        String subscriptionId = "someId";
        CountDownLatch firstEventReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        Subscription subscription = catchupSubscriptionModel.subscribe(subscriptionId, StartAtTime.beginningOfTime(), event -> {
            firstEventReached.countDown();
            awaitLatch(releaseReplay);
        });

        assertThat(firstEventReached.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(catchupSubscriptionModel.isRunning(subscriptionId)).isTrue();
            assertThat(catchupSubscriptionModel.isRunning()).isTrue();
        } finally {
            releaseReplay.countDown();
        }
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
    }

    /**
     * The reason {@code isCatchingUp} exists as a signal distinct from {@code isRunning}: a caller that gates on
     * liveness needs to know when the replay has actually handed over, since {@code isRunning(id)} is true for the
     * entire replay and cannot answer that on its own. On the dispatcher this also proves the answer comes from
     * whichever inner catch-up model owns the id, not from the shared delegate (see the class javadoc on
     * {@link CatchupSubscriptionModel#isCatchingUp(String)}: it deliberately never asks the delegate).
     */
    @Test
    void isCatchingUp_reports_true_while_the_owning_inner_models_replay_is_in_flight_and_false_once_it_hands_over() throws InterruptedException {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        String subscriptionId = "someId";
        CountDownLatch firstEventReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        Subscription subscription = catchupSubscriptionModel.subscribe(subscriptionId, StartAtTime.beginningOfTime(), event -> {
            firstEventReached.countDown();
            awaitLatch(releaseReplay);
        });

        assertThat(firstEventReached.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(catchupSubscriptionModel.isCatchingUp(subscriptionId)).isTrue();
        } finally {
            releaseReplay.countDown();
        }
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();

        assertThat(catchupSubscriptionModel.isCatchingUp(subscriptionId)).isFalse();
    }

    @Test
    void an_id_the_model_has_never_seen_is_not_catching_up() {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        assertThat(catchupSubscriptionModel.isCatchingUp("never-subscribed")).isFalse();
    }

    @Test
    void is_catching_up_rejects_a_null_subscription_id() {
        InMemoryEventStoreQueries events = new InMemoryEventStoreQueries(cloudEvent("1"));
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(new PermissiveCheckpointAwareSubscriptionModel(), events);

        Throwable thrown = catchThrowable(() -> catchupSubscriptionModel.isCatchingUp(null));

        assertThat(thrown).isInstanceOf(NullPointerException.class);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("test.event")
                .withTime(OffsetDateTime.now())
                .build();
    }

    // Ignores filter and sort order (neither is exercised by these tests) and simply serves the fixed event list, so
    // a real (non-Mongo) catch-up replay has something to read.
    private static final class InMemoryEventStoreQueries implements EventStoreQueries {
        private final List<CloudEvent> events;

        private InMemoryEventStoreQueries(CloudEvent... events) {
            this.events = List.of(events);
        }

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

    // A permissive live delegate (unlike CatchupSubscriptionModelDualModeLifecycleTest's throwing counting fake)
    // since these tests need a real catch-up replay to run to completion and hand over to it.
    private static final class PermissiveCheckpointAwareSubscriptionModel implements CheckpointAwareSubscriptionModel {

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return TimeBasedCheckpoint.beginningOfTime();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new NoOpSubscription(subscriptionId);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return new NoOpSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }

        @Override
        public void shutdown() {
        }
    }

    private record NoOpSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
