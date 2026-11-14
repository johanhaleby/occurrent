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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The catch-up model a stream event store is wired with holds a replay from the beginning paused when the live model
 * has lost the live start it would go live from, and tells its start refusal listeners. It answers whether it still
 * holds the replay for the refusal it told them of, and stops telling a listener once it is removed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupSubscriptionModelHeldPausedReportTest {

    private static final String ID = "subscription";

    private InMemorySubscriptionModel inMemory;
    private LiveModelThatLosesEveryLiveStart liveModel;
    private CatchupSubscriptionModel catchup;

    @BeforeEach
    void a_catchup_model_over_a_live_model_that_loses_every_live_start() {
        inMemory = new InMemorySubscriptionModel();
        liveModel = new LiveModelThatLosesEveryLiveStart(inMemory);
        catchup = new CatchupSubscriptionModel(liveModel, new NoHistory(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        inMemory.shutdown();
    }

    @Test
    void is_held_paused_for_the_refusal_it_reported_while_the_replay_is_held_for_a_lost_live_start() {
        // Given
        CatchupCannotResumeException reported = heldPausedAndReported();

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, reported);

        // Then
        assertThat(heldPaused).as("held paused for the refusal reported").isTrue();
    }

    @Test
    void is_not_held_paused_for_a_refusal_other_than_the_one_it_reported() {
        // Given
        heldPausedAndReported();

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, HistoryLossMessages.liveStartLostOnResume(ID, new StringBasedCheckpoint("live start read now")));

        // Then
        assertThat(heldPaused).as("held paused for another refusal").isFalse();
    }

    @Test
    void is_no_longer_held_paused_for_the_refusal_it_reported_once_the_subscription_is_cancelled() {
        // Given
        CatchupCannotResumeException reported = heldPausedAndReported();
        catchup.cancelSubscription(ID);

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, reported);

        // Then
        assertThat(heldPaused).as("held paused for the refusal reported").isFalse();
    }

    @Test
    void a_removed_start_refusal_listener_is_not_told_that_a_replay_is_held_for_a_lost_live_start() {
        // Given
        HeldPausedReports removed = new HeldPausedReports();
        HeldPausedReports kept = new HeldPausedReports();
        catchup.addStartRefusalListener(removed);
        catchup.addStartRefusalListener(kept);
        catchup.removeStartRefusalListener(removed);

        // When
        subscribeFromTheBeginning();

        // Then
        await().untilAsserted(() -> assertThat(kept.heldPaused).as("held paused reports to the listener kept").hasSize(1));
        assertThat(removed.heldPaused).as("held paused reports to the listener removed").isEmpty();
    }

    @Test
    void removing_a_start_refusal_listener_removes_it_from_the_live_model_too() {
        // Given
        HeldPausedReports listener = new HeldPausedReports();
        catchup.addStartRefusalListener(listener);
        assertThat(liveModel.listeners).as("listeners the live model was given").containsExactly(listener);

        // When
        catchup.removeStartRefusalListener(listener);

        // Then
        assertThat(liveModel.listeners).as("listeners the live model keeps").isEmpty();
    }

    private CatchupCannotResumeException heldPausedAndReported() {
        HeldPausedReports reports = new HeldPausedReports();
        catchup.addStartRefusalListener(reports);
        subscribeFromTheBeginning();
        await().untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").hasSize(1));
        assertThat(catchup.isPaused(ID)).as("held paused").isTrue();
        return reports.heldPaused.get(0);
    }

    private void subscribeFromTheBeginning() {
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), __ -> {
        });
    }

    // Records each refusal it is told of a subscription held paused, and refuses to be told of any other refusal
    private static final class HeldPausedReports implements StartRefusalListener {
        private final List<CatchupCannotResumeException> heldPaused = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            throw new AssertionError("A catch-up subscription model was not expected to refuse the start of " + subscriptionId, refusal);
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            heldPaused.add(refusal);
        }
    }

    // A live model that cannot resume from any live start the catch-up model hands it, as one whose history is gone, and
    // that keeps the start refusal listeners it is given
    private static final class LiveModelThatLosesEveryLiveStart implements CheckpointAwareSubscriptionModel, StartRefusalReportingSubscriptions {
        private final InMemorySubscriptionModel delegate;
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();

        private LiveModelThatLosesEveryLiveStart(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("live start read now");
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return false;
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
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
    }

    private static final class NoHistory implements EventStoreQueries, CatchupReads {
        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            return new CatchupReadScope() {
                @Override
                public @Nullable String liveStart() {
                    return null;
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                    return Stream.empty();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return Stream.empty();
        }

        @Override
        public long count(Filter filter) {
            return 0;
        }

        @Override
        public boolean exists(Filter filter) {
            return false;
        }
    }
}
