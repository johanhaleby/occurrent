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

package org.occurrent.subscription.blocking.competingconsumers;

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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A catch-up model answers whether it reports start refusals as the live model it wraps does, and a competing consumer
 * model over a catch-up whose live model reports none doesn't wait for a report that never comes. The replay hands over
 * to a live model that refuses the live subscribe with {@link UnsupportedStartAtException}, which the handle of the
 * catch-up throws from {@code waitUntilStarted}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerOverACatchupThatReportsNoRefusalsTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration LONG_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration SHORT_BOUND = Duration.ofSeconds(5);

    private CompetingConsumerSubscriptionModel competing;

    @BeforeEach
    void no_competing_consumer_model_yet() {
        competing = null;
    }

    @AfterEach
    void shutdown() {
        if (competing != null) {
            competing.shutdown();
        }
    }

    @Test
    void throws_the_refusal_from_waitUntilStarted_without_waiting_for_the_timeout_when_the_catchup_model_wraps_a_live_model_that_reports_no_refusals() {
        // Given a replay whose handover to the live model is refused by the live subscribe
        Strategy strategy = new Strategy();
        strategy.grantOnRegister = true;
        CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(new LiveModelThatRefusesTheLiveSubscribe(), new NoHistory(), new CatchupSubscriptionModelConfig(100));
        competing = new CompetingConsumerSubscriptionModel(catchup, strategy);
        Subscription subscription = competing.subscribe(NODE, ID, null, StartAt.dynamic(context -> context.hasSubscriptionModelType(CatchupSubscriptionModel.class) ? StartAtTime.beginningOfTime() : StartAt.now()), __ -> {
        });

        // When
        Throwable thrown = assertTimeoutPreemptively(SHORT_BOUND, () -> catchThrowable(() -> subscription.waitUntilStarted(LONG_TIMEOUT)));

        // Then
        assertThat(thrown).as("thrown by waitUntilStarted").isInstanceOf(UnsupportedStartAtException.class);
    }

    @Test
    void a_catchup_model_reports_no_start_refusals_when_its_live_model_does_not_implement_the_capability() {
        // Given
        CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(new LiveModelThatRefusesTheLiveSubscribe(), new NoHistory(), new CatchupSubscriptionModelConfig(100));

        // When
        boolean reports = catchup.reportsStartRefusals();

        // Then
        assertThat(reports).isFalse();
    }

    @Test
    void a_catchup_model_reports_start_refusals_when_its_live_model_does() {
        // Given
        CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(new ReportingLiveModel(), new NoHistory(), new CatchupSubscriptionModelConfig(100));

        // When
        boolean reports = catchup.reportsStartRefusals();

        // Then
        assertThat(reports).isTrue();
    }

    @Test
    void a_stream_catchup_model_reports_no_start_refusals_when_its_live_model_does_not_implement_the_capability() {
        // Given
        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(new LiveModelThatRefusesTheLiveSubscribe(), new NoHistory(), new CatchupSubscriptionModelConfig(100));

        // When
        boolean reports = catchup.reportsStartRefusals();

        // Then
        assertThat(reports).isFalse();
    }

    @Test
    void a_stream_catchup_model_reports_start_refusals_when_its_live_model_does() {
        // Given
        StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(new ReportingLiveModel(), new NoHistory(), new CatchupSubscriptionModelConfig(100));

        // When
        boolean reports = catchup.reportsStartRefusals();

        // Then
        assertThat(reports).isTrue();
    }

    // A live model without the start refusal capability, whose live subscribe refuses the start the catch-up hands over
    private static class LiveModelThatRefusesTheLiveSubscribe implements CheckpointAwareSubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new UnsupportedStartAtException(startAt, "The live model refuses the start of " + subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("live start read now");
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }

    private static final class ReportingLiveModel extends LiveModelThatRefusesTheLiveSubscribe implements StartRefusalReportingSubscriptions {
        private final CopyOnWriteArrayList<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
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

    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private volatile boolean grantOnRegister;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }
}
