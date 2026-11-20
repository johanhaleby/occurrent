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
import io.cloudevents.core.builder.CloudEventBuilder;
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
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A competing consumer model over a catch-up model, stopped from inside the action of a subscription that replays. The
 * stop is a call from a replay thread of the catch-up model, so it takes effect once the action has returned instead
 * of throwing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStopFromAReplayActionOverCatchupTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration EVENTUALLY = Duration.ofSeconds(10);

    private InMemorySubscriptionModel inMemory;
    private StreamCatchupSubscriptionModel catchup;
    private CompetingConsumerSubscriptionModel competing;

    @BeforeEach
    void create_a_competing_consumer_model_over_a_catchup_model_over_an_in_memory_model_with_some_history() {
        inMemory = new InMemorySubscriptionModel();
        catchup = new StreamCatchupSubscriptionModel(new LiveModel(inMemory), new OneEventOfHistory(), new CatchupSubscriptionModelConfig(100));
        competing = new CompetingConsumerSubscriptionModel(catchup, new GrantingStrategy());
    }

    @AfterEach
    void shutdown() {
        try {
            competing.shutdown();
        } finally {
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_stop_of_the_competing_consumer_model_from_the_action_of_a_replay_throws_nothing_and_stops_both_models_once_the_action_has_returned() throws InterruptedException {
        // Given
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        CountDownLatch actionReturned = new CountDownLatch(1);

        // When
        competing.subscribe(NODE, ID, null, StartAtTime.beginningOfTime(), event -> {
            try {
                competing.stop();
            } catch (Throwable e) {
                thrown.set(e);
            } finally {
                actionReturned.countDown();
            }
        });

        // Then
        assertThat(actionReturned.await(20, TimeUnit.SECONDS)).as("the action of the replay ran and returned").isTrue();
        assertThat(thrown.get()).as("what stop() on the competing consumer model threw when called from the action of a replay").isNull();
        await().atMost(EVENTUALLY).untilAsserted(() -> {
            assertThat(catchup.isRunning()).as("whether the catch-up model runs once the action has returned").isFalse();
            assertThat(competing.isRunning()).as("whether the competing consumer model runs once the action has returned").isFalse();
        });
    }

    // The history a replay reads, one event, whichever way it asks for it
    private static final class OneEventOfHistory implements EventStoreQueries, CatchupReads {
        private final List<CloudEvent> history = List.of(CloudEventBuilder.v1().withId("history").withSource(URI.create("urn:test")).withType("test.event")
                .withTime(OffsetDateTime.parse("2026-01-01T00:00:00Z")).build());

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
                    return OneEventOfHistory.this.query(filter, skip, limit, sortBy);
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return history.stream().skip(skip).limit(limit);
        }

        @Override
        public long count(Filter filter) {
            return history.size();
        }

        @Override
        public boolean exists(Filter filter) {
            return !history.isEmpty();
        }
    }

    private static final class GrantingStrategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
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

    // A live model that takes any position the catch-up model hands it and starts from now
    private static final class LiveModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;

        private LiveModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
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
            return true;
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

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
