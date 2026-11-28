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
import org.awaitility.core.ConditionTimeoutException;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

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
    private static final Duration HOLD_THE_ACTION_OPEN = Duration.ofMillis(1500);

    private InMemorySubscriptionModel inMemory;
    private StreamCatchupSubscriptionModel catchup;
    private CompetingConsumerSubscriptionModel competing;
    private final List<Node> nodes = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_a_competing_consumer_model_over_a_catchup_model_over_an_in_memory_model_with_some_history() {
        inMemory = new InMemorySubscriptionModel();
        catchup = new StreamCatchupSubscriptionModel(new LiveModel(inMemory), new History(1), new CatchupSubscriptionModelConfig(100));
        competing = new CompetingConsumerSubscriptionModel(catchup, new GrantingStrategy());
    }

    @AfterEach
    void shutdown() {
        try {
            nodes.forEach(Node::shutdown);
        } finally {
            try {
                competing.shutdown();
            } finally {
                inMemory.shutdown();
            }
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

    @Test
    @Timeout(60)
    void a_stop_from_the_action_of_a_replay_over_several_events_hands_the_lease_on_only_once_the_action_has_returned_and_the_stopped_node_delivers_nothing_more() throws InterruptedException {
        // Given two nodes that share one lease, the second of which waits for it
        SharedLease lease = new SharedLease();
        List<Delivery> deliveries = new CopyOnWriteArrayList<>();
        CountDownLatch secondNodeWaits = new CountDownLatch(1);
        CountDownLatch secondNodeDelivers = new CountDownLatch(1);
        AtomicBoolean firstEvent = new AtomicBoolean(true);
        AtomicReference<@Nullable Throwable> stopThrew = new AtomicReference<>();
        Node first = new Node("node-1", lease, 3);
        Node second = new Node("node-2", lease, 3);
        nodes.add(first);
        nodes.add(second);
        long begin = System.nanoTime();
        first.competing.subscribe(first.name, ID, null, StartAtTime.beginningOfTime(), event -> {
            long started = System.nanoTime();
            try {
                if (firstEvent.compareAndSet(true, false)) {
                    secondNodeWaits.await(10, TimeUnit.SECONDS);
                    try {
                        first.competing.stop();
                    } catch (Throwable e) {
                        stopThrew.set(e);
                    }
                    // The action stays open, so a lease given up inside stop() lets the second node deliver meanwhile
                    secondNodeDelivers.await(HOLD_THE_ACTION_OPEN.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                deliveries.add(new Delivery(first.name, event.getId(), started, System.nanoTime()));
            }
        });

        // When the second node subscribes, and the first node's action stops the first node on the first event
        second.competing.subscribe(second.name, ID, null, StartAtTime.beginningOfTime(), event -> {
            long started = System.nanoTime();
            secondNodeDelivers.countDown();
            deliveries.add(new Delivery(second.name, event.getId(), started, System.nanoTime()));
        });
        secondNodeWaits.countDown();
        boolean secondNodeDelivered = waitUpTo(EVENTUALLY, () -> deliveries.stream().anyMatch(delivery -> delivery.node.equals(second.name)));
        waitUpTo(Duration.ofSeconds(1), () -> deliveries.stream().filter(delivery -> delivery.node.equals(second.name)).count() == 3);

        // Then
        String timeline = timeline(begin, deliveries, lease);
        List<Delivery> byTheFirstNode = deliveries.stream().filter(delivery -> delivery.node.equals(first.name)).toList();
        List<Delivery> byTheSecondNode = deliveries.stream().filter(delivery -> delivery.node.equals(second.name)).toList();
        long firstNodeDeliveredLastAt = byTheFirstNode.stream().mapToLong(delivery -> delivery.endedAt).max().orElse(Long.MIN_VALUE);
        assertThat(stopThrew.get()).as("what stop() threw when called from the action of the replay").isNull();
        assertThat(secondNodeDelivered).as("whether the second node delivered once the first gave up the lease. " + timeline).isTrue();
        assertAll(
                () -> assertThat(byTheFirstNode).as("what the first node delivered, which is nothing after the event whose action stopped it. " + timeline)
                        .extracting(delivery -> delivery.eventId).containsExactly("history-1"),
                () -> assertThat(byTheSecondNode).as("what the second node delivered before the first node's last delivery ended. " + timeline)
                        .noneMatch(delivery -> delivery.startedAt < firstNodeDeliveredLastAt)
        );
    }

    private String timeline(long begin, List<Delivery> deliveries, SharedLease lease) {
        List<Map.Entry<Long, String>> lines = new ArrayList<>();
        deliveries.forEach(delivery -> lines.add(Map.entry(delivery.startedAt, delivery.node + " delivered " + delivery.eventId + " from " + millis(begin, delivery.startedAt) + " to " + millis(begin, delivery.endedAt))));
        lease.releases.forEach(release -> lines.add(Map.entry(release.at, release.subscriberId + " released the lease at " + millis(begin, release.at) + " on thread " + release.thread)));
        lines.sort(Map.Entry.comparingByKey());
        return "Timeline in ms since the first subscribe: " + lines.stream().map(Map.Entry::getValue).toList();
    }

    private static String millis(long begin, long nanos) {
        return String.format("%.1f", (nanos - begin) / 1_000_000.0);
    }

    private static boolean waitUpTo(Duration timeout, BooleanSupplier condition) {
        try {
            await().atMost(timeout).pollInterval(5, TimeUnit.MILLISECONDS).until(condition::getAsBoolean);
            return true;
        } catch (ConditionTimeoutException e) {
            return false;
        }
    }

    private record Delivery(String node, String eventId, long startedAt, long endedAt) {
    }

    private record Release(String subscriberId, long at, String thread) {
    }

    // A catch-up model over a live model of its own, under a competing consumer model of its own
    private static final class Node {
        private final String name;
        private final InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel();
        private final StreamCatchupSubscriptionModel catchup;
        private final CompetingConsumerSubscriptionModel competing;

        private Node(String name, CompetingConsumerStrategy strategy, int events) {
            this.name = name;
            this.catchup = new StreamCatchupSubscriptionModel(new LiveModel(inMemory), new History(events), new CatchupSubscriptionModelConfig(100));
            this.competing = new CompetingConsumerSubscriptionModel(catchup, strategy);
        }

        private void shutdown() {
            try {
                competing.shutdown();
            } finally {
                inMemory.shutdown();
            }
        }
    }

    // One lease for each subscription, held by one subscriber at a time. Giving it up grants it to the subscriber that has waited longest, which is told on a thread of the strategy's own
    private static final class SharedLease implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new HashMap<>();
        private final Map<String, Deque<String>> waiting = new HashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final ExecutorService notifier = Executors.newSingleThreadExecutor();
        private final List<Release> releases = new CopyOnWriteArrayList<>();

        @Override
        public synchronized boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            if (holder == null || holder.equals(subscriberId)) {
                return true;
            }
            Deque<String> queue = waiting.computeIfAbsent(subscriptionId, __ -> new ArrayDeque<>());
            if (!queue.contains(subscriberId)) {
                queue.add(subscriberId);
            }
            return false;
        }

        @Override
        public synchronized void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            waiting.getOrDefault(subscriptionId, new ArrayDeque<>()).remove(subscriberId);
            giveUp(subscriptionId, subscriberId);
        }

        @Override
        public synchronized void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            giveUp(subscriptionId, subscriberId);
        }

        @Override
        public synchronized boolean hasLock(String subscriptionId, String subscriberId) {
            return subscriberId.equals(holders.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
            notifier.shutdownNow();
        }

        private void giveUp(String subscriptionId, String subscriberId) {
            if (!holders.remove(subscriptionId, subscriberId)) {
                return;
            }
            releases.add(new Release(subscriberId, System.nanoTime(), Thread.currentThread().getName()));
            String next = waiting.getOrDefault(subscriptionId, new ArrayDeque<>()).poll();
            if (next != null) {
                holders.put(subscriptionId, next);
                notifier.execute(() -> listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, next)));
            }
        }
    }

    // The history a replay reads, whichever way it asks for it
    private static final class History implements EventStoreQueries, CatchupReads {
        private final List<CloudEvent> history;

        private History(int events) {
            this.history = IntStream.rangeClosed(1, events).<CloudEvent>mapToObj(number -> CloudEventBuilder.v1().withId(events == 1 ? "history" : "history-" + number)
                    .withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.parse("2026-01-01T00:00:00Z").plusSeconds(number)).build()).toList();
        }

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
                    return History.this.query(filter, skip, limit, sortBy);
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
