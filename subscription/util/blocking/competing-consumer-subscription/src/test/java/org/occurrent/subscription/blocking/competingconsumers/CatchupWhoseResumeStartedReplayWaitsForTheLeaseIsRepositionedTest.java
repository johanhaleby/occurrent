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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.jspecify.annotations.Nullable;
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
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A replay that a {@code resumeSubscription(id)} started, whose event waits for the lease of a competing consumer in
 * the action, is held by a repositioning {@code resumeSubscription(id, startAt)} from another thread, which waits for
 * the replay to return. The resume tells the action that its event is no longer wanted, so the action returns from its
 * wait for the lease and the resume doesn't time out, also when the catch-up model is shut down while the resume waits.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupWhoseResumeStartedReplayWaitsForTheLeaseIsRepositionedTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration BOUND = Duration.ofSeconds(3);

    private final FencedStrategy strategy = new FencedStrategy();
    private final AtomicReference<Thread> repositioningThread = new AtomicReference<>();
    private final AtomicReference<String> repositioningOutcome = new AtomicReference<>();
    private final AtomicReference<Long> repositioningMillis = new AtomicReference<>();
    private StreamCatchupSubscriptionModel catchup;
    private CompetingConsumerSubscriptionModel competing;
    private CompletableFuture<Void> repositioning;

    @BeforeEach
    void a_resume_started_replay_whose_event_waits_for_the_lease_and_a_repositioning_resume_waiting_for_it() throws Exception {
        History history = new History();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        catchup = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(new LiveModel(), storage), history, new CatchupSubscriptionModelConfig(100, CheckpointStorageConfig.useCheckpointStorage(storage)));
        competing = new CompetingConsumerSubscriptionModel(catchup, strategy);
        competing.start();

        // Given a subscription that went live after an empty replay, and was then paused with a position the catch-up replays from
        competing.subscribe(NODE, ID, null, StartAtTime.beginningOfTime(), cloudEvent -> {
        }).waitUntilStarted();
        catchup.pauseSubscription(ID);
        storage.save(ID, TimeBasedCheckpoint.beginningOfTime());
        history.hasTheEvent = true;

        // And a resume of it that starts a replay whose event waits for a lease this node is never given
        strategy.fenced = true;
        catchup.resumeSubscription(ID);
        assertThat(strategy.askedWithoutTheLease.await(5, TimeUnit.SECONDS)).as("the event of the replay resume started waits for the lease").isTrue();
    }

    // A repositioning resume from another thread, which tells the action of the replay it holds that its event is unwanted
    private void startTheRepositioningResume() {
        long before = System.nanoTime();
        repositioning = CompletableFuture.runAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            try {
                catchup.resumeSubscription(ID, StartAtTime.beginningOfTime());
                repositioningOutcome.set("returned");
            } catch (Throwable t) {
                repositioningOutcome.set("threw " + t.getClass().getName() + ": " + t.getMessage());
            } finally {
                repositioningMillis.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before));
            }
        });
    }

    @AfterEach
    void let_the_repositioning_finish_report_it_and_shut_down() throws Exception {
        try {
            strategy.releaseTheGate();
            repositioning.get(15, TimeUnit.SECONDS);
        } finally {
            System.out.println("REPOSITIONING resumeSubscription(id, startAt): " + repositioningOutcome.get() + " after " + repositioningMillis.get() + " ms");
            strategy.fenced = false;
            CompletableFuture.runAsync(competing::shutdown).get(20, TimeUnit.SECONDS);
        }
    }

    @Test
    void a_shutdown_of_the_catchup_model_while_the_repositioning_resume_waits_for_the_held_replay_lets_the_replay_leave_its_lease_wait_and_the_resume_finish_without_its_timeout() throws Exception {
        // Given the repositioning resume has told the action its event is unwanted, and the action's renewed ask for the lease is held
        strategy.armTheGate();
        startTheRepositioningResume();
        assertThat(strategy.theActionAskedAgain.await(5, TimeUnit.SECONDS)).as("the action woke from its lease wait and asked the strategy again").isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> repositioningThread.get() != null && Arrays.stream(repositioningThread.get().getStackTrace())
                .anyMatch(frame -> frame.getMethodName().equals("awaitTheActions")));

        // When the catch-up model is shut down directly, and the held ask is then released
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(catchup::shutdown);
        boolean shutdownReturned = awaitDone(shutdown);
        strategy.releaseTheGate();

        // Then
        assertAll(
                () -> assertThat(shutdownReturned).as("the shutdown of the catch-up model returned within %s while the resume waited for the replay", BOUND).isTrue(),
                () -> await().atMost(BOUND).untilAsserted(() -> assertThat(strategy.replayThreadIsWaitingForTheLease()).as("the action of the held replay is still in its wait for the lease").isFalse()),
                () -> await().atMost(BOUND).untilAsserted(() -> assertThat(repositioning.isDone()).as("the repositioning resume has finished within %s of the release", BOUND).isTrue()),
                () -> assertThat(repositioningOutcome.get()).as("what the repositioning resume did, which is not the ten second timeout").doesNotContain("within 10000 ms"));
    }

    @Test
    void without_a_shutdown_the_repositioning_resume_ends_the_lease_wait_of_the_held_replay_and_returns_with_the_subscription_running() {
        // When
        startTheRepositioningResume();

        // Then
        assertAll(
                () -> await().atMost(BOUND).untilAsserted(() -> assertThat(strategy.replayThreadIsWaitingForTheLease()).as("the action of the held replay is still in its wait for the lease").isFalse()),
                () -> await().atMost(BOUND).untilAsserted(() -> assertThat(repositioningOutcome.get()).as("what the repositioning resume did within %s", BOUND).isEqualTo("returned")),
                () -> assertThat(catchup.isRunning(ID)).as("the subscription runs after the repositioning resume").isTrue());
    }

    private static boolean awaitDone(CompletableFuture<Void> future) {
        try {
            future.get(BOUND.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("Tested").withTime(OffsetDateTime.now()).build();
    }

    // Grants the lease while not fenced. Once fenced, nobody but the thread of the test has it, so an event of the
    // replay is asked for it and waits, and the first thread that asks is the replay thread the test then watches.
    private static final class FencedStrategy implements CompetingConsumerStrategy {
        final CountDownLatch askedWithoutTheLease = new CountDownLatch(1);
        volatile boolean fenced;
        private final Thread testThread = Thread.currentThread();
        private final AtomicReference<Thread> replayThread = new AtomicReference<>();

        final CountDownLatch theActionAskedAgain = new CountDownLatch(1);
        private final CountDownLatch gate = new CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicBoolean gateArmed = new java.util.concurrent.atomic.AtomicBoolean();

        // The next ask of the replay thread is held until the gate is released
        void armTheGate() {
            gateArmed.set(true);
        }

        void releaseTheGate() {
            gate.countDown();
        }

        boolean replayThreadIsWaitingForTheLease() {
            Thread thread = replayThread.get();
            return thread != null && Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("awaitTheLease"));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (fenced && Thread.currentThread() != testThread) {
                replayThread.compareAndSet(null, Thread.currentThread());
                askedWithoutTheLease.countDown();
                if (Thread.currentThread() == replayThread.get() && gateArmed.compareAndSet(true, false)) {
                    theActionAskedAgain.countDown();
                    try {
                        gate.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            return !fenced;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // A live model that tracks which subscriptions run and which are paused, and can resume one at a position
    private static final class LiveModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            if (!paused.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!paused.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (!running.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not running");
            }
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global");
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }

    // The history a replay reads, empty until the test adds its one event. Read by time, since this store writes no position.
    private static final class History implements EventStoreQueries, CatchupReads {
        volatile boolean hasTheEvent;

        private Stream<CloudEvent> events() {
            return hasTheEvent ? Stream.of(cloudEvent()) : Stream.empty();
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return events();
        }

        @Override
        public long count(Filter filter) {
            return hasTheEvent ? 1 : 0;
        }

        @Override
        public boolean exists(Filter filter) {
            return hasTheEvent;
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
                    return events();
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
