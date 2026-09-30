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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A lease that changes hands while subscribe(..) is under way on a node, which is what the strategy's refresh thread
 * reports to the model as it happens. The node ends up serving the subscription only while it holds the lease, whichever
 * way the lease moved in that window. The lease is the real one in MongoDB, with a second node's strategy on the same
 * collection as the rival, and its time is short so that the refresh thread notices a change soon.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseChangeDuringSubscribeOverNativeMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration LEASE_TIME = Duration.ofSeconds(3);
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    private static final String SUBSCRIPTION_ID = "s1";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private String locks;
    private NativeMongoLeaseCompetingConsumerStrategy strategyA;
    private NativeMongoLeaseCompetingConsumerStrategy rival;
    private ObservedStrategy observedA;
    private WrappedModel wrappedOnA;
    private CompetingConsumerSubscriptionModel nodeA;
    private ExecutorService otherThreads;

    @BeforeEach
    void two_nodes_on_one_lease_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        locks = "locks-" + UUID.randomUUID();
        strategyA = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        rival = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        observedA = new ObservedStrategy(strategyA);
        wrappedOnA = new WrappedModel();
        nodeA = new CompetingConsumerSubscriptionModel(wrappedOnA, observedA);
        otherThreads = Executors.newCachedThreadPool();
    }

    @AfterEach
    void shutdown() {
        otherThreads.shutdownNow();
        nodeA.shutdown();
        rival.shutdown();
        mongoClient.close();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_node_that_loses_its_lease_while_subscribe_resumes_the_subscription_does_not_run_it_once_subscribe_returns() {
        Gate resumeOfS1 = new Gate();
        wrappedOnA.resumeGates.put(SUBSCRIPTION_ID, resumeOfS1);
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> subscribeOnA(), otherThreads);
        try {
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("subscribe on A waits inside the resume of s1 in the wrapped model, holding its lock").isTrue();
            assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A won the lease when it registered").isTrue();

            // A's lease expires in MongoDB and the rival takes it over, and A's refresh thread then tells A it lost it
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                expireTheLease();
                return rival.registerCompetingConsumer(SUBSCRIPTION_ID, "B");
            });
            observedA.prohibited.awaitCallbackSettled();
            assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A no longer holds the lease once the rival took it over").isFalse();
        } finally {
            resumeOfS1.open();
        }

        assertThat(subscribing).as("subscribe of s1 on A once the wrapped model resumed it").succeedsWithin(EVENTUALLY);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrappedOnA.isRunning(SUBSCRIPTION_ID))
                .as("[s1 runs in the wrapped model of A although A lost its lease to the rival]").isFalse());
        assertThat(rival.hasLock(SUBSCRIPTION_ID, "B")).as("the rival holds the lease, so it is the node that delivers").isTrue();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_node_granted_the_lease_while_subscribe_has_found_it_not_held_and_not_yet_recorded_the_subscription_runs_it() {
        assertThat(rival.registerCompetingConsumer(SUBSCRIPTION_ID, "B")).as("the rival holds the lease when A subscribes").isTrue();
        // subscribe on A asks whether A holds the lease, is told no, and the lease is free before it records anything
        Gate hasLockOfS1 = new Gate();
        observedA.holdTheFirstHasLockAnswer(hasLockOfS1);
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> subscribeOnA(), otherThreads);
        try {
            assertThat(hasLockOfS1.awaitEnteredOnAnotherThread()).as("subscribe on A waits between asking for the lease and recording the subscription").isTrue();

            rival.unregisterCompetingConsumer(SUBSCRIPTION_ID, "B");
            observedA.granted.awaitCallbackSettled();
            assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease once its refresh thread took it").isTrue();
        } finally {
            hasLockOfS1.open();
        }

        assertThat(subscribing).as("subscribe of s1 on A once it recorded the subscription").succeedsWithin(EVENTUALLY);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrappedOnA.isRunning(SUBSCRIPTION_ID))
                .as("[s1 runs in the wrapped model of A once A holds the lease]").isTrue());
    }

    private Subscription subscribeOnA() {
        return nodeA.subscribe("A", SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    private void expireTheLease() {
        database.getCollection(locks).updateOne(Filters.eq("_id", SUBSCRIPTION_ID), Updates.set("expiresAt", new Date(0)));
    }

    // Tells when the first call of one kind reaches the model from the strategy, and on which thread, and whether it has
    // returned
    private static final class Callback {
        private final CompletableFuture<Thread> receivedOn = new CompletableFuture<>();
        private final CompletableFuture<Void> returned = new CompletableFuture<>();

        private void around(Runnable call) {
            receivedOn.complete(Thread.currentThread());
            try {
                call.run();
            } finally {
                returned.complete(null);
            }
        }

        // The model's callback has either returned, or waits for something, which one that waits for the lock does
        private void awaitCallbackSettled() {
            Thread thread = receivedOn.orTimeout(15, SECONDS).join();
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> returned.isDone() || thread.getState() == Thread.State.WAITING);
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);
        private volatile @Nullable Thread enteredOn;

        private void pass() {
            enteredOn = Thread.currentThread();
            entered.countDown();
            try {
                open.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private boolean awaitEnteredOnAnotherThread() {
            try {
                return entered.await(10, TimeUnit.SECONDS) && enteredOn != Thread.currentThread();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private void open() {
            open.countDown();
        }
    }

    // The real strategy, with the first grant and the first prohibition it reports to the model observed, and the answer
    // of the first hasLock held back at a gate after the real answer was read, so it is stale once it is given
    private static final class ObservedStrategy implements CompetingConsumerStrategy {
        private final CompetingConsumerStrategy real;
        private final Callback granted = new Callback();
        private final Callback prohibited = new Callback();
        private final Map<CompetingConsumerListener, CompetingConsumerListener> observers = new ConcurrentHashMap<>();
        private final AtomicReference<@Nullable Gate> firstHasLock = new AtomicReference<>();

        private ObservedStrategy(CompetingConsumerStrategy real) {
            this.real = real;
        }

        private void holdTheFirstHasLockAnswer(Gate gate) {
            firstHasLock.set(gate);
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return real.registerCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            real.unregisterCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            real.releaseCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            boolean answer = real.hasLock(subscriptionId, subscriberId);
            Gate gate = firstHasLock.getAndSet(null);
            if (gate != null) {
                gate.pass();
            }
            return answer;
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
            CompetingConsumerListener observer = new CompetingConsumerListener() {
                @Override
                public void onConsumeGranted(String subscriptionId, String subscriberId) {
                    granted.around(() -> listener.onConsumeGranted(subscriptionId, subscriberId));
                }

                @Override
                public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                    prohibited.around(() -> listener.onConsumeProhibited(subscriptionId, subscriberId));
                }
            };
            observers.put(listener, observer);
            real.addListener(observer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
            CompetingConsumerListener observer = observers.remove(listener);
            if (observer != null) {
                real.removeListener(observer);
            }
        }

        @Override
        public void shutdown() {
            real.shutdown();
        }
    }

    // A model of a user's own that holds a subscription paused when asked to, and whose resume of a subscription waits at
    // its gate once. The wait is outside the monitor of this model, so it holds up nothing but the call that waits.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, false);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, true);
        }

        private synchronized Subscription make(String subscriptionId, boolean paused) {
            if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
            }
            (running && !paused ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Set<String> subscriptionIds() {
            Set<String> ids = new HashSet<>(runningIds);
            ids.addAll(pausedIds);
            return ids;
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            Gate gate = resumeGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
