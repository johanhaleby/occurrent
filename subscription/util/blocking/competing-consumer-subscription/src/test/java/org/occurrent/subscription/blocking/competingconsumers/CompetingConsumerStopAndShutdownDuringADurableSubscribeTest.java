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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A durable model reads the stored position of a subscription to make it from its default start position and to
 * resume it, which takes as long as the database cannot be reached. Neither stop() nor shutdown() waits for the
 * subscribe of one that does not compete while it reads. That subscribe starts the wrapped models before the
 * subscription is made, so the subscription runs once made and is not resumed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStopAndShutdownDuringADurableSubscribeTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than EVENTUALLY, so a stop() or shutdown() that waits for the read is still waiting when the test gives
    // up on it
    private static final Duration READ_HELD_AT_MOST = Duration.ofSeconds(30);

    @ParameterizedTest(name = "called during the subscribe: {0}, start position the durable model gets: {1}")
    @CsvSource({"stop(), now", "stop(), default", "shutdown(), now", "shutdown(), default"})
    void stop_and_shutdown_return_while_the_subscribe_of_a_subscription_that_does_not_compete_reads_its_stored_position(String called, String durableModelStartsAt) {
        Fixture fixture = new Fixture();
        Gate read = new Gate();
        try {
            fixture.storage.nextRead.set(read);
            // Throws when it finds this model shut down
            CompletableFuture<@Nullable Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> fixture.subscribeNc(durableModelStartsAt)));
            if (durableModelStartsAt.equals("default")) {
                assertThat(read.awaitEntered()).as("the durable model reads the stored position of nc to make it").isTrue();
            } else {
                // The durable model reads nothing to make it from now(), so the subscribe reads only to resume it
                await().atMost(EVENTUALLY).until(() -> read.entered() || subscribing.isDone());
            }

            Runnable call = called.equals("stop()") ? fixture.model::stop : fixture.model::shutdown;
            assertThat(CompletableFuture.runAsync(call)).as("[%s during the subscribe of nc, while a read of its stored position is held]", called).succeedsWithin(EVENTUALLY);
            read.open();
            assertThat(subscribing).as("the subscribe of nc").succeedsWithin(EVENTUALLY);

            assertThat(fixture.wrapped.runs("nc")).as("[nc runs in the wrapped model once %s and the subscribe returned]", called).isFalse();
        } finally {
            read.open();
            fixture.model.shutdown();
        }
    }

    @Test
    void a_subscription_that_does_not_compete_runs_once_its_subscribe_returns_with_no_read_of_its_stored_position() {
        Fixture fixture = new Fixture();
        try {
            fixture.subscribeNc("now");

            assertThat(fixture.wrapped.log).as("[calls to the wrapped model, which was stopped before the subscribe]").containsExactly("stop", "start false", "subscribe nc running=true");
            assertThat(fixture.storage.reads).as("reads of the stored position of nc").hasValue(0);
            assertThat(fixture.wrapped.runs("nc")).as("nc runs in the wrapped model once its subscribe returned").isTrue();
        } finally {
            fixture.model.shutdown();
        }
    }

    private static final class Fixture {
        private final WrappedModel wrapped = new WrappedModel();
        private final Storage storage = new Storage();
        private final CompetingConsumerSubscriptionModel model;

        // The wrapped model is stopped, as one built with autoStartup(false) is
        private Fixture() {
            wrapped.stop();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
            model = new CompetingConsumerSubscriptionModel(durable, new Strategy());
        }

        // Resolves to null here, which makes this model hand nc straight to the durable model, and to the given start
        // position there
        private void subscribeNc(String durableModelStartsAt) {
            StartAt there = durableModelStartsAt.equals("now") ? StartAt.now() : StartAt.subscriptionModelDefault();
            model.subscribe(SUBSCRIBER, "nc", null, StartAt.dynamic(context -> context.subscriptionModelType() == CompetingConsumerSubscriptionModel.class ? null : there), __ -> {
            });
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);

        private void pass() {
            entered.countDown();
            try {
                if (!open.await(READ_HELD_AT_MOST.toMillis(), MILLISECONDS)) {
                    throw new IllegalStateException("Timed out waiting for the test to open a gate");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private boolean entered() {
            return entered.getCount() == 0;
        }

        private boolean awaitEntered() {
            try {
                return entered.await(EVENTUALLY.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private void open() {
            open.countDown();
        }
    }

    // Stores nothing, and its next read waits at a gate when one is set, as a read waits through a database outage
    private static final class Storage implements CheckpointStorage {
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicReference<@Nullable Gate> nextRead = new AtomicReference<>();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            reads.incrementAndGet();
            Gate gate = nextRead.getAndSet(null);
            if (gate != null) {
                gate.pass();
            }
            return null;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
        }

        @Override
        public boolean exists(String subscriptionId) {
            return false;
        }
    }

    // Grants every lease on register
    private static final class Strategy implements CompetingConsumerStrategy {
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
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void shutdown() {
        }
    }

    // A model that holds what it is given paused while it is stopped, and that a durable model can resume a
    // subscription in from a stored position. Every subscribe, resume, start, stop and shutdown is kept in log.
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final List<String> log = new CopyOnWriteArrayList<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            log.add("subscribe " + subscriptionId + " running=" + running);
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            log.add("stop");
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            log.add("start " + resumeSubscriptionsAutomatically);
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public synchronized void shutdown() {
            log.add("shutdown");
            running = false;
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            return running && runningIds.contains(subscriptionId);
        }

        // Delivers, which a subscription the wrapped model holds as running does only while that model runs
        private synchronized boolean runs(String subscriptionId) {
            return running && runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            log.add("resumeSubscription " + subscriptionId);
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            log.add("pauseSubscription " + subscriptionId);
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
