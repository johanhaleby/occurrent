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
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Once start(..) or stop() has returned, no thread of theirs still holds a subscription's lock, so a lease callback made
 * on the caller's thread right after is acted on there and then, and is not left to a later try on another thread. The
 * rounds run side by side, since the window is the few instructions between a worker thread completing its work and
 * unlocking, and the callback is made at once, without waiting for anything.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStartStopReleaseTheirLocksBeforeReturningTest {

    private static final String NODE = "node";
    private static final int ROUNDS = 3000;
    private static final int PARALLEL_ROUNDS = 4;
    private static final List<String> SUBSCRIPTION_IDS = List.of("s1", "s2", "s3", "s4", "s5", "s6");
    // The last id first, since its worker tends to be the one that start(..) and stop() finish with
    private static final List<String> LAST_STARTED_FIRST = SUBSCRIPTION_IDS.reversed();

    @Test
    @Timeout(value = 60, unit = SECONDS)
    void a_grant_made_on_the_callers_thread_right_after_stop_returned_is_acted_on_there_and_then() throws Exception {
        Queue<String> notActedOnAtOnce = new ConcurrentLinkedQueue<>();

        inRounds(round -> {
            WrappedModel wrapped = new WrappedModel();
            Strategy strategy = new Strategy();
            CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
            try {
                for (String id : SUBSCRIPTION_IDS) {
                    model.subscribe(NODE, id, null, StartAt.subscriptionModelDefault(), __ -> {
                    });
                }
                model.stop();
                strategy.unregisteredOn.clear();
                Thread grantedOn = Thread.currentThread();

                for (String id : LAST_STARTED_FIRST) {
                    // A late grant for a consumer that stop() gave up, which the model hands back as it is stopped
                    strategy.holders.add(id);
                    model.onConsumeGranted(id, NODE);
                    if (strategy.unregisteredOn.get(id) != grantedOn || strategy.holders.contains(id) || wrapped.isRunning(id)) {
                        notActedOnAtOnce.add("round " + round + " " + id);
                    }
                }
            } finally {
                model.shutdown();
            }
        });

        assertThat(notActedOnAtOnce.isEmpty())
                .as("[grants made on the caller's thread right after stop() returned that were not handed back there and then, %d of %d, the first ones %s]", notActedOnAtOnce.size(), ROUNDS * SUBSCRIPTION_IDS.size(), firstOf(notActedOnAtOnce))
                .isTrue();
    }

    @Test
    @Timeout(value = 60, unit = SECONDS)
    void a_grant_made_on_the_callers_thread_right_after_start_without_resuming_returned_runs_the_waiting_subscription_there_and_then() throws Exception {
        Queue<String> notRunAtOnce = new ConcurrentLinkedQueue<>();

        inRounds(round -> {
            WrappedModel wrapped = new WrappedModel();
            Strategy strategy = new Strategy();
            // Another node holds every lease, so each subscription waits for its own
            strategy.rivals.addAll(SUBSCRIPTION_IDS);
            CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
            try {
                for (String id : SUBSCRIPTION_IDS) {
                    model.subscribe(NODE, id, null, StartAt.subscriptionModelDefault(), __ -> {
                    });
                }
                model.stop();
                model.start(false);
                Thread grantedOn = Thread.currentThread();

                for (String id : LAST_STARTED_FIRST) {
                    strategy.rivals.remove(id);
                    strategy.holders.add(id);
                    model.onConsumeGranted(id, NODE);
                    if (wrapped.startedOn.get(id) != grantedOn || !wrapped.isRunning(id)) {
                        notRunAtOnce.add("round " + round + " " + id);
                    }
                }
            } finally {
                model.shutdown();
            }
        });

        assertThat(notRunAtOnce.isEmpty())
                .as("[grants made on the caller's thread right after start(false) returned that did not run the waiting subscription there and then, %d of %d, the first ones %s]", notRunAtOnce.size(), ROUNDS * SUBSCRIPTION_IDS.size(), firstOf(notRunAtOnce))
                .isTrue();
    }

    private static List<String> firstOf(Queue<String> violations) {
        return violations.stream().limit(3).toList();
    }

    private static void inRounds(Consumer<Integer> round) throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(PARALLEL_ROUNDS);
        try {
            List<Future<?>> rounds = IntStream.range(0, ROUNDS).<Future<?>>mapToObj(i -> threads.submit(() -> round.accept(i))).toList();
            for (Future<?> future : rounds) {
                future.get(50, SECONDS);
            }
        } finally {
            threads.shutdownNow();
        }
    }

    // Grants a lease on register unless another node holds it, and records the thread of each unregister
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> rivals = ConcurrentHashMap.newKeySet();
        private final Map<String, Thread> unregisteredOn = new ConcurrentHashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (rivals.contains(subscriptionId)) {
                return false;
            }
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            unregisteredOn.put(subscriptionId, Thread.currentThread());
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
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
        }
    }

    // A model of a user's own that holds a subscription paused when asked to, and records the thread that made or resumed
    // each subscription there
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Thread> startedOn = new ConcurrentHashMap<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            startedOn.put(subscriptionId, Thread.currentThread());
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
            startedOn.put(subscriptionId, Thread.currentThread());
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
