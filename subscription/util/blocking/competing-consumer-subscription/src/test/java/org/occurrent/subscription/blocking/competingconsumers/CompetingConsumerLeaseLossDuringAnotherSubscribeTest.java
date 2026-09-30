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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wrapped model can take its time to subscribe, for example while a change stream opens. A lease callback for
 * another subscription must not wait for it, since that subscription keeps delivering without its lease until the
 * callback pauses it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseLossDuringAnotherSubscribeTest {

    @Test
    void a_lease_loss_pauses_the_subscription_without_waiting_for_another_subscription_the_wrapped_model_is_subscribing() throws Exception {
        SlowToSubscribeModel delegate = new SlowToSubscribeModel("s2");
        GrantingStrategy strategy = new GrantingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});

        CompletableFuture<Subscription> subscribingS2 = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s2", null, StartAt.subscriptionModelDefault(), __ -> {}));
        try {
            assertThat(delegate.subscribing.await(5, SECONDS)).as("s2's wrapped subscribe began").isTrue();

            CompletableFuture<Void> losingS1 = CompletableFuture.runAsync(() -> strategy.lose("s1", "node"));

            assertThat(losingS1).as("the lease loss of s1 while the wrapped model subscribes s2").succeedsWithin(Duration.ofSeconds(2));
            assertThat(delegate.isPaused("s1")).as("s1 paused in the wrapped model while s2's subscribe runs").isTrue();
        } finally {
            delegate.release.countDown();
        }
        assertThat(subscribingS2).succeedsWithin(Duration.ofSeconds(5));
        assertThat(delegate.isRunning("s2")).as("s2 runs in the wrapped model").isTrue();
    }

    @Test
    void a_subscription_that_loses_its_lease_while_the_wrapped_model_subscribes_it_is_paused_once_that_subscribe_returns() throws Exception {
        SlowToSubscribeModel delegate = new SlowToSubscribeModel("s1");
        GrantingStrategy strategy = new GrantingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

        CompletableFuture<Subscription> subscribingS1 = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));
        CompletableFuture<Void> losingS1;
        try {
            assertThat(delegate.subscribing.await(5, SECONDS)).as("s1's wrapped subscribe began").isTrue();
            losingS1 = CompletableFuture.runAsync(() -> strategy.lose("s1", "node"));
            // Given time to find nothing recorded, where the callback no longer waits on the subscribe
            losingS1.completeOnTimeout(null, 500, MILLISECONDS);
            losingS1.join();
        } finally {
            delegate.release.countDown();
        }
        assertThat(subscribingS1).succeedsWithin(Duration.ofSeconds(5));

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model without its lease").isFalse();
        assertThat(model.isPaused("s1")).as("s1 recorded as paused").isTrue();
    }

    @Test
    void a_grant_that_comes_while_a_lost_registration_is_recorded_starts_the_subscription() {
        SlowToSubscribeModel delegate = new SlowToSubscribeModel("none");
        GrantingStrategy strategy = new GrantingStrategy();
        // The registration loses, and the lease comes to this node on another thread before subscribe records it
        strategy.grantOnAnotherThreadAfterLosing("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model with the lease it was granted").isTrue();
        assertThat(model.isRunning("s1")).as("s1 recorded as running").isTrue();
    }

    // Always runs, and subscribing one chosen id waits until the test releases it
    private static final class SlowToSubscribeModel implements SubscriptionModel {
        private final String slowId;
        private final CountDownLatch subscribing = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final Set<String> runningIds = ConcurrentHashMap.newKeySet();
        private final Set<String> pausedIds = ConcurrentHashMap.newKeySet();

        private SlowToSubscribeModel(String slowId) {
            this.slowId = slowId;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (subscriptionId.equals(slowId)) {
                subscribing.countDown();
                try {
                    if (!release.await(10, SECONDS)) {
                        throw new IllegalStateException("never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            runningIds.add(subscriptionId);
            return new InstantSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
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
            return runningIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
            return new InstantSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }
    }

    private record InstantSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // Grants every lease asked for, unless told to lose one, and tells the model when a lease moves, as a notifier does
    private static final class GrantingStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final Set<String> grantedOnAnotherThreadAfterLosing = ConcurrentHashMap.newKeySet();

        void grantOnAnotherThreadAfterLosing(String subscriptionId) {
            grantedOnAnotherThreadAfterLosing.add(subscriptionId);
        }

        void lose(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
            listeners.forEach(l -> l.onConsumeProhibited(subscriptionId, subscriberId));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (grantedOnAnotherThreadAfterLosing.remove(subscriptionId)) {
                holders.put(subscriptionId, subscriberId);
                // Waits a while for the grant, not for good, since a model that holds its monitor here makes the
                // grant wait for this registration to return
                CompletableFuture.runAsync(() -> listeners.forEach(l -> l.onConsumeGranted(subscriptionId, subscriberId)))
                        .completeOnTimeout(null, 500, MILLISECONDS).join();
                return false;
            }
            holders.put(subscriptionId, subscriberId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
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
    }
}
