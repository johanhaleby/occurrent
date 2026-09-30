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
import java.util.*;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A wrapped model written by a user, which does not override {@link SubscriptionModel#subscribePaused}, and which
 * either runs all the time or can throw from {@link SubscriptionModel#stop()}. A node delivers a subscription only while
 * it holds its lease, and after any stop() the wrapped model holds what this model records. A subscription made while
 * stopped reaches such a model, while it runs, only once the node wins the lease.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerOverAUserWrittenModelTest {

    @Test
    void a_subscription_made_while_stopped_over_a_model_that_always_runs_reaches_that_model_once_the_node_holds_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(true, false, false);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();

        assertThatCode(() -> model.subscribe("node", "s2", null, StartAt.subscriptionModelDefault(), __ -> {})).doesNotThrowAnyException();
        assertThat(delegate.knows("s2")).as("s2 in the running wrapped model while stopped").isFalse();
        assertThat(strategy.holders).as("leases held while stopped").isEmpty();

        model.start(false);
        assertThat(delegate.isRunning("s2")).as("s2 runs in the wrapped model after start(false)").isTrue();
        assertThat(strategy.holders).as("leases held after start(false)").contains("s2");
    }

    @Test
    void a_subscription_made_while_stopped_after_a_resume_reaches_the_running_wrapped_model_once_the_node_holds_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false, false, false);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();
        model.resumeSubscription("s1");

        assertThatCode(() -> model.subscribe("node", "s2", null, StartAt.subscriptionModelDefault(), __ -> {})).doesNotThrowAnyException();
        assertThat(delegate.knows("s2")).as("s2 in the running wrapped model while stopped").isFalse();
        assertThat(strategy.holders).as("leases held while stopped").containsExactly("s1");

        model.start(false);
        assertThat(delegate.isRunning("s2")).as("s2 runs in the wrapped model after start(false)").isTrue();
        assertThat(strategy.holders).as("leases held after start(false)").containsExactlyInAnyOrder("s1", "s2");
    }

    @Test
    void a_stop_whose_wrapped_stop_throws_pauses_every_subscription_there_before_giving_up_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false, true, false);
        delegate.start(true);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});

        assertThat(catchThrowable(model::stop)).hasMessage("stop failed");

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model after a stop that threw").isFalse();
        assertThat(strategy.holders).as("leases held after a stop that threw").isEmpty();
        assertThat(model.isPaused("s1")).isTrue();
    }

    @Test
    void a_subscription_made_after_a_stop_whose_wrapped_stop_threw_reaches_the_wrapped_model_once_the_node_holds_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false, true, false);
        delegate.start(true);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        catchThrowable(model::stop);

        assertThatCode(() -> model.subscribe("node", "s2", null, StartAt.subscriptionModelDefault(), __ -> {})).doesNotThrowAnyException();
        assertThat(delegate.knows("s2")).as("s2 in the running wrapped model while stopped").isFalse();
        assertThat(strategy.holders).as("leases held while stopped").isEmpty();

        model.start(true);
        assertThat(delegate.isRunning("s2")).as("s2 runs in the wrapped model after start(true)").isTrue();
        assertThat(strategy.holders).as("leases held after start(true)").containsExactlyInAnyOrder("s1", "s2");
    }

    @Test
    void a_start_after_a_stop_whose_wrapped_stop_threw_runs_the_subscription_again_with_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false, true, false);
        delegate.start(true);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        catchThrowable(model::stop);

        Throwable startFailure = catchThrowable(() -> model.start(true));

        assertThat(startFailure).as("start(true) after a stop that threw").isNull();
        assertThat(delegate.isRunning("s1")).isTrue();
        assertThat(strategy.holders).as("lease held by the node that delivers s1").contains("s1");
    }

    @Test
    void a_subscription_the_wrapped_model_cannot_pause_keeps_its_lease_and_makes_stop_throw() {
        UserWrittenModel delegate = new UserWrittenModel(true, false, true);
        LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});

        assertThat(catchThrowable(model::stop)).isInstanceOf(IllegalStateException.class).hasMessageContaining("s1");

        assertThat(delegate.isRunning("s1")).isTrue();
        assertThat(model.isRunning("s1")).as("recorded as running while the wrapped model delivers it").isTrue();
        assertThat(strategy.holders).as("lease kept while the wrapped model delivers s1").contains("s1");
    }

    // Starts where it is told, and holds a subscription made while it is stopped paused. One that always runs ignores
    // stop(), and one that cannot pause ignores pauseSubscription(..).
    private static final class UserWrittenModel implements SubscriptionModel {
        private final boolean alwaysRuns;
        private final boolean cannotPause;
        private boolean stopThrowsOnce;
        private boolean running;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();

        private UserWrittenModel(boolean alwaysRuns, boolean stopThrowsOnce, boolean cannotPause) {
            this.alwaysRuns = alwaysRuns;
            this.stopThrowsOnce = stopThrowsOnce;
            this.cannotPause = cannotPause;
            this.running = alwaysRuns;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
            }
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public void stop() {
            if (stopThrowsOnce) {
                stopThrowsOnce = false;
                throw new IllegalStateException("stop failed");
            }
            if (alwaysRuns) {
                return;
            }
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        private boolean knows(String subscriptionId) {
            return runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!pausedIds.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            runningIds.add(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (!cannotPause && runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }
    }

    private record UserWrittenSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // Grants every lease asked for and records who holds one
    private static final class LeaseRecordingStrategy implements CompetingConsumerStrategy {
        private final Set<String> holders = new HashSet<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();

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
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }
    }
}
