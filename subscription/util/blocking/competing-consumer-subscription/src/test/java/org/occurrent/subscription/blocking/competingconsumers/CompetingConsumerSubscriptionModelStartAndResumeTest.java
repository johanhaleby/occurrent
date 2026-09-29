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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What starting the model and resuming a subscription do when the lease is not free, or when the wrapped model throws
 * on a subscription whose lease was just won. The strategy tells its listeners about a grant on the thread that
 * registers, the way the MongoDB lease strategies do, and nothing here needs MongoDB.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelStartAndResumeTest {

    private static final String SUBSCRIBER_ID = "subscriber";

    private final RecordingDelegate delegate = new RecordingDelegate();
    private final SynchronousLeaseStrategy strategy = new SynchronousLeaseStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void start_throws_what_the_wrapped_model_threw_once_every_consumer_had_its_turn() {
        strategy.grantOnRegister = false;
        subscribe("failing-1");
        subscribe("healthy");
        subscribe("failing-2");
        model.stop();
        strategy.grantOnRegister = true;
        delegate.throwsOn.addAll(Set.of("failing-1", "failing-2"));

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that a subscription did not start").isInstanceOf(IllegalStateException.class);
        assertThat(thrown.getSuppressed()).as("and learns about every one of them").hasSize(1);
        assertThat(delegate.running).as("a failing consumer does not keep the others from starting").containsExactly("healthy");
        assertThat(strategy.holders).as("a consumer that failed to start gave its lease back").containsExactly("healthy");
    }

    @Test
    void resuming_a_consumer_the_wrapped_model_throws_on_throws_to_the_caller() {
        strategy.grantOnRegister = false;
        subscribe("failing");
        model.pauseSubscription("failing");
        strategy.grantOnRegister = true;
        delegate.throwsOn.add("failing");

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("failing"));

        assertThat(thrown).as("the grant that registering brought failed to start the subscription, and the caller of resume is told")
                .isInstanceOf(IllegalStateException.class);
        assertThat(strategy.holders).isEmpty();
        assertThat(model.isRunning("failing")).isFalse();
    }

    @Test
    void a_consumer_start_resumes_while_another_node_holds_its_lease_is_resumed_once_this_node_wins_it() {
        strategy.grantOnRegister = true;
        subscribe("stopped");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);
        assertThat(model.isRunning("stopped")).isFalse();

        strategy.grant("stopped");

        assertThat(model.isRunning("stopped"))
                .as("the grant resumes the subscription rather than handing the lease back as if a user had paused it")
                .isTrue();
        assertThat(strategy.holders).containsExactly("stopped");
        assertThat(strategy.calls).as("nothing gave the lease up after the grant").endsWith("grant stopped");
    }

    private void subscribe(String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    /**
     * Keeps track of which subscriptions run and which are paused, and throws when starting any subscription in
     * {@link #throwsOn}.
     */
    private static final class RecordingDelegate implements SubscriptionModel {
        private final Set<String> throwsOn = new HashSet<>();
        private final List<String> running = new ArrayList<>();
        private final Set<String> paused = new HashSet<>();
        private boolean started = true;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throwIfRefused(subscriptionId);
            running.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public void stop() {
            started = false;
            paused.addAll(running);
            running.clear();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            started = true;
        }

        @Override
        public boolean isRunning() {
            return started;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throwIfRefused(subscriptionId);
            paused.remove(subscriptionId);
            running.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.add(subscriptionId);
        }

        private void throwIfRefused(String subscriptionId) {
            if (throwsOn.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped model cannot start " + subscriptionId + " right now");
            }
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Grants the lease on register when {@link #grantOnRegister} is set, and tells the listeners on the registering
     * thread, as a lease strategy does for a lease that changed hands. {@link #grant(String)} plays a refresh round
     * granting a lease that another node gave up.
     */
    private static final class SynchronousLeaseStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new ArrayList<>();
        private final Set<String> holders = new HashSet<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();
        private boolean grantOnRegister;

        void grant(String subscriptionId) {
            calls.add("grant " + subscriptionId);
            holders.add(subscriptionId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, SUBSCRIBER_ID));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register " + subscriptionId);
            if (grantOnRegister && holders.add(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister " + subscriptionId);
            if (holders.remove(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release " + subscriptionId);
            if (holders.remove(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
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
