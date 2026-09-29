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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * When the wrapped model throws while starting a subscription whose lease was just won, the lease is given back. The
 * throw is a plain {@link IllegalStateException} here, standing in for anything the wrapped model can throw, and
 * without MongoDB, the same style as {@link CompetingConsumerSubscriptionModelDelegatePauseRefusalTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelDelegateThrowsTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final String SUBSCRIBER_ID = "subscriber";

    private final DelegateThatThrowsOnStart delegate = new DelegateThatThrowsOnStart();
    private final LeaseRecordingStrategy strategy = new LeaseRecordingStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void a_subscribe_the_wrapped_model_throws_on_gives_the_lease_up_and_leaves_the_id_free() {
        strategy.grantOnRegister = true;
        delegate.throwsOnNextStarts.set(1);

        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(strategy.calls).containsExactly("register", "unregister");
        assertThat(strategy.holders).isEmpty();
        assertThatCode(() -> model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).as("the subscription that was refused left nothing behind to refuse the next one").doesNotThrowAnyException();
        assertThat(delegate.subscribed).containsExactly(SUBSCRIPTION_ID);
    }

    @Test
    void a_waiting_consumer_the_wrapped_model_throws_on_when_granted_gives_the_lease_up_and_starts_on_a_later_grant() {
        strategy.grantOnRegister = false;
        model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
        delegate.throwsOnNextStarts.set(1);

        assertThatCode(strategy::grant).as("the strategy granting the lease has nobody to hand the failure to").doesNotThrowAnyException();

        assertThat(strategy.calls).containsExactly("register", "release");
        assertThat(strategy.holders).isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();

        strategy.grant();

        assertThat(delegate.subscribed).containsExactly(SUBSCRIPTION_ID);
        assertThat(strategy.holders).containsExactly(SUBSCRIPTION_ID);
    }

    private static final class DelegateThatThrowsOnStart implements SubscriptionModel {
        private final AtomicInteger throwsOnNextStarts = new AtomicInteger();
        private final List<String> subscribed = new ArrayList<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (throwsOnNextStarts.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("The wrapped model cannot start " + subscriptionId + " right now");
            }
            subscribed.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscribed.remove(subscriptionId);
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
            return subscribed.contains(subscriptionId);
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
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Records every call and which subscriptions hold the lease. {@link #grant()} plays a refresh round granting the
     * lease to {@link #SUBSCRIBER_ID}.
     */
    private static final class LeaseRecordingStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new ArrayList<>();
        private final Set<String> holders = new HashSet<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();
        private boolean grantOnRegister;

        void grant() {
            holders.add(SUBSCRIPTION_ID);
            listeners.forEach(listener -> listener.onConsumeGranted(SUBSCRIPTION_ID, SUBSCRIBER_ID));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register");
            if (grantOnRegister) {
                holders.add(subscriptionId);
            }
            return grantOnRegister;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister");
            if (holders.remove(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release");
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
