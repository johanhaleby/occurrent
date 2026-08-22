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
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Covers issue #737, finding 1: a delegate that refuses {@code pauseSubscription} (for example a catch-up
 * subscription whose replay already failed before the delegate ever learned the id, so it throws
 * {@link UnknownSubscriptionException} rather than acting) used to abort the lease release that follows, in
 * {@link CompetingConsumerSubscriptionModel#pauseSubscription(String)}. The node's local state stayed
 * {@code Running} while the lease was never released for another node to take over. Deterministic and without
 * MongoDB, the same style as {@link CompetingConsumerSubscriptionModelPausedWhileWaitingTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelDelegatePauseRefusalTest {

    private static final String SUBSCRIBER_ID = "subscriber";

    private final DelegateThatRefusesPause delegate = new DelegateThatRefusesPause();
    private final RecordingCompetingConsumerStrategy strategy = new RecordingCompetingConsumerStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void a_delegate_refusal_still_releases_the_lease_and_reports_the_consumer_paused() {
        String subscriptionId = "subscriptionId";
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), event -> {
        });
        delegate.refusePauseOf(subscriptionId);

        assertThatCode(() -> model.pauseSubscription(subscriptionId))
                .as("the delegate's refusal must not propagate and abort the state update and lease release below it")
                .doesNotThrowAnyException();

        assertThat(model.isPaused(subscriptionId))
                .as("the local state still has to flip to paused, even though the delegate never learned the id")
                .isTrue();
        assertThat(strategy.calls)
                .as("a user-triggered pause unregisters the competing consumer; this must still run despite the "
                        + "delegate throwing")
                .contains("unregister:" + subscriptionId + ":" + SUBSCRIBER_ID);
    }

    private static final class DelegateThatRefusesPause implements SubscriptionModel {
        private final List<String> refusedIds = new ArrayList<>();

        void refusePauseOf(String subscriptionId) {
            refusedIds.add(subscriptionId);
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
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
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (refusedIds.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
        }
    }

    private record FakeSubscription(String id) implements SubscriptionHandle {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Grants the lock to every registration immediately, so {@code model.subscribe(..)} always leaves the consumer
     * Running rather than Waiting, reaching the branch under test.
     */
    private static final class RecordingCompetingConsumerStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new ArrayList<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register:" + subscriptionId + ":" + subscriberId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister:" + subscriptionId + ":" + subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release:" + subscriptionId + ":" + subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
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
