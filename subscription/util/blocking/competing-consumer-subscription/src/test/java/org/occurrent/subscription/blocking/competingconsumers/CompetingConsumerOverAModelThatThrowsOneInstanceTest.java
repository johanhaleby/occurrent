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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A wrapped model can throw one exception instance for every failure. When two of its calls fail while one call of this
 * model handles them, that call throws the instance as it is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerOverAModelThatThrowsOneInstanceTest {

    private final IllegalStateException closed = new IllegalStateException("wrapped model closed");
    private final ThrowingOneInstance wrapped = new ThrowingOneInstance(closed);
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, new GrantingTheFirstToAsk());

    @AfterEach
    void shutdown() {
        wrapped.failing = false;
        wrapped.failingToSayWhetherItRuns = false;
        model.shutdown();
    }

    @Test
    void a_cancel_that_fails_and_then_cannot_find_out_whether_the_wrapped_model_holds_the_subscription_throws_that_failure() {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}).waitUntilStarted();
        wrapped.failing = true;
        wrapped.failingToSayWhetherItRuns = true;

        Throwable thrown = catchThrowable(() -> model.cancelSubscription("s1"));

        assertThat(thrown).as("[what cancelSubscription(..) threw]").isSameAs(closed);
    }

    @Test
    void a_stop_that_fails_to_pause_two_subscriptions_with_the_same_instance_throws_that_instance() {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}).waitUntilStarted();
        model.subscribe("node", "s2", null, StartAt.subscriptionModelDefault(), __ -> {}).waitUntilStarted();
        wrapped.failing = true;

        Throwable thrown = catchThrowable(model::stop);

        assertThat(thrown).as("[what stop() threw]").isSameAs(closed);
    }

    // While failing, throws the one instance from a cancel and a pause, and keeps every subscription running through its
    // own stop(), so a stop() of the competing model goes on to pause each one. Throws it from a check of whether a
    // subscription runs too, while failing to say.
    private static final class ThrowingOneInstance extends InMemorySubscriptionModel {
        private final IllegalStateException failure;
        private volatile boolean failing;
        private volatile boolean failingToSayWhetherItRuns;

        private ThrowingOneInstance(IllegalStateException failure) {
            super(RetryStrategy.none());
            this.failure = failure;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            throwIfFailing();
            super.cancelSubscription(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (failingToSayWhetherItRuns) {
                throw failure;
            }
            return super.isRunning(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throwIfFailing();
            super.pauseSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            if (!failing) {
                super.stop();
            }
        }

        private void throwIfFailing() {
            if (failing) {
                throw failure;
            }
        }
    }

    private static final class GrantingTheFirstToAsk implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
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
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
        }
    }
}
