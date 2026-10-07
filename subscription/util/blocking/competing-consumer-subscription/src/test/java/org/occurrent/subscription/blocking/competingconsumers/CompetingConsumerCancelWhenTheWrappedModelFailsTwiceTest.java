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
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A cancel that takes effect in the wrapped model and then throws, when the wrapped model then fails to say whether it
 * still holds the subscription with an {@code Error}, throws its own failure with that {@code Error} suppressed on it,
 * and gives up the lease of a subscription nothing runs any more.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerCancelWhenTheWrappedModelFailsTwiceTest {

    private final IllegalStateException cancelFailure = new IllegalStateException("cancel failed after taking effect");
    private final Error isRunningFailure = new Error("isRunning failed");
    private final GrantingTheFirstToAsk strategy = new GrantingTheFirstToAsk();
    private final FailingAfterTheCancelTookEffect wrapped = new FailingAfterTheCancelTookEffect(cancelFailure, isRunningFailure);
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);

    @AfterEach
    void shutdown() {
        wrapped.failing = false;
        model.shutdown();
    }

    @Test
    void a_cancel_that_took_effect_then_failed_and_cannot_find_out_whether_the_wrapped_model_holds_the_subscription_with_an_error_throws_its_own_failure_and_gives_up_the_lease() throws Exception {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}).waitUntilStarted();
        wrapped.failing = true;

        Throwable thrown = catchThrowable(() -> model.cancelSubscription("s1"));
        wrapped.failing = false;

        assertThat(thrown).as("[what cancelSubscription(..) threw]").isSameAs(cancelFailure);
        assertThat(thrown.getSuppressed()).as("[what cancelSubscription(..) threw has suppressed]").containsOnly(isRunningFailure);
        assertThat(strategy.gaveUpS1.await(10, SECONDS)).as("the lease of s1 is given up").isTrue();
        assertThat(wrapped.subscriptionIds()).as("[subscriptions the wrapped model has after the cancel]").isEmpty();
    }

    // Cancels the subscription, and then throws from the cancel and from each check of whether a subscription runs
    // while failing
    private static final class FailingAfterTheCancelTookEffect extends InMemorySubscriptionModel {
        private final IllegalStateException cancelFailure;
        private final Error isRunningFailure;
        private volatile boolean failing;

        private FailingAfterTheCancelTookEffect(IllegalStateException cancelFailure, Error isRunningFailure) {
            super(RetryStrategy.none());
            this.cancelFailure = cancelFailure;
            this.isRunningFailure = isRunningFailure;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            super.cancelSubscription(subscriptionId);
            if (failing) {
                throw cancelFailure;
            }
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (failing) {
                throw isRunningFailure;
            }
            return super.isRunning(subscriptionId);
        }
    }

    // Grants each lease to the node that asks first, and counts down once the lease of s1 is given up
    private static final class GrantingTheFirstToAsk implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final CountDownLatch gaveUpS1 = new CountDownLatch(1);

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            giveUp(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            giveUp(subscriptionId, subscriberId);
        }

        private void giveUp(String subscriptionId, String subscriberId) {
            if (holders.remove(subscriptionId, subscriberId) && subscriptionId.equals("s1")) {
                gaveUpS1.countDown();
            }
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
