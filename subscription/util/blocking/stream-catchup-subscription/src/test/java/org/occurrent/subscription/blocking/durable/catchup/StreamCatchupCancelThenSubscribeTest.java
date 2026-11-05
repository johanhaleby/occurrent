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

package org.occurrent.subscription.blocking.durable.catchup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A cancel of an id marks only the subscribes of the id whose start-position function another thread is asking when the
 * cancel takes the claim. A subscribe made after that waits for the cancel and then goes ahead, as it did before the
 * function was asked ahead of the claim.
 */
@Timeout(60)
@DisplayName("StreamCatchupCancelThenSubscribe")
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupCancelThenSubscribeTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration BOUND = Duration.ofSeconds(30);

    private final HoldsTheCancel live = new HoldsTheCancel();
    private StreamCatchupSubscriptionModel catchup;

    @AfterEach
    void release_the_cancel_and_shut_down() {
        live.release.complete(null);
        if (catchup != null) {
            catchup.shutdown();
        }
    }

    @Nested
    @DisplayName("when a subscribe is made while a cancel of the same id holds the claim")
    class When_a_subscribe_is_made_while_a_cancel_of_the_same_id_holds_the_claim {

        @Test
        @DisplayName("a plain StartAt.now() goes ahead once the cancel returns")
        void a_plain_StartAt_now_goes_ahead_once_the_cancel_returns() {
            subscribeWhileTheCancelHoldsTheClaimAndAssertItGoesAhead(StartAt.now());
        }

        @Test
        @DisplayName("a StartAt.dynamic returning StartAt.now() goes ahead once the cancel returns")
        void a_StartAt_dynamic_returning_StartAt_now_goes_ahead_once_the_cancel_returns() {
            subscribeWhileTheCancelHoldsTheClaimAndAssertItGoesAhead(StartAt.dynamic(() -> StartAt.now()));
        }
    }

    private void subscribeWhileTheCancelHoldsTheClaimAndAssertItGoesAhead(StartAt startAt) {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(live);
        catchup = new StreamCatchupSubscriptionModel(new LiveStartLosingSubscriptionModel(live, false, false, () -> {
        }), eventStore, new CatchupSubscriptionModelConfig(100));
        CompletableFuture<Void> cancel = CompletableFuture.runAsync(() -> catchup.cancelSubscription(SUBSCRIPTION_ID));
        assertThat(live.cancelStarted).as("the cancel holding the claim").succeedsWithin(BOUND);
        AtomicReference<Thread> subscribing = new AtomicReference<>();
        CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> {
            subscribing.set(Thread.currentThread());
            return catchup.subscribe(SUBSCRIPTION_ID, null, startAt, event -> {
            });
        });
        await().atMost(BOUND).until(() -> subscribing.get() != null && LockSupport.getBlocker(subscribing.get()) instanceof AbstractQueuedSynchronizer);

        // When
        live.release.complete(null);

        // Then
        assertThat(cancel).as("the cancel").succeedsWithin(BOUND);
        assertThat(subscribe).as("the subscribe").succeedsWithin(BOUND);
        Subscription subscription = subscribe.join();
        assertAll(
                () -> assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).as("the subscription started").isTrue(),
                () -> assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("the catch-up runs the id").isTrue()
        );
    }

    /**
     * Holds the first cancel until it is released, so the call that cancels holds the claim for its id meanwhile.
     */
    private static final class HoldsTheCancel extends InMemorySubscriptionModel {
        private final CompletableFuture<Void> cancelStarted = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();

        @Override
        public void cancelSubscription(String subscriptionId) {
            cancelStarted.complete(null);
            release.orTimeout(BOUND.toSeconds(), TimeUnit.SECONDS).join();
            super.cancelSubscription(subscriptionId);
        }
    }
}
