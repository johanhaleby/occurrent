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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Once {@code shutdown()} has begun, every event the wrapped model hands over goes to the action without the lease, so
 * {@code shutdown()} shuts the wrapped model down also when the lease strategy throws from its own {@code shutdown()} or
 * {@code removeListener(..)}, and throws what the lease strategy threw once it has.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerShutdownOverAFailingLeaseStrategyTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Far longer than an event the wrapped model hands over after shutdown() has begun takes to reach the action
    private static final Duration AFTERWARDS = Duration.ofMillis(500);

    private final FailingOnShutdown strategy = new FailingOnShutdown();
    private final InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
    private final List<String> received = new CopyOnWriteArrayList<>();

    @AfterEach
    void shutdown() {
        inMemory.shutdown();
    }

    @Test
    void a_lease_strategy_that_throws_a_runtime_exception_from_its_own_shutdown_still_has_the_wrapped_model_shut_down() throws Exception {
        theLeaseStrategyThrowsFromItsOwnShutdown(new IllegalStateException("lease strategy shutdown failed"));
    }

    @Test
    void a_lease_strategy_that_throws_an_error_from_its_own_shutdown_still_has_the_wrapped_model_shut_down() throws Exception {
        theLeaseStrategyThrowsFromItsOwnShutdown(new Error("lease strategy shutdown failed"));
    }

    @Test
    void the_failure_of_the_lease_strategys_own_shutdown_is_thrown_with_what_failed_after_it_suppressed_also_when_giving_up_every_lease_keeps_throwing() throws Exception {
        IllegalStateException shutdownFailure = new IllegalStateException("lease strategy shutdown failed");
        IllegalStateException removeListenerFailure = new IllegalStateException("removeListener failed");
        strategy.shutdownFailure = shutdownFailure;
        strategy.removeListenerFailure = removeListenerFailure;
        strategy.unregisterFailure = new Error("unregister failed");
        subscribeAndDeliverE1();

        Throwable thrown = catchThrowable(model::shutdown);

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(shutdownFailure);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw after the lease strategy's own shutdown threw]").containsExactly(removeListenerFailure);
        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isEqualTo(1);
        assertThat(inMemory.subscriptionIds()).as("[subscriptions the wrapped model has once shutdown() has thrown]").isEmpty();
    }

    // The lease strategy no longer reports the lease of s1 held, so an e2 the wrapped model hands over reaches the action
    // only because shutdown() has begun
    private void theLeaseStrategyThrowsFromItsOwnShutdown(Throwable failure) throws Exception {
        strategy.shutdownFailure = failure;
        subscribeAndDeliverE1();
        strategy.fenced = true;

        Throwable thrown = catchThrowable(model::shutdown);
        inMemory.accept(List.of(event("e2")));
        Thread.sleep(AFTERWARDS.toMillis());

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(failure);
        assertThat(received).as("[events s1 received once shutdown() has thrown %s]", failure).containsExactly("e1");
        assertThat(inMemory.subscriptionIds()).as("[subscriptions the wrapped model has once shutdown() has thrown %s]", failure).isEmpty();
    }

    private void subscribeAndDeliverE1() {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();
        inMemory.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received before shutdown()]").containsExactly("e1"));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Grants each lease to the node that asks first, and reports it held unless fenced. Throws what is set from its own
    // shutdown(), from removeListener(..) and from unregisterCompetingConsumer(..), each time it is called.
    private static final class FailingOnShutdown implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final AtomicInteger unregisterAttempts = new AtomicInteger();
        private volatile boolean fenced;
        private volatile @Nullable Throwable shutdownFailure;
        private volatile @Nullable Throwable removeListenerFailure;
        private volatile @Nullable Throwable unregisterFailure;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            unregisterAttempts.incrementAndGet();
            throwIfSet(unregisterFailure);
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return !fenced && subscriberId.equals(holders.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
            throwIfSet(removeListenerFailure);
        }

        @Override
        public void shutdown() {
            throwIfSet(shutdownFailure);
        }

        private static void throwIfSet(@Nullable Throwable failure) {
            if (failure instanceof Error error) {
                throw error;
            } else if (failure != null) {
                throw (RuntimeException) failure;
            }
        }
    }
}
