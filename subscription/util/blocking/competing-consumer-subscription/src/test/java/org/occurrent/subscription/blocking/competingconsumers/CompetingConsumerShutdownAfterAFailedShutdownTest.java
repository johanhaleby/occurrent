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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A {@code shutdown()} whose wrapped model throws from its own {@code shutdown()} keeps the leases, and a later
 * {@code shutdown()} whose wrapped model shuts down gives each of them up. A {@code shutdown()} called on a thread of
 * its own while another shuts the lease strategy or the wrapped model down returns at once and throws nothing, and no
 * event goes through without the lease meanwhile. The one under way throws its own failure, and a {@code shutdown()}
 * called after it has ended runs again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerShutdownAfterAFailedShutdownTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    private static final Duration PROMPTLY = Duration.ofSeconds(2);
    private static final Duration AFTERWARDS = Duration.ofMillis(500);

    private final Leases strategy = new Leases();
    private final List<String> received = new CopyOnWriteArrayList<>();

    @Test
    void a_later_shutdown_whose_wrapped_model_shuts_down_gives_up_the_lease_a_failed_one_kept() {
        IllegalStateException failure = new IllegalStateException("wrapped model shutdown failed");
        boolean[] failing = {true};
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                if (failing[0]) {
                    throw failure;
                }
                super.shutdown();
            }
        };
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);

        Throwable thrown = catchThrowable(model::shutdown);
        assertThat(thrown).as("[what the first shutdown() threw]").isSameAs(failure);
        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1 after the first shutdown()").isZero();

        failing[0] = false;
        model.shutdown();

        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1 after the later shutdown()").isEqualTo(1);
        assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    @Test
    void a_shutdown_called_while_one_whose_wrapped_model_throws_is_under_way_returns_promptly_and_throws_nothing_and_a_later_one_gives_the_lease_up() throws Exception {
        IllegalStateException failure = new IllegalStateException("wrapped model shutdown failed");
        CountDownLatch inWrappedShutdown = new CountDownLatch(1);
        CountDownLatch releaseWrappedShutdown = new CountDownLatch(1);
        AtomicInteger wrappedShutdownCalls = new AtomicInteger();
        // Only the first call throws, so a shutdown() that calls it again returns
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                if (wrappedShutdownCalls.incrementAndGet() == 1) {
                    inWrappedShutdown.countDown();
                    try {
                        releaseWrappedShutdown.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw failure;
                }
                super.shutdown();
            }
        };
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);
        strategy.fenced = true;

        AtomicReference<Throwable> thrownByFirst = new AtomicReference<>();
        AtomicReference<Throwable> thrownBySecond = new AtomicReference<>();
        Thread first = Thread.ofPlatform().start(() -> thrownByFirst.set(catchThrowable(model::shutdown)));
        assertThat(inWrappedShutdown.await(EVENTUALLY.toSeconds(), SECONDS)).as("the first shutdown() reaches the wrapped model's own shutdown()").isTrue();
        Thread second = Thread.ofPlatform().start(() -> thrownBySecond.set(catchThrowable(model::shutdown)));
        second.join(PROMPTLY.toMillis());

        try {
            assertThat(second.isAlive()).as("the second shutdown() is still running while the first waits in the wrapped model").isFalse();
            assertThat(first.isAlive()).as("the first shutdown() is still waiting in the wrapped model").isTrue();
            releaseWrappedShutdown.countDown();
            first.join(EVENTUALLY.toMillis());
            assertThat(first.isAlive()).as("the first shutdown() is still running after the wrapped model threw").isFalse();
            wrapped.accept(List.of(event("e2")));
            Thread.sleep(AFTERWARDS.toMillis());

            assertThat(thrownByFirst.get()).as("[what the first shutdown() threw]").isSameAs(failure);
            assertThat(thrownBySecond.get()).as("[what the second shutdown() threw]").isNull();
            assertThat(wrappedShutdownCalls.get()).as("calls to the wrapped model's own shutdown()").isEqualTo(1);
            assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isZero();
            assertThat(received).as("[events s1 received after the first shutdown() threw, with hasLock false]").containsExactly("e1");

            model.shutdown();

            assertThat(wrappedShutdownCalls.get()).as("calls to the wrapped model's own shutdown() once a later shutdown() ran").isEqualTo(2);
            assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1 once a later shutdown() ran").isEqualTo(1);
            assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
        } finally {
            releaseWrappedShutdown.countDown();
            strategy.fenced = false;
            first.join(EVENTUALLY.toMillis());
            model.shutdown();
        }
    }

    // The first shutdown() waits in the lease strategy's own shutdown(), so an event handed over then waits for the
    // lease. The second shutdown() returns at once and must not let it through either.
    @Test
    void a_shutdown_called_while_another_shuts_the_lease_strategy_down_lets_no_event_through_without_the_lease() throws Exception {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none());
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);
        strategy.fenced = true;
        strategy.blocksInShutdown = true;

        Thread first = Thread.ofPlatform().start(model::shutdown);
        Thread[] second = new Thread[1];
        try {
            assertThat(strategy.inShutdown.await(EVENTUALLY.toSeconds(), SECONDS)).as("the first shutdown() reaches the lease strategy's own shutdown()").isTrue();
            wrapped.accept(List.of(event("e2")));
            Thread.sleep(AFTERWARDS.toMillis());
            assertThat(received).as("[events s1 received while the lease strategy shuts down, with hasLock false]").containsExactly("e1");

            second[0] = Thread.ofPlatform().start(model::shutdown);
            second[0].join(PROMPTLY.toMillis());
            assertThat(second[0].isAlive()).as("the second shutdown() is still running while the first waits in the lease strategy").isFalse();
            Thread.sleep(AFTERWARDS.toMillis());
            assertThat(received).as("[events s1 received once a second shutdown() is called, with hasLock false]").containsExactly("e1");
        } finally {
            strategy.releaseShutdown.countDown();
        }
        first.join(EVENTUALLY.toMillis());
        second[0].join(EVENTUALLY.toMillis());

        assertThat(received).as("[events s1 received once both shutdown() calls returned]").containsExactly("e1", "e2");
    }

    private void subscribeAndDeliverE1(CompetingConsumerSubscriptionModel model, InMemorySubscriptionModel wrapped) {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();
        wrapped.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received before shutdown()]").containsExactly("e1"));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Grants each lease to the node that asks first, and reports it held unless fenced. Counts the attempts to give a
    // lease up, and can wait in its own shutdown() until released.
    private static final class Leases implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final AtomicInteger unregisterAttempts = new AtomicInteger();
        private final CountDownLatch inShutdown = new CountDownLatch(1);
        private final CountDownLatch releaseShutdown = new CountDownLatch(1);
        private volatile boolean fenced;
        private volatile boolean blocksInShutdown;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            unregisterAttempts.incrementAndGet();
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
        }

        @Override
        public void shutdown() {
            if (blocksInShutdown) {
                inShutdown.countDown();
                try {
                    releaseShutdown.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
