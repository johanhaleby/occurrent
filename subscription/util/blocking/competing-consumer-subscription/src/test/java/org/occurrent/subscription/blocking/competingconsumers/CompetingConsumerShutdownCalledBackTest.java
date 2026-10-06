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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code shutdown()} that the wrapped model or the lease strategy calls back while another {@code shutdown()} can wait
 * for the call it runs inside returns at once instead of waiting for the other one. That is a call back from their own
 * {@code shutdown()}, on the thread of the other one, and one from the wrapped model's {@code start(..)} that the other
 * one waits for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerShutdownCalledBackTest {

    private static final Duration PROMPTLY = Duration.ofSeconds(2);
    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    // Long enough for a held event to have reached the action, had it been let through
    private static final Duration WHILE_HELD = Duration.ofMillis(500);

    private final Leases strategy = new Leases();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final AtomicLong millisInTheShutdownCalledBack = new AtomicLong(-1);
    private final AtomicReference<Throwable> thrownByTheShutdownCalledBack = new AtomicReference<>();
    private final AtomicReference<CompetingConsumerSubscriptionModel> model = new AtomicReference<>();
    private final AtomicBoolean calledBack = new AtomicBoolean();

    @Test
    void a_shutdown_the_wrapped_model_calls_back_from_its_own_shutdown_returns_promptly_and_the_one_under_way_gives_the_lease_up() throws Exception {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                callBackOnce();
                super.shutdown();
            }
        };
        subscribeOver(wrapped, strategy);

        Thread shuttingDown = Thread.ofPlatform().daemon().start(() -> model.get().shutdown());
        shuttingDown.join(EVENTUALLY.toMillis());

        assertThat(shuttingDown.isAlive()).as("the shutdown() under way is still waiting").isFalse();
        assertThat(millisInTheShutdownCalledBack.get()).as("milliseconds the shutdown() called back took").isBetween(0L, PROMPTLY.toMillis());
        assertThat(thrownByTheShutdownCalledBack.get()).as("[what the shutdown() called back threw]").isNull();
        assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    @Test
    void a_shutdown_the_lease_strategy_calls_back_from_its_own_shutdown_returns_promptly_and_the_one_under_way_gives_the_lease_up() throws Exception {
        Leases callingBack = new Leases() {
            @Override
            public void shutdown() {
                callBackOnce();
            }
        };
        subscribeOver(new InMemorySubscriptionModel(RetryStrategy.none()), callingBack);

        Thread shuttingDown = Thread.ofPlatform().daemon().start(() -> model.get().shutdown());
        shuttingDown.join(EVENTUALLY.toMillis());

        assertThat(shuttingDown.isAlive()).as("the shutdown() under way is still waiting").isFalse();
        assertThat(millisInTheShutdownCalledBack.get()).as("milliseconds the shutdown() called back took").isBetween(0L, PROMPTLY.toMillis());
        assertThat(thrownByTheShutdownCalledBack.get()).as("[what the shutdown() called back threw]").isNull();
        assertThat(callingBack.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    @Test
    void a_shutdown_called_back_from_a_wrapped_model_that_then_fails_to_shut_down_lets_no_held_event_through_and_keeps_the_lease() throws Exception {
        IllegalStateException failure = new IllegalStateException("wrapped model shutdown failed");
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                callBackOnce();
                throw failure;
            }
        };
        subscribeOver(wrapped, strategy);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread shuttingDown = Thread.ofPlatform().daemon().start(() -> {
            try {
                model.get().shutdown();
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        shuttingDown.join(EVENTUALLY.toMillis());

        assertThat(shuttingDown.isAlive()).as("the shutdown() under way is still waiting").isFalse();
        assertThat(thrown.get()).as("[what the shutdown() under way threw]").isSameAs(failure);
        assertThat(millisInTheShutdownCalledBack.get()).as("milliseconds the shutdown() called back took").isBetween(0L, PROMPTLY.toMillis());
        assertThat(thrownByTheShutdownCalledBack.get()).as("[what the shutdown() called back threw]").isNull();
        assertThat(strategy.holders).as("[subscriptions with a lease held]").containsOnlyKeys("s1");

        strategy.reportsNoLeaseHeld = true;
        wrapped.accept(List.of(event("e2")));
        Thread.sleep(WHILE_HELD.toMillis());
        assertThat(received).as("[events the action received]").containsExactly("e1");
    }

    @Test
    void a_shutdown_the_wrapped_model_calls_back_from_its_own_start_returns_promptly_and_lets_another_that_waits_for_that_start_shut_it_down_once() throws Exception {
        CountDownLatch inStart = new CountDownLatch(1);
        CountDownLatch otherShutdownWaits = new CountDownLatch(1);
        AtomicBoolean callsBackOnStart = new AtomicBoolean();
        AtomicLong shutDowns = new AtomicLong();
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void start(boolean resumeSubscriptionsAutomatically) {
                super.start(resumeSubscriptionsAutomatically);
                if (callsBackOnStart.compareAndSet(true, false)) {
                    inStart.countDown();
                    try {
                        otherShutdownWaits.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    callBackOnce();
                }
            }

            @Override
            public void shutdown() {
                shutDowns.incrementAndGet();
                super.shutdown();
            }
        };
        subscribeOver(wrapped, strategy);
        model.get().stop();
        callsBackOnStart.set(true);
        Thread starting = Thread.ofPlatform().daemon().start(() -> model.get().start());
        assertThat(inStart.await(EVENTUALLY.toSeconds(), TimeUnit.SECONDS)).as("start() starts the wrapped model").isTrue();

        Thread shuttingDown = Thread.ofPlatform().daemon().start(() -> model.get().shutdown());
        // The only wait before the wrapped model's own shutdown() is the one for the start
        await().atMost(EVENTUALLY).until(() -> shuttingDown.getState() == Thread.State.WAITING);
        otherShutdownWaits.countDown();
        shuttingDown.join(EVENTUALLY.toMillis());
        starting.join(EVENTUALLY.toMillis());

        assertThat(shuttingDown.isAlive()).as("the other shutdown() is still waiting").isFalse();
        assertThat(millisInTheShutdownCalledBack.get()).as("milliseconds the shutdown() called back took").isBetween(0L, PROMPTLY.toMillis());
        assertThat(thrownByTheShutdownCalledBack.get()).as("[what the shutdown() called back threw]").isNull();
        assertThat(shutDowns.get()).as("times the wrapped model was shut down").isEqualTo(1);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty());
    }

    private void subscribeOver(InMemorySubscriptionModel wrapped, CompetingConsumerStrategy leases) {
        CompetingConsumerSubscriptionModel made = new CompetingConsumerSubscriptionModel(wrapped, leases);
        model.set(made);
        made.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), event -> received.add(event.getId())).waitUntilStarted();
        wrapped.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).until(() -> received.contains("e1"));
    }

    private void callBackOnce() {
        if (!calledBack.compareAndSet(false, true)) {
            return;
        }
        long began = System.nanoTime();
        try {
            model.get().shutdown();
        } catch (Throwable e) {
            thrownByTheShutdownCalledBack.set(e);
        }
        millisInTheShutdownCalledBack.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Grants each lease to the node that asks first, and reports it held while that node has it, unless told otherwise
    private static class Leases implements CompetingConsumerStrategy {
        final Map<String, String> holders = new ConcurrentHashMap<>();
        volatile boolean reportsNoLeaseHeld;

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
            return !reportsNoLeaseHeld && subscriberId.equals(holders.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
        }
    }
}
