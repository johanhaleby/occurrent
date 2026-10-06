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

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * {@code shutdown()} shuts the wrapped model down before the lease strategy, so an event the wrapped model hands over
 * while the lease strategy shuts down goes to the action only with the lease. When the lease strategy throws from its
 * own {@code shutdown()} or {@code removeListener(..)}, {@code shutdown()} still has the wrapped model shut down and
 * throws what the lease strategy threw. When the wrapped model throws from its own {@code shutdown()}, that failure is
 * thrown as it is and the lease strategy is left running, so a model that may still deliver does so only with the lease.
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
    private final List<String> shutdownCalls = new CopyOnWriteArrayList<>();

    @AfterEach
    void shutdown() {
        strategy.releaseShutdown.countDown();
        inMemory.shutdown();
    }

    // The lease strategy's shutdown() blocks, so an event handed over while it does can reach the action only if
    // shutdown() lets it through. The wrapped model is already shut down by then, so it hands over nothing.
    @Test
    void an_event_the_wrapped_model_is_handed_while_the_lease_strategy_shuts_down_does_not_reach_the_action_without_the_lease() throws Exception {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                shutdownCalls.add("wrapped model");
                super.shutdown();
            }
        };
        strategy.shutdownRecorder = shutdownCalls;
        strategy.blockShutdown = true;
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);
        strategy.fenced = true;

        Thread shutdown = Thread.ofPlatform().start(model::shutdown);
        assertThat(strategy.inShutdown.await(5, SECONDS)).as("shutdown() reaches the lease strategy's own shutdown()").isTrue();
        wrapped.accept(List.of(event("e2")));
        Thread.sleep(AFTERWARDS.toMillis());

        assertThat(received).as("[events s1 received while hasLock is false and the lease strategy shuts down]").containsExactly("e1");
        assertThat(shutdownCalls).as("[what shutdown() shut down, in order]").containsExactly("wrapped model", "lease strategy");
        strategy.releaseShutdown.countDown();
        shutdown.join(EVENTUALLY.toMillis());
    }

    @Test
    void a_wrapped_model_that_throws_from_its_own_shutdown_still_delivers_only_with_the_lease_and_leaves_the_lease_strategy_running() throws Exception {
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
        strategy.fenced = true;

        Throwable thrown = catchThrowable(model::shutdown);
        wrapped.accept(List.of(event("e2")));
        Thread.sleep(AFTERWARDS.toMillis());

        try {
            assertThat(received).as("[events s1 received after shutdown() threw, with hasLock false]").containsExactly("e1");
            assertThat(strategy.shutdownCalls.get()).as("calls to the lease strategy's own shutdown()").isZero();
            assertThat(strategy.removeListenerCalls.get()).as("calls to removeListener(..)").isZero();
            assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isZero();
            assertThat(thrown).as("[what shutdown() threw]").isSameAs(failure);
        } finally {
            failing[0] = false;
            strategy.fenced = false;
            model.shutdown();
        }
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

    @Test
    void a_lease_strategy_that_throws_the_same_instance_from_its_own_shutdown_and_from_removeListener_still_has_the_wrapped_model_shut_down() throws Exception {
        IllegalStateException closed = new IllegalStateException("lease strategy closed");
        strategy.shutdownFailure = closed;
        strategy.removeListenerFailure = closed;
        subscribeAndDeliverE1();
        strategy.fenced = true;

        Throwable thrown = catchThrowable(model::shutdown);
        inMemory.accept(List.of(event("e2")));
        Thread.sleep(AFTERWARDS.toMillis());

        assertThat(received).as("[events s1 received once shutdown() has thrown]").containsExactly("e1");
        assertThat(inMemory.subscriptionIds()).as("[subscriptions the wrapped model has once shutdown() has thrown]").isEmpty();
        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isEqualTo(1);
        assertThat(thrown).as("[what shutdown() threw]").isSameAs(closed);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").isEmpty();
    }

    @Test
    void a_wrapped_model_that_throws_from_its_own_shutdown_has_it_thrown_as_it_is_also_when_the_lease_strategy_would_throw_the_same_instance() throws Exception {
        IllegalStateException closed = new IllegalStateException("shared resource closed");
        strategy.shutdownFailure = closed;
        InMemorySubscriptionModel wrapped = shutDownThenThrowing(closed);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);
        strategy.fenced = true;

        Throwable thrown = catchThrowable(model::shutdown);
        wrapped.accept(List.of(event("e2")));
        Thread.sleep(AFTERWARDS.toMillis());

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(closed);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").isEmpty();
        assertThat(received).as("[events s1 received once shutdown() has thrown]").containsExactly("e1");
        assertThat(strategy.shutdownCalls.get()).as("calls to the lease strategy's own shutdown()").isZero();
    }

    @Test
    void an_error_from_the_wrapped_models_own_shutdown_is_thrown_as_it_is_and_the_lease_strategy_is_left_alone() {
        theFailureOfTheWrappedModelIsThrownAloneAndTheLeaseStrategyLeftAlone(new IllegalStateException("lease strategy shutdown failed"), new Error("wrapped model shutdown failed"));
    }

    @Test
    void a_runtime_exception_from_the_wrapped_models_own_shutdown_is_thrown_as_it_is_and_the_lease_strategy_is_left_alone() {
        theFailureOfTheWrappedModelIsThrownAloneAndTheLeaseStrategyLeftAlone(new Error("lease strategy shutdown failed"), new IllegalStateException("wrapped model shutdown failed"));
    }

    private void theFailureOfTheWrappedModelIsThrownAloneAndTheLeaseStrategyLeftAlone(Throwable leaseStrategyFailure, Throwable wrappedModelFailure) {
        strategy.shutdownFailure = leaseStrategyFailure;
        strategy.removeListenerFailure = new IllegalStateException("removeListener failed");
        InMemorySubscriptionModel wrapped = shutDownThenThrowing(wrappedModelFailure);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);

        Throwable thrown = catchThrowable(model::shutdown);

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(wrappedModelFailure);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").isEmpty();
        assertThat(strategy.shutdownCalls.get()).as("calls to the lease strategy's own shutdown()").isZero();
        assertThat(strategy.removeListenerCalls.get()).as("calls to removeListener(..)").isZero();
        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isZero();
    }

    @Test
    void a_checked_failure_of_the_lease_strategys_own_shutdown_is_thrown_as_it_is_with_what_failed_after_it_suppressed() {
        IOException shutdownFailure = new IOException("lease strategy shutdown failed");
        IllegalStateException removeListenerFailure = new IllegalStateException("removeListener failed");
        strategy.shutdownFailure = shutdownFailure;
        strategy.removeListenerFailure = removeListenerFailure;
        subscribeAndDeliverE1();

        Throwable thrown = catchThrowable(model::shutdown);

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(shutdownFailure);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").containsExactly(removeListenerFailure);
    }

    // Shut down before it throws, so it delivers nothing more
    private static InMemorySubscriptionModel shutDownThenThrowing(Throwable failure) {
        return new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                super.shutdown();
                FailingOnShutdown.throwIfSet(failure);
            }
        };
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
        subscribeAndDeliverE1(model, inMemory);
    }

    private void subscribeAndDeliverE1(CompetingConsumerSubscriptionModel model, InMemorySubscriptionModel wrapped) {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();
        wrapped.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received before shutdown()]").containsExactly("e1"));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Grants each lease to the node that asks first, and reports it held unless fenced. Throws what is set from its own
    // shutdown(), from removeListener(..) and from unregisterCompetingConsumer(..), each time it is called. Counts those
    // calls, and can block in its own shutdown().
    private static final class FailingOnShutdown implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final AtomicInteger unregisterAttempts = new AtomicInteger();
        private final AtomicInteger shutdownCalls = new AtomicInteger();
        private final AtomicInteger removeListenerCalls = new AtomicInteger();
        private final CountDownLatch inShutdown = new CountDownLatch(1);
        private final CountDownLatch releaseShutdown = new CountDownLatch(1);
        private volatile boolean blockShutdown;
        private volatile @Nullable List<String> shutdownRecorder;
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
            removeListenerCalls.incrementAndGet();
            throwIfSet(removeListenerFailure);
        }

        @Override
        public void shutdown() {
            shutdownCalls.incrementAndGet();
            List<String> recorder = shutdownRecorder;
            if (recorder != null) {
                recorder.add("lease strategy");
            }
            if (blockShutdown) {
                inShutdown.countDown();
                try {
                    releaseShutdown.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            throwIfSet(shutdownFailure);
        }

        // A checked failure too, which code written in another JVM language can throw
        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void throwIfSet(@Nullable Throwable failure) throws E {
            if (failure != null) {
                throw (E) failure;
            }
        }
    }
}
