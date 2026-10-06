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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Once {@code shutdown()} has begun, every event the wrapped model hands over goes to the action without the lease, so
 * {@code shutdown()} shuts the wrapped model down also when the lease strategy throws from its own {@code shutdown()} or
 * {@code removeListener(..)}. Once it has, it throws what failed, with a failure of the wrapped model's own
 * {@code shutdown()} ahead of what the lease strategy threw.
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
    void a_lease_strategy_and_a_wrapped_model_that_throw_the_same_instance_from_their_own_shutdown_have_it_thrown_as_it_is() throws Exception {
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
    }

    @Test
    void the_failure_of_the_wrapped_models_own_shutdown_is_thrown_ahead_of_a_runtime_exception_from_the_lease_strategy() {
        theFailureOfTheWrappedModelIsThrownFirst(new IllegalStateException("lease strategy shutdown failed"), new Error("wrapped model shutdown failed"));
    }

    @Test
    void the_failure_of_the_wrapped_models_own_shutdown_is_thrown_ahead_of_an_error_from_the_lease_strategy() {
        theFailureOfTheWrappedModelIsThrownFirst(new Error("lease strategy shutdown failed"), new IllegalStateException("wrapped model shutdown failed"));
    }

    private void theFailureOfTheWrappedModelIsThrownFirst(Throwable leaseStrategyFailure, Throwable wrappedModelFailure) {
        IllegalStateException removeListenerFailure = new IllegalStateException("removeListener failed");
        strategy.shutdownFailure = leaseStrategyFailure;
        strategy.removeListenerFailure = removeListenerFailure;
        InMemorySubscriptionModel wrapped = shutDownThenThrowing(wrappedModelFailure);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeAndDeliverE1(model, wrapped);

        Throwable thrown = catchThrowable(model::shutdown);

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(wrappedModelFailure);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").containsExactly(leaseStrategyFailure, removeListenerFailure);
        assertThat(strategy.unregisterAttempts.get()).as("attempts to give up the lease of s1").isZero();
    }

    @Test
    void a_checked_failure_of_the_lease_strategys_own_shutdown_is_thrown_wrapped_with_what_failed_after_it_suppressed_on_the_wrapper() {
        IOException shutdownFailure = new IOException("lease strategy shutdown failed");
        IllegalStateException removeListenerFailure = new IllegalStateException("removeListener failed");
        strategy.shutdownFailure = shutdownFailure;
        strategy.removeListenerFailure = removeListenerFailure;
        subscribeAndDeliverE1();

        Throwable thrown = catchThrowable(model::shutdown);

        assertThat(thrown).as("[what shutdown() threw]").isExactlyInstanceOf(IllegalStateException.class).hasCause(shutdownFailure);
        assertThat(thrown.getSuppressed()).as("[what shutdown() threw has suppressed]").containsExactly(removeListenerFailure);
        assertThat(shutdownFailure.getSuppressed()).as("[what the lease strategy's own shutdown threw has suppressed]").isEmpty();
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

        // A checked failure too, which code written in another JVM language can throw
        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void throwIfSet(@Nullable Throwable failure) throws E {
            if (failure != null) {
                throw (E) failure;
            }
        }
    }
}
