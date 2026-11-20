/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.internal.UnwantedDelivery;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wrappers that return each other, or themselves, from getWrappedSubscriptionModel keep a walk with no cycle check going
 * for good. The walk that tells whether a stage set on the delivering thread belongs to a model this model wraps stops at
 * the cycle, so an event delivered for the subscription of that stage, which a model outside the cycle set, still
 * reaches the action.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerOverWrappersThatWrapEachOtherTest {

    private static final String NODE = "node";

    private final List<String> reachedTheAction = new CopyOnWriteArrayList<>();
    private final CountDownLatch theActionWasReached = new CountDownLatch(1);
    private final Object modelOutsideTheCycle = new Object();
    private final DirectModel direct = new DirectModel();
    private @Nullable CompetingConsumerSubscriptionModel competing;

    @AfterEach
    void shutdown() {
        if (competing != null) {
            competing.shutdown();
        }
    }

    @Test
    void an_event_delivered_while_a_stage_of_a_model_outside_the_cycle_is_set_reaches_the_action_of_a_competing_subscription_whose_delegate_wraps_itself() throws InterruptedException {
        // Given a competing subscription whose delegate is a wrapper that returns itself
        CyclicWrapper selfWrapping = new CyclicWrapper(direct);
        selfWrapping.wraps(selfWrapping);
        subscribeAndStart(selfWrapping);

        // When an event for the same subscription id is delivered with the stage of a model outside the cycle set on the thread
        deliverOnADaemonThreadWithAStageSet("x", "delivered");

        // Then
        assertThat(theActionWasReached.await(5, TimeUnit.SECONDS)).as("the action was reached within 5 seconds, so the delivery did not spin on the wrapper that wraps itself").isTrue();
        assertThat(reachedTheAction).as("the events that reached the action").containsExactly("delivered");
    }

    @Test
    void an_event_delivered_while_a_stage_of_a_model_outside_the_cycle_is_set_reaches_the_action_of_a_competing_subscription_whose_delegate_is_one_of_two_wrappers_that_wrap_each_other() throws InterruptedException {
        // Given a competing subscription whose delegate is one of two wrappers that return each other
        CyclicWrapper first = new CyclicWrapper(direct);
        CyclicWrapper second = new CyclicWrapper(direct);
        first.wraps(second);
        second.wraps(first);
        subscribeAndStart(first);

        // When an event for the same subscription id is delivered with the stage of a model outside the cycle set on the thread
        deliverOnADaemonThreadWithAStageSet("x", "delivered");

        // Then
        assertThat(theActionWasReached.await(5, TimeUnit.SECONDS)).as("the action was reached within 5 seconds, so the delivery did not spin on the two wrappers that wrap each other").isTrue();
        assertThat(reachedTheAction).as("the events that reached the action").containsExactly("delivered");
    }

    private void subscribeAndStart(SubscriptionModel delegate) {
        competing = new CompetingConsumerSubscriptionModel(delegate, new AlwaysTheLeaseHolder());
        competing.start();
        competing.subscribe(NODE, "x", null, StartAt.now(), event -> {
            reachedTheAction.add(event.getId());
            theActionWasReached.countDown();
        });
    }

    // A daemon thread, so a delivery that never returns doesn't keep the JVM from exiting
    private void deliverOnADaemonThreadWithAStageSet(String subscriptionId, String eventId) {
        Thread delivering = new Thread(() ->
                UnwantedDelivery.run(subscriptionId, modelOutsideTheCycle, new CompletableFuture<>(), () -> direct.deliver(subscriptionId, event(eventId))), "delivering");
        delivering.setDaemon(true);
        delivering.start();
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    private static final class AlwaysTheLeaseHolder implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // Does its work through a model of its own, and reports whichever wrapper it was told to wrap, itself or another
    private static final class CyclicWrapper implements SubscriptionModel, SubscriptionModelWrapper {
        private final SubscriptionModel own;
        private volatile @Nullable SubscriptionModel wrapped;

        private CyclicWrapper(SubscriptionModel own) {
            this.own = own;
        }

        void wraps(SubscriptionModel wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            SubscriptionModel model = wrapped;
            if (model == null) {
                throw new IllegalStateException("Nothing is wrapped");
            }
            return model;
        }

        // The default walks the wrapper chain without a cycle check, which this fixture must not take part in
        @Override
        public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
            return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return own.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public void stop() {
            own.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            own.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return own.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return own.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return own.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return own.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            own.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            own.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            own.shutdown();
        }
    }

    // A subscription model that calls the consumer of a subscription on the thread that calls deliver
    private static final class DirectModel implements SubscriptionModel {
        private final Map<String, Consumer<CloudEvent>> consumers = new ConcurrentHashMap<>();

        void deliver(String subscriptionId, CloudEvent event) {
            Consumer<CloudEvent> consumer = consumers.get(subscriptionId);
            if (consumer == null) {
                throw new IllegalStateException("No consumer for " + subscriptionId);
            }
            consumer.accept(event);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            consumers.put(subscriptionId, action);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
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
            return consumers.containsKey(subscriptionId);
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            consumers.remove(subscriptionId);
        }

        @Override
        public void shutdown() {
        }
    }
}
