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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerLosesNoHeldEventTest.FenceStrategy;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * What an event that waits for the lease and a lease that is lost do over a wrapped model that redelivers an event its
 * action withdrew. The wrapped model here delivers on the thread that calls it, so each case decides which thread holds
 * the event and what the wrapped model is asked meanwhile.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerOverARedeliveringModelTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration QUIET_FOR = Duration.ofMillis(600);

    private final FenceStrategy strategy = new FenceStrategy();
    private final Redelivering wrapped = new Redelivering();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final ExecutorService threads = Executors.newCachedThreadPool();
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);
    private CompetingConsumerSubscriptionModel model;

    @BeforeEach
    void captureTheLog() {
        logged.start();
        modelLog.addAppender(logged);
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
    }

    @AfterEach
    void shutdown() {
        wrapped.shutdownFails = false;
        strategy.fenced = false;
        model.shutdown();
        threads.shutdownNow();
        modelLog.detachAppender(logged);
    }

    @Test
    void an_event_the_wrapped_model_delivers_inside_a_call_this_model_makes_into_it_is_let_through_with_a_warning_instead_of_held() {
        // Given, with the lease closing as the wrapped model starts to deliver, which nobody is told of
        wrapped.deliverWhenSubscribed = event("e1");
        wrapped.beforeDelivering = () -> strategy.fenced = true;

        // When
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), action()), threads);

        // Then, with e1 delivered on the thread that calls the wrapped model, which can't return while e1 waits
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received, e1 being delivered inside a call into the wrapped model while this node doesn't hold the lease]").containsExactly("e1"));
        assertThat(subscribing).as("[subscribe, which the wrapped model delivers e1 inside of]").succeedsWithin(EVENTUALLY);
        assertThat(wrapped.withdrawn).as("[events withdrawn]").isEmpty();
        assertThat(warnings()).as("[warnings for e1, let through without the lease]").singleElement().extracting(ILoggingEvent::getFormattedMessage).asString()
                .contains("Delivering an event without the lease", "the thread that delivers it is inside a call this model makes into the wrapped model", "subscriberId=node", "subscriptionId=s1");
    }

    @Test
    void an_event_that_waits_for_the_lease_is_let_through_with_a_warning_once_this_model_has_forgotten_the_subscription() throws Exception {
        // Given
        subscribed();
        strategy.fenced = true;
        CompletableFuture<Void> delivered = deliverOnAThreadOfItsOwn("e1");
        assertThat(strategy.askedWithoutTheLease.await(EVENTUALLY.toSeconds(), SECONDS)).as("e1 waits for the lease").isTrue();
        wrapped.cancelFailsAfterForgetting = true;

        // When
        assertThatThrownBy(() -> model.cancelSubscription("s1")).hasMessageContaining("cancel failed");

        // Then
        assertThat(delivered).as("[delivery of e1, which waits for the lease of a subscription this model has forgotten]").succeedsWithin(EVENTUALLY);
        assertThat(received).as("[events s1 received]").containsExactly("e1");
        assertThat(wrapped.withdrawn).as("[events withdrawn]").isEmpty();
        assertThat(warnings()).as("[warnings for e1, let through without the lease]").singleElement().extracting(ILoggingEvent::getFormattedMessage).asString()
                .contains("Delivering an event without the lease", "this model has forgotten the subscription", "subscriberId=node", "subscriptionId=s1");
    }

    @Test
    void an_event_waits_for_the_lease_again_after_the_wrapped_model_failed_to_shut_down() throws Exception {
        // Given
        subscribed();
        wrapped.shutdownFails = true;
        assertThatThrownBy(model::shutdown).hasMessageContaining("shutdown failed");
        strategy.fenced = true;

        // When
        CompletableFuture<Void> delivered = deliverOnAThreadOfItsOwn("e2");

        // Then, with the wrapped model still delivering, so e2 is held for the lease and not withdrawn as at a shutdown
        assertThat(strategy.askedWithoutTheLease.await(EVENTUALLY.toSeconds(), SECONDS)).as("e2 waits for the lease, since the wrapped model may still deliver").isTrue();
        assertThat(wrapped.withdrawn).as("[events withdrawn while e2 waited]").isEmpty();
        strategy.fenced = false;
        assertThat(delivered).as("[delivery of e2, once the lease is held]").succeedsWithin(EVENTUALLY);
        assertThat(received).as("[events s1 received]").containsExactly("e2");
    }

    @Test
    void an_event_that_waits_for_the_lease_is_delivered_once_it_is_back_when_the_wrapped_model_had_stopped_running_the_subscription_as_it_was_lost() throws Exception {
        // Given, with the wrapped model no longer running s1 though e1 is still in its action
        subscribed();
        strategy.fenced = true;
        CompletableFuture<Void> delivered = deliverOnAThreadOfItsOwn("e1");
        assertThat(strategy.askedWithoutTheLease.await(EVENTUALLY.toSeconds(), SECONDS)).as("e1 waits for the lease").isTrue();
        wrapped.reportsNotRunning = true;

        // When the lease is lost, so no pause is owed for a run that is not there, and then comes back
        strategy.transfer("s1", "node", "other-node");
        Thread.sleep(QUIET_FOR.toMillis());
        strategy.fenced = false;
        strategy.transfer("s1", "other-node", "node");

        // Then
        assertThat(delivered).as("[delivery of e1, once the lease is back. A prohibition owed for a subscription the wrapped model didn't run keeps e1 waiting]").succeedsWithin(EVENTUALLY);
        assertThat(received).as("[events s1 received]").containsExactly("e1");
        assertThat(wrapped.withdrawn).as("[events withdrawn]").isEmpty();
    }

    private void subscribed() {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), action()).waitUntilStarted();
    }

    private Consumer<CloudEvent> action() {
        return e -> received.add(e.getId());
    }

    private CompletableFuture<Void> deliverOnAThreadOfItsOwn(String id) {
        return CompletableFuture.runAsync(() -> {
            strategy.deliveringThread = Thread.currentThread();
            wrapped.deliver("s1", event(id));
        }, threads);
    }

    private List<ILoggingEvent> warnings() {
        synchronized (logged) {
            return new ArrayList<>(logged.list).stream().filter(e -> e.getLevel() == Level.WARN).toList();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Delivers on the thread that calls deliver, and treats AttemptNotMade from the action as the models that redeliver
    // do, which is to note that the event was not delivered.
    private static final class Redelivering implements SubscriptionModel, RedeliveringSubscriptions {
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final List<String> withdrawn = new CopyOnWriteArrayList<>();
        private volatile boolean started = true;
        volatile boolean shutdownFails;
        volatile boolean reportsNotRunning;
        volatile boolean cancelFailsAfterForgetting;
        volatile @Nullable CloudEvent deliverWhenSubscribed;
        volatile Runnable beforeDelivering = () -> { };

        void deliver(String subscriptionId, CloudEvent event) {
            Consumer<CloudEvent> action = actions.get(subscriptionId);
            try {
                action.accept(event);
            } catch (AttemptNotMade notMade) {
                withdrawn.add(event.getId());
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            running.add(subscriptionId);
            deliverTheEventForTheCall(subscriptionId);
            return subscription(subscriptionId);
        }

        // Once, on the thread of the call that runs the subscription
        private void deliverTheEventForTheCall(String subscriptionId) {
            CloudEvent event = deliverWhenSubscribed;
            deliverWhenSubscribed = null;
            if (event != null) {
                beforeDelivering.run();
                deliver(subscriptionId, event);
            }
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            reportsNotRunning = false;
            running.add(subscriptionId);
            deliverTheEventForTheCall(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running.remove(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
            running.remove(subscriptionId);
            if (cancelFailsAfterForgetting) {
                throw new IllegalStateException("cancel failed");
            }
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            started = true;
        }

        @Override
        public boolean isRunning() {
            return started;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return !reportsNotRunning && running.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !reportsNotRunning && actions.containsKey(subscriptionId) && !running.contains(subscriptionId);
        }

        @Override
        public void shutdown() {
            if (shutdownFails) {
                throw new IllegalStateException("shutdown failed");
            }
            started = false;
        }

        @Override
        public boolean redeliversAnAttemptNotMade() {
            return true;
        }

        private static Subscription subscription(String id) {
            return new Subscription() {
                @Override
                public String id() {
                    return id;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
