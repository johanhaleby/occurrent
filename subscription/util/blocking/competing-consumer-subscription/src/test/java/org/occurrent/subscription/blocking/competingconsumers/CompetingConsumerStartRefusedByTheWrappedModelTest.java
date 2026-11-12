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
import ch.qos.logback.core.AppenderBase;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A wrapped model that reports through {@link StartRefusalReportingSubscriptions} that it refused the start of a
 * subscription after {@code subscribe} returned. The competing consumer it refused is forgotten, gives its lease back
 * and frees its id, and its handle throws the refusal. A report for a consumer the wrapped model never started, or for
 * an id this model has no competing consumer for, changes nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerStartRefusedByTheWrappedModelTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration EVENTUALLY = Duration.ofSeconds(10);

    private final Logger modelLogger = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);
    private final LoggedEvents logged = new LoggedEvents();
    private WrappedModel wrapped;
    private Strategy strategy;
    private CompetingConsumerSubscriptionModel model;

    @BeforeEach
    void create_instances() {
        wrapped = new WrappedModel();
        strategy = new Strategy();
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        logged.start();
        modelLogger.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        modelLogger.detachAppender(logged);
        model.shutdown();
    }

    @Test
    void a_refusal_reported_while_the_wrapped_model_subscribes_is_acted_on_once_the_subscribe_returns() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.whileSubscribing.set(wrapped::refuse);

        // When
        Subscription subscription = subscribe();

        // Then
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void a_refusal_reported_for_a_consumer_waiting_for_its_lease_leaves_it_waiting() {
        // Given
        Subscription subscription = subscribe();

        // When
        wrapped.report(ID);

        // Then
        AtomicBoolean started = new AtomicBoolean();
        assertThatCode(() -> started.set(subscription.waitUntilStarted(Duration.ofMillis(100)))).as("handle of the waiting consumer").doesNotThrowAnyException();
        assertThat(started).as("handle of the waiting consumer answers").isTrue();
        assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID);
        grantTheLease();
        assertThat(wrapped.isRunning(ID)).as("running in the wrapped model once granted the lease").isTrue();
        assertThat(model.isRunning(ID)).as("running once granted the lease").isTrue();
        assertThat(errorsLogged()).as("errors logged").isEmpty();
    }

    @Test
    void a_refusal_reported_for_an_unknown_id_or_one_that_does_not_compete_changes_nothing() {
        // Given a consumer the wrapped model no longer has, whose refusal is not reported yet
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        model.subscribe(NODE, "plain", null, StartAt.dynamic(context -> context.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault()), __ -> {
        });
        wrapped.drop(ID);

        // When
        wrapped.report("unknown");
        wrapped.report("plain");

        // Then
        assertAll(
                () -> assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("handle of the consumer").doesNotThrowAnyException(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").contains(ID),
                () -> assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID),
                () -> assertThat(wrapped.isRunning("plain")).as("non-competing subscription running in the wrapped model").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );

        // And the report for the consumer's own id is acted on
        wrapped.report(ID);
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void a_refusal_reported_for_a_consumer_paused_by_the_user_after_it_ran_forgets_it() {
        // Given
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.drop(ID);
        model.pauseSubscription(ID);
        assertThat(model.isPaused(ID)).as("paused by the user").isTrue();

        // When
        wrapped.report(ID);

        // Then
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void the_id_of_a_refused_consumer_can_be_subscribed_again_by_the_same_subscriber_while_the_old_handle_still_throws() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.whileSubscribing.set(wrapped::refuse);
        Subscription refused = subscribe();
        assertRefusedAndForgotten(refused);

        // When
        Subscription again = subscribe();

        // Then
        AtomicBoolean started = new AtomicBoolean();
        assertThatCode(() -> started.set(again.waitUntilStarted(Duration.ofMillis(100)))).as("new handle").doesNotThrowAnyException();
        assertAll(
                () -> assertThat(started).as("new handle started").isTrue(),
                () -> assertThatThrownBy(() -> refused.waitUntilStarted(Duration.ofMillis(100))).as("old handle").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID)
        );
    }

    @Test
    void shutdown_removes_the_listener_from_the_wrapped_model() {
        // Given
        assertThat(wrapped.listeners).as("listeners added by the competing consumer model").hasSize(1);

        // When
        model.shutdown();

        // Then
        assertThat(wrapped.listeners).as("listeners left after shutdown").isEmpty();
    }

    private Subscription subscribe() {
        return model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // What the strategy does when it hands the lease to this node, on a thread of its own in production
    private void grantTheLease() {
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
    }

    private void assertRefusedAndForgotten(Subscription subscription) {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThatThrownBy(() -> subscription.waitUntilStarted(Duration.ofMillis(100)))
                .as("handle of the refused consumer").isInstanceOf(UnsupportedStartAtException.class));
        assertAll(
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").singleElement().asString().contains("subscriptionId=" + ID)
        );
    }

    private List<String> errorsLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }

    // Grants the lease on register when told to, and otherwise the consumer waits for a grant the test gives
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private volatile boolean grantOnRegister;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // Refuses a start the way a MongoDB model does once subscribe has returned, by dropping the subscription and then
    // telling its listeners. A test can also drop without telling, or tell without dropping.
    private static final class WrappedModel implements SubscriptionModel, StartRefusalReportingSubscriptions {
        private final Set<String> runningIds = ConcurrentHashMap.newKeySet();
        private final Set<String> pausedIds = ConcurrentHashMap.newKeySet();
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();
        // Run once, by the next subscribe before it returns
        private final AtomicReference<@Nullable Consumer<String>> whileSubscribing = new AtomicReference<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            runningIds.add(subscriptionId);
            @Nullable Consumer<String> during = whileSubscribing.getAndSet(null);
            if (during != null) {
                during.accept(subscriptionId);
            }
            return new WrappedSubscription(subscriptionId);
        }

        private void refuse(String subscriptionId) {
            drop(subscriptionId);
            report(subscriptionId);
        }

        private void drop(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        private void report(String subscriptionId) {
            listeners.forEach(listener -> listener.handleStartRefusal(subscriptionId, new UnsupportedStartAtException(StartAt.subscriptionModelDefault(), "Refused the start of " + subscriptionId)));
        }

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!pausedIds.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            drop(subscriptionId);
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
            return runningIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
