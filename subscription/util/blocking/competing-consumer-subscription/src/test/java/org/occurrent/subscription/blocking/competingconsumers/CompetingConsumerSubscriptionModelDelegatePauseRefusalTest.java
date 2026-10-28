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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.SubscriptionRefusedException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers issue #737, finding 1: a delegate that refuses {@code pauseSubscription} (for example a catch-up
 * subscription whose replay already failed before the delegate ever learned the id, so it throws
 * {@link UnknownSubscriptionException} rather than acting) used to abort the lease release that follows, in
 * {@link CompetingConsumerSubscriptionModel#pauseSubscription(String)}. The node's local state stayed
 * {@code Running} while the lease was never released for another node to take over. Deterministic and without
 * MongoDB, the same style as {@link CompetingConsumerSubscriptionModelPausedWhileWaitingTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelDelegatePauseRefusalTest {

    private static final String SUBSCRIBER_ID = "subscriber";
    private static final String SUBSCRIPTION_ID = "subscriptionId";

    private final DelegateThatRefusesPause delegate = new DelegateThatRefusesPause();
    private final RecordingCompetingConsumerStrategy strategy = new RecordingCompetingConsumerStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
    private final ListAppender<ILoggingEvent> logEvents = new ListAppender<>();
    private final Logger modelLogger = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    @BeforeEach
    void capture_what_the_model_logs() {
        logEvents.start();
        modelLogger.addAppender(logEvents);
    }

    @AfterEach
    void stop_capturing_what_the_model_logs() {
        modelLogger.detachAppender(logEvents);
        logEvents.stop();
    }

    @Test
    void a_delegate_refusal_still_releases_the_lease_and_reports_the_consumer_paused() {
        String subscriptionId = "subscriptionId";
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), event -> {
        });
        delegate.refusePauseOf(subscriptionId);

        assertThatCode(() -> model.pauseSubscription(subscriptionId))
                .as("the delegate's refusal must not propagate and abort the state update and lease release below it")
                .doesNotThrowAnyException();

        assertThat(model.isPaused(subscriptionId))
                .as("the local state still has to flip to paused, even though the delegate never learned the id")
                .isTrue();
        assertThat(strategy.calls)
                .as("a user-triggered pause unregisters the competing consumer; this must still run despite the "
                        + "delegate throwing")
                .contains("unregister:" + subscriptionId + ":" + SUBSCRIBER_ID);
    }

    // The wrapped model refusing the pause means it doesn't know the subscription or doesn't run it, so there is
    // nothing to pause there. Returning normally and giving up the lease is correct, since keeping it would leave this node
    // holding a lease that no subscription on it serves and no other node could take over.

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    @DisplayName("a user pause refused by the wrapped model returns normally, pauses the consumer and unregisters it")
    void a_user_pause_refused_by_the_wrapped_model_returns_normally_pauses_the_consumer_and_unregisters_it(SubscriptionRefusedException refusal) {
        // Given
        subscribeAndRefusePause(refusal);

        // When
        Throwable thrown = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));

        // Then
        assertAll(
                () -> assertThat(thrown).isNull(),
                () -> assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue(),
                () -> assertThat(strategy.calls).contains("unregister:" + SUBSCRIPTION_ID + ":" + SUBSCRIBER_ID)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    @DisplayName("a user pause refused by the wrapped model logs one warning naming the subscription, the subscriber and the refusal")
    void a_user_pause_refused_by_the_wrapped_model_logs_one_warning_naming_the_subscription_the_subscriber_and_the_refusal(SubscriptionRefusedException refusal) {
        // Given
        subscribeAndRefusePause(refusal);

        // When
        model.pauseSubscription(SUBSCRIPTION_ID);

        // Then
        assertThat(warnings()).singleElement().satisfies(warning -> assertAll(
                // By position, since the refusal's message holds the subscription id too
                () -> assertThat(warning.getArgumentArray()).startsWith(SUBSCRIPTION_ID, refusal.getClass().getName(), refusal.getMessage(), SUBSCRIBER_ID, true),
                () -> assertThat(warning.getThrowableProxy().getClassName()).isEqualTo(refusal.getClass().getName()),
                () -> assertThat(warning.getThrowableProxy().getMessage()).isEqualTo(refusal.getMessage())
        ));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    @DisplayName("a pause after a lost lease refused by the wrapped model pauses the consumer and releases the lease")
    void a_pause_after_a_lost_lease_refused_by_the_wrapped_model_pauses_the_consumer_and_releases_the_lease(SubscriptionRefusedException refusal) {
        // Given
        subscribeAndRefusePause(refusal);

        // When
        Throwable thrown = catchThrowable(() -> model.onConsumeProhibited(SUBSCRIPTION_ID, SUBSCRIBER_ID));

        // Then
        assertAll(
                () -> assertThat(thrown).isNull(),
                () -> assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue(),
                () -> assertThat(strategy.calls).contains("release:" + SUBSCRIPTION_ID + ":" + SUBSCRIBER_ID),
                () -> assertThat(strategy.calls).doesNotContain("unregister:" + SUBSCRIPTION_ID + ":" + SUBSCRIBER_ID)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    @DisplayName("a pause after a lost lease refused by the wrapped model logs one warning naming the subscription, the subscriber and the refusal")
    void a_pause_after_a_lost_lease_refused_by_the_wrapped_model_logs_one_warning_naming_the_subscription_the_subscriber_and_the_refusal(SubscriptionRefusedException refusal) {
        // Given
        subscribeAndRefusePause(refusal);

        // When
        model.onConsumeProhibited(SUBSCRIPTION_ID, SUBSCRIBER_ID);

        // Then
        assertThat(warnings()).singleElement().satisfies(warning -> assertAll(
                // By position, since the refusal's message holds the subscription id too
                () -> assertThat(warning.getArgumentArray()).startsWith(SUBSCRIPTION_ID, refusal.getClass().getName(), refusal.getMessage(), SUBSCRIBER_ID, false),
                () -> assertThat(warning.getThrowableProxy().getClassName()).isEqualTo(refusal.getClass().getName()),
                () -> assertThat(warning.getThrowableProxy().getMessage()).isEqualTo(refusal.getMessage())
        ));
    }

    private static Stream<SubscriptionRefusedException> refusals() {
        return Stream.of(
                new UnknownSubscriptionException(SUBSCRIPTION_ID),
                new SubscriptionNotRunningException(SUBSCRIPTION_ID)
        );
    }

    private void subscribeAndRefusePause(SubscriptionRefusedException refusal) {
        model.subscribe(SUBSCRIBER_ID, SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> {
        });
        delegate.refusePauseOf(SUBSCRIPTION_ID, refusal);
    }

    private List<ILoggingEvent> warnings() {
        return logEvents.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    private static final class DelegateThatRefusesPause implements SubscriptionModel {
        private final Map<String, SubscriptionRefusedException> refusals = new HashMap<>();

        void refusePauseOf(String subscriptionId) {
            refusePauseOf(subscriptionId, new UnknownSubscriptionException(subscriptionId));
        }

        void refusePauseOf(String subscriptionId, SubscriptionRefusedException refusal) {
            refusals.put(subscriptionId, refusal);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
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
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            SubscriptionRefusedException refusal = refusals.get(subscriptionId);
            if (refusal != null) {
                throw refusal;
            }
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Grants the lock to every registration immediately, so {@code model.subscribe(..)} always leaves the consumer
     * Running rather than Waiting, reaching the branch under test.
     */
    private static final class RecordingCompetingConsumerStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new ArrayList<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register:" + subscriptionId + ":" + subscriberId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister:" + subscriptionId + ":" + subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release:" + subscriptionId + ":" + subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }
    }
}
