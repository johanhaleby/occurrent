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

package org.occurrent.subscription.reactor.durable;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The subscribe a reactor catch-up calls with whether it has cancelled the subscribe, see
 * {@link org.occurrent.subscription.api.reactor.internal.CancellableRegistration}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelCancellableRegistrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Nested
    class OverAModelThatManagesNamedSubscriptions {

        @Test
        void registers_nothing_and_returns_null_when_the_call_was_cancelled() {
            // Given
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed");
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), __ -> Mono.empty(), () -> true);
            List<String> subscribedAfterTheCall = List.copyOf(wrapped.subscribedIds);

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(subscribedAfterTheCall).as("the subscribes the wrapped model got").isEmpty(),
                    () -> assertThat(model.subscribe("x", __ -> Mono.empty()).waitUntilStarted().block(TIMEOUT))
                            .as("what waiting for a subscribe of x afterwards gives").isNull());
        }

        @Test
        void hands_the_subscription_over_when_the_call_was_not_cancelled() {
            // Given
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed");
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), __ -> Mono.empty(), () -> false);

            // Then
            assertAll(
                    () -> assertThat(requireNonNull(subscription).waitUntilStarted().block(TIMEOUT)).as("what waiting for the subscription gives").isNull(),
                    () -> assertThat(wrapped.subscribedIds).as("the subscribes the wrapped model got").containsExactly("x"));
        }
    }

    @Nested
    class OverAModelThatOnlyStreamsEvents {

        @Test
        void registers_nothing_and_returns_null_when_the_call_was_cancelled() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            AtomicBoolean askedHoldingTheMonitor = new AtomicBoolean();

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), __ -> Mono.empty(), () -> {
                askedHoldingTheMonitor.set(Thread.holdsLock(model));
                return true;
            });

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(askedHoldingTheMonitor).as("whether the model asked holding the monitor its cancel takes").isTrue(),
                    () -> assertThat(model.subscriptionIds()).as("the ids the model holds").isEmpty(),
                    () -> assertThat(model.subscribe("x", __ -> Mono.empty()).waitUntilStarted().block(TIMEOUT))
                            .as("what waiting for a subscribe of x afterwards gives").isNull());
        }

        @Test
        void registers_the_subscription_when_the_call_was_not_cancelled() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), __ -> Mono.empty(), () -> false);

            // Then
            assertAll(
                    () -> assertThat(requireNonNull(subscription).waitUntilStarted().block(TIMEOUT)).as("what waiting for the subscription gives").isNull(),
                    () -> assertThat(model.isRunning("x")).as("whether the model runs x").isTrue());
        }
    }
}
