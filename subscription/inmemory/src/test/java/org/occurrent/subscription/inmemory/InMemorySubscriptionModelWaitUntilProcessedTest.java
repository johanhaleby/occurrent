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

package org.occurrent.subscription.inmemory;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySubscriptionModel waitUntilAllEventsProcessed")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySubscriptionModelWaitUntilProcessedTest {

    private InMemorySubscriptionModel subscriptionModel;

    @BeforeEach
    void create() {
        subscriptionModel = new InMemorySubscriptionModel();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Nested
    @DisplayName("when a handler is slower than the queue drains")
    class When_a_handler_is_slower_than_the_queue_drains {

        @Test
        void the_wait_does_not_return_until_the_handler_has_finished() throws InterruptedException {
            // Given a handler that has been entered but is not allowed to finish yet
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            List<String> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("slow", cloudEvent -> {
                entered.countDown();
                await(release);
                handled.add(cloudEvent.getId());
            }).waitUntilStarted();

            subscriptionModel.accept(List.of(cloudEvent("1")));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // When the wait is given less time than the handler will take, it must report a timeout rather than
            // treating an empty queue as done. This is the assertion a queue-only implementation fails.
            boolean drainedWhileHandlerRunning = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofMillis(300));

            // Then
            assertAll(
                    () -> assertThat(drainedWhileHandlerRunning).isFalse(),
                    () -> assertThat(handled).isEmpty()
            );

            release.countDown();
        }

        @Test
        void the_wait_returns_once_the_handler_finishes() throws InterruptedException {
            // Given
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            List<String> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("slow", cloudEvent -> {
                entered.countDown();
                await(release);
                handled.add(cloudEvent.getId());
            }).waitUntilStarted();
            subscriptionModel.accept(List.of(cloudEvent("1")));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // When
            release.countDown();
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            // Then
            assertAll(
                    () -> assertThat(drained).isTrue(),
                    () -> assertThat(handled).containsExactly("1")
            );
        }
    }

    @Nested
    @DisplayName("when events have been written")
    class When_events_have_been_written {

        @Test
        void the_wait_makes_a_plain_assertion_enough() {
            // Given
            List<String> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("projection", cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted();

            // When
            subscriptionModel.accept(List.of(cloudEvent("1"), cloudEvent("2"), cloudEvent("3")));
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            // Then no polling is needed anywhere in this test
            assertAll(
                    () -> assertThat(drained).isTrue(),
                    () -> assertThat(handled).containsExactly("1", "2", "3")
            );
        }

        @Test
        void every_subscription_has_to_finish_not_just_one() {
            // Given
            List<String> fast = new CopyOnWriteArrayList<>();
            List<String> slow = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("fast", cloudEvent -> fast.add(cloudEvent.getId())).waitUntilStarted();
            subscriptionModel.subscribe("slow", cloudEvent -> {
                sleep(Duration.ofMillis(150));
                slow.add(cloudEvent.getId());
            }).waitUntilStarted();

            // When
            subscriptionModel.accept(List.of(cloudEvent("1")));
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            // Then
            assertAll(
                    () -> assertThat(drained).isTrue(),
                    () -> assertThat(fast).containsExactly("1"),
                    () -> assertThat(slow).containsExactly("1")
            );
        }
    }

    @Nested
    @DisplayName("when nothing is there to wait for")
    class When_nothing_is_there_to_wait_for {

        @Test
        void the_wait_returns_immediately_with_no_subscriptions() {
            // When
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            // Then
            assertThat(drained).isTrue();
        }

        @Test
        void the_wait_returns_immediately_when_no_event_was_written() {
            // Given
            subscriptionModel.subscribe("projection", cloudEvent -> {
            }).waitUntilStarted();

            // When
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            // Then
            assertThat(drained).isTrue();
        }
    }

    @Nested
    @DisplayName("when a subscription is paused")
    class When_a_subscription_is_paused {

        @Test
        void the_wait_ignores_it_rather_than_hanging_on_its_undrainable_queue() throws InterruptedException {
            // Given a subscription with one event stuck in its handler and a second still queued behind it, so it has
            // outstanding work that pausing can never drain. Without this the count would be zero and the test would
            // pass whether or not the wait skips paused subscriptions.
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            List<String> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("paused", cloudEvent -> {
                entered.countDown();
                await(release);
                handled.add(cloudEvent.getId());
            }).waitUntilStarted();

            subscriptionModel.accept(List.of(cloudEvent("1"), cloudEvent("2")));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // When
            subscriptionModel.pauseSubscription("paused");
            boolean drained = subscriptionModel.waitUntilAllEventsProcessed(Duration.ofSeconds(2));

            // Then
            assertAll(
                    () -> assertThat(drained).isTrue(),
                    () -> assertThat(subscriptionModel.isPaused("paused")).isTrue(),
                    () -> assertThat(handled).isEmpty()
            );

            release.countDown();
        }
    }

    @Nested
    @DisplayName("when a handler throws")
    class When_a_handler_throws {

        @Test
        void the_wait_returns_once_the_retries_are_exhausted() {
            // Given a handler that always throws, under a retry strategy that gives up quickly
            AtomicInteger attempts = new AtomicInteger();
            InMemorySubscriptionModel failingModel = new InMemorySubscriptionModel(RetryStrategy.retry().maxAttempts(2).backoff(org.occurrent.retry.Backoff.none()));
            try {
                failingModel.subscribe("failing", cloudEvent -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("boom");
                }).waitUntilStarted();

                // When
                failingModel.accept(List.of(cloudEvent("1")));
                boolean drained = failingModel.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

                // Then the wait does not hang on a handler that will never succeed
                assertAll(
                        () -> assertThat(drained).isTrue(),
                        () -> assertThat(attempts.get()).isGreaterThanOrEqualTo(1)
                );
            } finally {
                failingModel.shutdown();
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, SECONDS)) {
                throw new IllegalStateException("Latch was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Test")
                .build();
    }
}
