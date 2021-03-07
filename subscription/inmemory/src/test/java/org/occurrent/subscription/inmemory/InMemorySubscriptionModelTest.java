/*
 * Copyright 2021 Johan Haleby
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


import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.time.TimeConversion;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

public class InMemorySubscriptionModelTest {

    private InMemoryEventStore inMemoryEventStore;
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private ObjectMapper objectMapper;

    @BeforeEach
    void event_store_and_subscription_model_are_initialized_before_each_test() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        inMemoryEventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void events_written_to_event_store_are_propagated_to_all_subscribers() {
        // Given
        CloudEvent cloudEvent = new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSubject("subject")
                .withType("type1")
                .withSource(URI.create("urn:source"))
                .withTime(OffsetDateTime.now())
                .withData("test".getBytes(UTF_8))
                .build();

        CopyOnWriteArrayList<CloudEvent> receivedEvents1 = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> receivedEvents2 = new CopyOnWriteArrayList<>();

        inMemorySubscriptionModel.subscribe("subscription1", receivedEvents1::add);
        inMemorySubscriptionModel.subscribe("subscription2", receivedEvents2::add);

        // When
        inMemoryEventStore.write("streamId1", Stream.of(cloudEvent));

        // Then
        await().until(receivedEvents1::size, is(1));
        await().until(receivedEvents2::size, is(1));

        assertAll(
                () -> assertThat(receivedEvents1).extracting(CloudEvent::getId).containsOnly(cloudEvent.getId()),
                () -> assertThat(receivedEvents2).extracting(CloudEvent::getId).containsOnly(cloudEvent.getId())
        );
    }

    @Test
    void events_are_retried_on_failure() {
        // Given
        CloudEvent cloudEvent = new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSubject("subject")
                .withType("type1")
                .withSource(URI.create("urn:source"))
                .withTime(OffsetDateTime.now())
                .withData("test".getBytes(UTF_8))
                .build();

        CopyOnWriteArrayList<CloudEvent> receivedEvents = new CopyOnWriteArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);

        inMemorySubscriptionModel.subscribe("subscription1", e -> {
            if (counter.incrementAndGet() < 3) {
                throw new IllegalStateException("expected");
            }
            receivedEvents.add(e);
        });

        // When
        inMemoryEventStore.write("streamId1", Stream.of(cloudEvent));

        // Then
        await().untilAsserted(() -> {
            assertThat(receivedEvents).extracting(CloudEvent::getId).containsOnly(cloudEvent.getId());
        });
    }

    @Test
    void inmemory_subscription_model_throws_iae_when_subscription_already_exists() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();

        // When
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void throws_iae_when_start_at_is_not_now() {
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe("subscription1", StartAt.subscriptionPosition(new StringBasedSubscriptionPosition("343")), __ -> {
        }));

        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("InMemorySubscriptionModel only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
    }
    
    @Nested
    @DisplayName("Lifecycle")
    class LifeCycleTest {

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Test
        void inmemory_subscription_model_allows_cancelling_a_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CountDownLatch eventReceived = new CountDownLatch(1);
            String subscriberId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriberId, __ -> eventReceived.countDown()).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

            // When
            inMemoryEventStore.write("1", serialize(nameDefined1));
            // The subscription is async so we need to wait for it
            eventReceived.await(1, SECONDS);
            
            inMemorySubscriptionModel.cancelSubscription(subscriberId);

            // Then
            assertAll(
                    () -> assertThat(inMemorySubscriptionModel.isRunning(subscriberId)).isFalse(),
                    () -> assertThat(inMemorySubscriptionModel.isPaused(subscriberId)).isFalse()
            );
        }

        @Test
        void inmemory_subscription_model_is_running_returns_false_when_subscription_is_not_started() {
            boolean running = inMemorySubscriptionModel.isRunning(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void inmemory_subscription_model_is_running_returns_false_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriptionId, __ -> {});
            inMemorySubscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean running = inMemorySubscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isFalse();
        }

        @Test
        void inmemory_subscription_model_is_running_returns_true_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriptionId, __ -> {
            });

            // When
            boolean running = inMemorySubscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isTrue();
        }

        @Test
        void inmemory_subscription_model_is_paused_returns_false_when_subscription_is_not_started() {
            boolean running = inMemorySubscriptionModel.isPaused(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void inmemory_subscription_model_is_paused_returns_false_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriptionId, __ -> {
            });

            // When
            boolean paused = inMemorySubscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isFalse();
        }

        @Test
        void inmemory_subscription_model_is_paused_returns_true_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriptionId, __ -> {
            });
            inMemorySubscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean paused = inMemorySubscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isTrue();
        }

        @Test
        void inmemory_subscription_model_allows_stopping_and_starting_all_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            inMemorySubscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

            // When
            inMemorySubscriptionModel.stop();

            // Then
            inMemorySubscriptionModel.start();

            inMemoryEventStore.write("1", serialize(nameDefined1));
            inMemoryEventStore.write("2", 0, serialize(nameDefined2));
            inMemoryEventStore.write("1", 1, serialize(nameWasChanged1));

            await("state").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        }

        @Test
        void inmemory_subscription_model_allows_pausing_and_resuming_individual_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> subscription1State = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> subscription2State = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            inMemorySubscriptionModel.subscribe(subscriptionId, subscription1State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            inMemorySubscriptionModel.subscribe(UUID.randomUUID().toString(), subscription2State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

            // When
            inMemorySubscriptionModel.pauseSubscription(subscriptionId);

            inMemoryEventStore.write("1", serialize(nameDefined1));

            await("subscription2 received event").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(subscription2State).hasSize(1));
            Thread.sleep(200); // We wait a little bit longer to give subscription some time to receive the event (even though it shouldn't!)
            assertThat(subscription1State).isEmpty();

            // Then
            inMemorySubscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted();

            inMemoryEventStore.write("2", 0, serialize(nameDefined2));
            inMemoryEventStore.write("1", 1, serialize(nameWasChanged1));

            await("subscription1 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription1State).extracting(CloudEvent::getId).containsExactly(nameDefined2.getEventId(), nameWasChanged1.getEventId()));
            await("subscription2 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription2State).extracting(CloudEvent::getId).containsExactly(nameDefined1.getEventId(), nameDefined2.getEventId(), nameWasChanged1.getEventId()));
        }
    }

    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(io.cloudevents.core.builder.CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}