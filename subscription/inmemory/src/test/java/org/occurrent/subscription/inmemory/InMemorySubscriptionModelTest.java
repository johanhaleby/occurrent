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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.time.TimeConversion;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
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
    void inmemory_subscription_model_refuses_a_subscription_id_that_already_exists() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();

        // When
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());

        // Then
        assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void refuses_a_start_position_it_does_not_support() {
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe("subscription1", StartAt.checkpoint(new StringBasedCheckpoint("343")), __ -> {
        }));

        assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessage("InMemorySubscriptionModel only supports starting from 'now' and 'default' (StartAt.now() or StartAt.subscriptionModelDefault())");
    }
    
    @Test
    void inmemory_subscription_model_allows_cancelling_a_subscription() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CountDownLatch eventReceived = new CountDownLatch(1);
        String subscriberId = UUID.randomUUID().toString();
        inMemorySubscriptionModel.subscribe(subscriberId, __ -> eventReceived.countDown()).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

        // When
        inMemoryEventStore.write("1", serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        assertThat(eventReceived.await(1, SECONDS))
                .as("the event must have been delivered before the subscription is cancelled")
                .isTrue();

        inMemorySubscriptionModel.cancelSubscription(subscriberId);

        // Then
        assertAll(
                () -> assertThat(inMemorySubscriptionModel.isRunning(subscriberId)).isFalse(),
                () -> assertThat(inMemorySubscriptionModel.isPaused(subscriberId)).isFalse()
        );
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(io.cloudevents.core.builder.CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}