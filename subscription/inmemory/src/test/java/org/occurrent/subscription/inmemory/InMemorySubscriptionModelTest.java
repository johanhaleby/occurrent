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


import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedSubscriptionPosition;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

public class InMemorySubscriptionModelTest {

    private InMemoryEventStore inMemoryEventStore;
    private InMemorySubscriptionModel inMemorySubscriptionModel;

    @BeforeEach
    void event_store_and_subscription_model_are_initialized_before_each_test() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        inMemoryEventStore = new InMemoryEventStore(inMemorySubscriptionModel);
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
    void throws_iae_when_start_at_is_not_now() {
        Throwable throwable = catchThrowable(() -> inMemorySubscriptionModel.subscribe("subscription1", StartAt.subscriptionPosition(new StringBasedSubscriptionPosition("343")), __ -> {
        }));

        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("InMemorySubscriptionModel only supports starting from 'now' (StartAt.now())");
    }
}