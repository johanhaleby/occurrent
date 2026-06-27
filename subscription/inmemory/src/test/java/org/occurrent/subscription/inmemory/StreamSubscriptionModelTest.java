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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.StreamSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The typed {@link StreamSubscriptionModel} facade over the shared {@link InMemorySubscriptionModel}. It wraps the
 * Occurrent {@link Filter} in an {@code OccurrentSubscriptionFilter} for the delegate and forwards life-cycle calls, so
 * a DCB query cannot be passed to it: that is a compile-time guarantee, not something a runtime test can express.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamSubscriptionModelTest {

    private InMemorySubscriptionModel delegate;
    private StreamSubscriptionModel streamSubscriptionModel;

    @BeforeEach
    void create_subscription_model() {
        delegate = new InMemorySubscriptionModel();
        streamSubscriptionModel = StreamSubscriptionModel.from(delegate);
    }

    @AfterEach
    void shutdown() {
        delegate.shutdown();
    }

    @Test
    void delivers_only_filter_matching_events() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        streamSubscriptionModel.subscribe("sub", Filter.type("OrderPlaced"), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent matching = event("OrderPlaced");
        CloudEvent nonMatching = event("OrderCancelled");
        delegate.accept(Stream.of(matching, nonMatching));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    @Test
    void the_no_filter_overload_subscribes_to_every_event() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        streamSubscriptionModel.subscribe("sub", received::add)
                .waitUntilStarted();

        CloudEvent first = event("OrderPlaced");
        CloudEvent second = event("OrderCancelled");
        delegate.accept(Stream.of(first, second));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(first.getId(), second.getId()));
    }

    @Test
    void cancel_through_the_facade_stops_delivery() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        streamSubscriptionModel.subscribe("sub", Filter.type("OrderPlaced"), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent first = event("OrderPlaced");
        delegate.accept(Stream.of(first));
        await().untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).containsExactly(first.getId()));

        streamSubscriptionModel.cancelSubscription("sub");

        delegate.accept(Stream.of(event("OrderPlaced")));
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).containsExactly(first.getId()));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create("urn:test"))
                .withTime(OffsetDateTime.now())
                .build();
    }
}
