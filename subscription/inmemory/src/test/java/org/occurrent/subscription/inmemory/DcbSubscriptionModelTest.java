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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.blocking.DcbSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The typed {@link DcbSubscriptionModel} facade over the shared {@link InMemorySubscriptionModel}. It only accepts a
 * {@link DcbQuery} and a {@link DcbStartAt}, so a time-based stream position cannot be passed to it: that is a
 * compile-time guarantee, not something a runtime test can express.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionModelTest {

    private InMemorySubscriptionModel delegate;
    private DcbSubscriptionModel dcbSubscriptionModel;

    @BeforeEach
    void create_subscription_model() {
        delegate = new InMemorySubscriptionModel();
        dcbSubscriptionModel = DcbSubscriptionModel.from(delegate);
    }

    @AfterEach
    void shutdown() {
        delegate.shutdown();
    }

    @Test
    void delivers_only_query_matching_dcb_events() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        dcbSubscriptionModel.subscribe("sub", DcbQuery.tagsAllOf("x:1"), DcbStartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent matching = dcbEvent("TypeA", 1L, List.of("x:1"));
        CloudEvent nonMatching = dcbEvent("TypeA", 2L, List.of("y:2"));
        delegate.accept(Stream.of(matching, nonMatching));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    @Test
    void the_no_start_position_overload_subscribes_at_the_default() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        dcbSubscriptionModel.subscribe("sub", DcbQuery.type("OrderPlaced"), received::add)
                .waitUntilStarted();

        CloudEvent matching = dcbEvent("OrderPlaced", 1L, List.of("order:1"));
        delegate.accept(Stream.of(matching));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    @Test
    void cancel_through_the_facade_stops_delivery() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        dcbSubscriptionModel.subscribe("sub", DcbQuery.tagsAllOf("x:1"), DcbStartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent first = dcbEvent("TypeA", 1L, List.of("x:1"));
        delegate.accept(Stream.of(first));
        await().untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).containsExactly(first.getId()));

        dcbSubscriptionModel.cancelSubscription("sub");

        delegate.accept(Stream.of(dcbEvent("TypeA", 2L, List.of("x:1"))));
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).containsExactly(first.getId()));
    }

    private static CloudEvent dcbEvent(String type, long position, List<String> tags) {
        CloudEvent base = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create("urn:test"))
                .withTime(OffsetDateTime.now())
                .build();
        return DcbCloudEvents.withPosition(DcbCloudEvents.withTags(base, tags), position);
    }
}
