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
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.DcbSubscriptionFilter;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySubscriptionModelDcbFilterTest {

    private InMemorySubscriptionModel subscriptionModel;

    @BeforeEach
    void create_subscription_model() {
        subscriptionModel = new InMemorySubscriptionModel();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void delivers_matching_dcb_event_to_subscriber_with_dcb_filter() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("x:1"))), received::add)
                .waitUntilStarted();

        CloudEvent matching = dcbEvent("TypeA", 1L, List.of("x:1"));
        subscriptionModel.accept(Stream.of(matching));

        await().untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    @Test
    void does_not_deliver_event_missing_required_tag() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("x:1"))), received::add)
                .waitUntilStarted();

        CloudEvent nonMatching = dcbEvent("TypeA", 1L, List.of("y:2"));
        subscriptionModel.accept(Stream.of(nonMatching));

        // Give the async dispatcher a window to (incorrectly) deliver; then assert nothing arrived.
        await().during(java.time.Duration.ofMillis(200)).atMost(java.time.Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(received).isEmpty());
    }

    @Test
    void does_not_deliver_a_stream_event_that_has_a_position_but_no_dcb_tags() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        // A DcbCriteria.all() matches any tags, so only the DCB discriminator (the DCB tags extension) keeps stream
        // events out. With stream position on by default, a stream event carries a position, so a "position > 0" guard
        // would leak it into this DCB subscription; the isDcbEvent guard must not.
        subscriptionModel.subscribe("sub", DcbSubscriptionFilter.filter(DcbCriteria.all()), received::add)
                .waitUntilStarted();

        // A stream-written event: it has a global position but no DCB tags extension, so it is not a DCB event.
        CloudEvent streamEvent = OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType("TypeA")
                .withSource(URI.create("urn:test"))
                .withTime(OffsetDateTime.now())
                .build(), 1L);
        subscriptionModel.accept(Stream.of(streamEvent));

        await().during(java.time.Duration.ofMillis(200)).atMost(java.time.Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(received).isEmpty());
    }

    @Test
    void filters_out_non_matching_and_keeps_matching_events_in_same_batch() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("x:1"))), received::add)
                .waitUntilStarted();

        CloudEvent matching = dcbEvent("TypeA", 1L, List.of("x:1", "y:2"));
        CloudEvent nonMatching = dcbEvent("TypeB", 2L, List.of("y:2"));
        subscriptionModel.accept(Stream.of(matching, nonMatching));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    @Test
    void type_filter_delivers_only_matching_type() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", DcbSubscriptionFilter.filter(DcbCriteria.type("OrderPlaced")), received::add)
                .waitUntilStarted();

        CloudEvent matching = dcbEvent("OrderPlaced", 1L, List.of("order:1"));
        CloudEvent nonMatching = dcbEvent("OrderCancelled", 2L, List.of("order:1"));
        subscriptionModel.accept(Stream.of(matching, nonMatching));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(matching.getId()));
    }

    private static CloudEvent dcbEvent(String type, long position, List<String> tags) {
        CloudEvent base = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create("urn:test"))
                .withTime(OffsetDateTime.now())
                .build();
        CloudEvent tagged = DcbCloudEvents.withTags(base, tags.stream().map(Tag::parse).toList());
        return OccurrentCloudEventExtension.withPosition(tagged, position);
    }
}
