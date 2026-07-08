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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@link AgnosticSubscriptionFilter} delivers both stream-written and DCB-appended events on the
 * in-memory subscription model, filtered only by the wrapped {@link Filter} (typically event type), with no
 * capability guard. Contrast with {@link InMemorySubscriptionModelDcbFilterTest}, which is scoped to DCB only.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySubscriptionModelAgnosticFilterTest {

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
    void delivers_both_a_stream_event_and_a_dcb_event_to_a_neutral_subscription() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", AgnosticSubscriptionFilter.filter(Filter.all()), received::add)
                .waitUntilStarted();

        CloudEvent streamEvent = streamEvent("TypeA", 1L);
        CloudEvent dcbEvent = dcbEvent("TypeB", 2L, List.of("x:1"));
        subscriptionModel.accept(List.of(streamEvent, dcbEvent));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactlyInAnyOrder(streamEvent.getId(), dcbEvent.getId()));
    }

    @Test
    void type_filter_delivers_only_matching_type_across_both_capabilities() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("sub", AgnosticSubscriptionFilter.filter(Filter.type("OrderPlaced")), received::add)
                .waitUntilStarted();

        CloudEvent matchingStream = streamEvent("OrderPlaced", 1L);
        CloudEvent matchingDcb = dcbEvent("OrderPlaced", 2L, List.of("order:1"));
        CloudEvent nonMatchingStream = streamEvent("OrderCancelled", 3L);
        CloudEvent nonMatchingDcb = dcbEvent("OrderCancelled", 4L, List.of("order:1"));
        subscriptionModel.accept(List.of(matchingStream, matchingDcb, nonMatchingStream, nonMatchingDcb));

        await().untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId)
                        .containsExactlyInAnyOrder(matchingStream.getId(), matchingDcb.getId()));
    }

    private static CloudEvent streamEvent(String type, long position) {
        CloudEvent base = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create("urn:test"))
                .withTime(OffsetDateTime.now())
                .build();
        return OccurrentCloudEventExtension.withPosition(base, position);
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
