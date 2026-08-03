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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.condition.Condition.eq;

/**
 * This model has taken a {@code DataFieldReader} since #498, and nothing exercised that path end to end: no
 * production or test code constructed it with a real reader, so the threading was only ever verified by reading it.
 * Every other subscription model that can now answer a payload filter copies this shape, which is reason enough to
 * hold it to a test.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySubscriptionModelPayloadFilterTest {

    private InMemorySubscriptionModel subscriptionModel;

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
    }

    @Test
    void a_payload_filter_delivers_only_matching_events_when_a_reader_is_supplied() {
        subscriptionModel = new InMemorySubscriptionModel(new JacksonDataFieldReader());
        List<String> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("big-amounts", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> received.add(cloudEvent.getId()));

        subscriptionModel.accept(List.of(event("matching", "{\"amount\":42}"), event("not-matching", "{\"amount\":7}")));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).containsExactly("matching"));
    }

    private static CloudEvent event(String id, String json) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("SomethingHappened")
                .withDataContentType("application/json")
                .withData(json.getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
