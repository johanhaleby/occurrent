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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

/**
 * A push subscription is fed by the application, so nothing has filtered the event before it arrives and the whole
 * match happens in process. Filtering on a payload field therefore needs a reader, and without one it refuses at
 * subscribe time, rather than accepting the subscription and failing later on the first event pushed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PushSubscriptionModelPayloadFilterTest {

    @Test
    void a_payload_filter_gates_which_events_a_handler_receives() {
        PushSubscriptionModel model = new PushSubscriptionModel(new JacksonDataFieldReader());
        List<String> received = new ArrayList<>();
        model.subscribe("big-amounts", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> received.add(cloudEvent.getId()));

        model.accept(List.of(event("matching", "{\"amount\":42}"), event("not-matching", "{\"amount\":7}")));

        assertThat(received).containsExactly("matching");
    }

    @Test
    void a_payload_filter_is_refused_at_subscribe_time_when_the_model_was_given_no_reader() {
        // The model cannot answer a payload condition at all, which is knowable before a single event arrives, so it
        // refuses here rather than accepting the subscription and failing on the first event pushed.
        PushSubscriptionModel model = new PushSubscriptionModel();

        Throwable thrown = catchThrowable(() -> model.subscribe("big-amounts",
                StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> {
                }));

        assertThat(thrown).isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("occurrent-common-inmemory-filter-matching-jackson");
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
