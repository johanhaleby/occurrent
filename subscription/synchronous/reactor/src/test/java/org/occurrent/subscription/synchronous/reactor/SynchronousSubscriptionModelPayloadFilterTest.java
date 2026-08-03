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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

/**
 * A synchronous subscription matches entirely in process, so filtering on a field inside the payload only works
 * when the model was given a reader for it. Built without one it refuses on the first event, which for a synchronous
 * subscription is inside the write.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelPayloadFilterTest {

    @Test
    void a_payload_filter_gates_which_events_a_handler_receives() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel(new JacksonDataFieldReader());
        List<String> received = new ArrayList<>();
        model.subscribe("big-amounts", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        model.dispatch(List.of(event("matching", "{\"amount\":42}"), event("not-matching", "{\"amount\":7}"))).block();

        assertThat(received).containsExactly("matching");
    }

    @Test
    void a_payload_filter_is_refused_on_the_first_event_when_the_model_was_given_no_reader() {
        // Registering succeeds, because building the matcher does not read anything. The refusal lands when an event
        // arrives and the payload has to be read.
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("big-amounts", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(event("1", "{\"amount\":42}"))).block());

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
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
