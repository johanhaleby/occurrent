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
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelTest {

    @Test
    void dispatches_matching_events_to_handlers_in_registration_order() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> order = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.fromRunnable(() -> order.add("first:" + cloudEvent.getId())));
        model.subscribe("second", cloudEvent -> Mono.fromRunnable(() -> order.add("second:" + cloudEvent.getId())));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"))))
                .verifyComplete();

        assertThat(order).containsExactly("first:1", "second:1");
    }

    @Test
    void a_filter_gates_which_events_a_handler_receives() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("only-name-defined", StreamSubscriptionFilter.filter(Filter.type("NameDefined")),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged"), cloudEvent("3", "NameDefined"))))
                .verifyComplete();

        assertThat(received).containsExactly("1", "3");
    }

    @Test
    void a_failing_handler_errors_the_returned_mono() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("boom", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"))))
                .verifyErrorMessage("handler failed");
    }

    @Test
    void has_subscriptions_reflects_whether_anything_is_registered() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        assertThat(model.hasSubscriptions()).isFalse();

        model.subscribe("sub", cloudEvent -> Mono.empty());

        assertThat(model.hasSubscriptions()).isTrue();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
