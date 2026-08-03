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
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PushSubscriptionModelTest {

    @Test
    void routes_a_pushed_event_to_a_matching_handler() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(received).containsExactly("1");
    }

    @Test
    void routes_a_pushed_batch_in_order() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        model.accept(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged")));

        assertThat(received).containsExactly("1", "2");
    }

    @Test
    void a_filter_gates_which_events_a_handler_receives() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("only-name-defined", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), cloudEvent -> received.add(cloudEvent.getId()));

        model.accept(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged"), cloudEvent("3", "NameDefined")));

        assertThat(received).containsExactly("1", "3");
    }

    @Test
    void a_second_consumer_is_refused_and_the_first_still_works() {
        // PushSubscriptionModel feeds exactly one consumer (ADR 88): a push sink has one broker acknowledgement per
        // message, so fan-out would let one failing consumer hold up every consumer behind it.
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("first", cloudEvent -> received.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.subscribe("second", cloudEvent -> {
        }));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("first")
                .hasMessageContaining("second");

        // The refused registration didn't disturb the one already in place.
        model.accept(cloudEvent("1", "NameDefined"));
        assertThat(received).containsExactly("1");
    }

    @Test
    void a_throwing_handler_propagates_to_the_caller() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("boom", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
    }

    @Test
    void registering_the_same_subscription_id_twice_is_rejected() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.subscribe("sub", cloudEvent -> {
        }));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already registered");
    }

    @Test
    void a_cancelled_subscription_receives_no_further_events() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> cancelledHandler = new ArrayList<>();
        model.subscribe("cancel-me", cloudEvent -> cancelledHandler.add(cloudEvent.getId()));

        model.cancelSubscription("cancel-me");
        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(cancelledHandler).isEmpty();
    }

    @Test
    void cancelling_the_sole_subscription_frees_the_sink_for_a_different_id() {
        // The single-consumer slot counts what is registered now, not whether anything ever was, so cancelling
        // "cancel-me" must free it for an unrelated id, not just for "cancel-me" again.
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> cancelledHandler = new ArrayList<>();
        List<String> newHandler = new ArrayList<>();
        model.subscribe("cancel-me", cloudEvent -> cancelledHandler.add(cloudEvent.getId()));

        model.cancelSubscription("cancel-me");
        Throwable thrown = catchThrowable(() -> model.subscribe("different-id", cloudEvent -> newHandler.add(cloudEvent.getId())));

        assertThat(thrown).isNull();
        model.accept(cloudEvent("1", "NameDefined"));
        assertThat(cancelledHandler).isEmpty();
        assertThat(newHandler).containsExactly("1");
    }

    @Test
    void a_cancelled_subscription_id_can_be_registered_again() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("a", cloudEvent -> {
        });

        model.cancelSubscription("a");

        List<String> received = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.subscribe("a", cloudEvent -> received.add(cloudEvent.getId())));

        assertThat(thrown).isNull();
        model.accept(cloudEvent("1", "NameDefined"));
        assertThat(received).containsExactly("1");
    }

    @Test
    void cancelling_an_unknown_id_is_a_no_op() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.cancelSubscription("unknown"));

        assertThat(thrown).isNull();
        model.accept(cloudEvent("1", "NameDefined"));
        assertThat(received).containsExactly("1");
    }

    @Test
    void cancelling_an_already_cancelled_id_is_a_no_op() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });
        model.cancelSubscription("sub");

        Throwable thrown = catchThrowable(() -> model.cancelSubscription("sub"));

        assertThat(thrown).isNull();
    }

    @Test
    void has_subscriptions_reflects_whether_anything_is_registered() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        assertThat(model.hasSubscriptions()).isFalse();

        model.subscribe("sub", cloudEvent -> {
        });

        assertThat(model.hasSubscriptions()).isTrue();
    }

    @Test
    void a_started_subscription_handle_is_returned() {
        PushSubscriptionModel model = new PushSubscriptionModel();

        var subscription = model.subscribe("sub", cloudEvent -> {
        });

        assertThat(subscription.id()).isEqualTo("sub");
        assertThat(subscription.waitUntilStarted(Duration.ofMillis(1))).isTrue();
    }

    @Test
    void can_be_used_as_a_plain_cloud_event_consumer() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        // A listener may hold the model as a Consumer<CloudEvent> and feed events through it.
        Consumer<CloudEvent> listener = model;
        listener.accept(cloudEvent("1", "NameDefined"));

        assertThat(received).containsExactly("1");
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
