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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PushSubscriptionModelTest {

    @Test
    void a_second_consumer_is_refused_and_the_first_still_works() {
        // PushSubscriptionModel feeds exactly one consumer (ADR 90): a push sink has one broker acknowledgement per
        // message, so fan-out would let one failing consumer hold up every consumer behind it.
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        Throwable thrown = catchThrowable(() -> model.subscribe("second", cloudEvent -> Mono.empty()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("first")
                .hasMessageContaining("second");

        // The refused registration didn't disturb the one already in place.
        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyComplete();
        assertThat(received).containsExactly("1");
    }

    @Test
    void cancelling_the_sole_subscription_frees_the_sink_for_a_different_id() {
        // The single-consumer slot counts what is registered now, not whether anything ever was, so cancelling
        // "cancel-me" must free it for an unrelated id, not just for "cancel-me" again.
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> cancelledHandler = new ArrayList<>();
        List<String> newHandler = new ArrayList<>();
        model.subscribe("cancel-me", cloudEvent -> Mono.fromRunnable(() -> cancelledHandler.add(cloudEvent.getId())));

        model.cancelSubscription("cancel-me");
        Throwable thrown = catchThrowable(() ->
                model.subscribe("different-id", cloudEvent -> Mono.fromRunnable(() -> newHandler.add(cloudEvent.getId()))));

        assertThat(thrown).isNull();
        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyComplete();
        assertThat(cancelledHandler).isEmpty();
        assertThat(newHandler).containsExactly("1");
    }

    @Test
    void routes_a_pushed_batch_in_order() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged"))))
                .verifyComplete();

        assertThat(received).containsExactly("1", "2");
    }

    @Test
    void a_filter_gates_which_events_a_handler_receives() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("only-name-defined", StreamSubscriptionFilter.filter(Filter.type("NameDefined")),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged"), cloudEvent("3", "NameDefined"))))
                .verifyComplete();

        assertThat(received).containsExactly("1", "3");
    }

    @Test
    void a_failing_handler_errors_the_returned_mono() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("boom", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMessage("handler failed");
    }

    @Test
    void has_subscriptions_reflects_whether_anything_is_registered() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        assertThat(model.hasSubscriptions()).isFalse();

        model.subscribe("sub", cloudEvent -> Mono.empty());

        assertThat(model.hasSubscriptions()).isTrue();
    }

    @Test
    void registering_the_same_subscription_id_twice_is_rejected() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> Mono.empty());

        Throwable thrown = catchThrowable(() -> model.subscribe("sub", cloudEvent -> Mono.empty()));

        assertThat(thrown).isInstanceOf(DuplicateSubscriptionIdException.class);
    }

    @Test
    void a_started_subscription_handle_is_returned() {
        PushSubscriptionModel model = new PushSubscriptionModel();

        var subscription = model.subscribe("sub", cloudEvent -> Mono.empty());

        assertThat(subscription.id()).isEqualTo("sub");
        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();
        StepVerifier.create(subscription.waitUntilStarted(Duration.ofSeconds(5))).expectNext(true).verifyComplete();
    }

    @Test
    void registering_on_a_stopped_model_answers_not_started_and_the_handle_from_resuming_it_answers_started() {
        // RegisteringSubscribable has no background thread to wait for. Registering on a running model starts the
        // subscription there and then, and registering on a stopped one leaves it paused, so the handle it returns
        // must say so rather than claim success it has not delivered on yet.
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.stop();

        var registered = model.subscribe("sub", cloudEvent -> Mono.empty());

        StepVerifier.create(registered.waitUntilStarted(Duration.ofMillis(50))).expectNext(false).verifyComplete();

        var started = model.resumeSubscription("sub");

        StepVerifier.create(started.waitUntilStarted()).verifyComplete();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
