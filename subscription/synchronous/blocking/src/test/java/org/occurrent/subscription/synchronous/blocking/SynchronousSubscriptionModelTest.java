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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelTest {

    @Test
    void invokes_matching_handler_synchronously_on_the_calling_thread() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<CloudEvent> received = new ArrayList<>();
        Thread callingThread = Thread.currentThread();
        List<Thread> handlerThreads = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> {
            received.add(cloudEvent);
            handlerThreads.add(Thread.currentThread());
        });

        model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged")));

        assertThat(received).extracting(CloudEvent::getId).containsExactly("1", "2");
        assertThat(handlerThreads).containsOnly(callingThread);
    }

    @Test
    void a_filter_gates_which_events_a_handler_receives() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("only-name-defined", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), cloudEvent -> received.add(cloudEvent.getId()));

        model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged"), cloudEvent("3", "NameDefined")));

        assertThat(received).containsExactly("1", "3");
    }

    @Test
    void multiple_handlers_run_in_registration_order() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> order = new ArrayList<>();
        model.subscribe("first", cloudEvent -> order.add("first:" + cloudEvent.getId()));
        model.subscribe("second", cloudEvent -> order.add("second:" + cloudEvent.getId()));

        model.dispatch(List.of(cloudEvent("1", "NameDefined")));

        assertThat(order).containsExactly("first:1", "second:1");
    }

    @Test
    void a_throwing_handler_propagates_to_the_caller() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("boom", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined"))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
    }

    @Test
    void registering_the_same_subscription_id_twice_is_rejected() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.subscribe("sub", cloudEvent -> {
        }));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already registered");
    }

    @Test
    void has_subscriptions_reflects_whether_anything_is_registered() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        assertThat(model.hasSubscriptions()).isFalse();

        model.subscribe("sub", cloudEvent -> {
        });

        assertThat(model.hasSubscriptions()).isTrue();
    }

    @Test
    void a_started_subscription_handle_is_returned() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();

        var subscription = model.subscribe("sub", cloudEvent -> {
        });

        assertThat(subscription.id()).isEqualTo("sub");
        assertThat(subscription.waitUntilStarted(java.time.Duration.ofMillis(1))).isTrue();
    }

    @Test
    void a_stopped_model_dispatches_to_nobody_and_the_caller_still_returns() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        model.stop();
        model.dispatch(List.of(cloudEvent("1", "NameDefined")));

        assertThat(received).isEmpty();
        assertThat(model.isRunning()).isFalse();
        assertThat(model.isPaused("sub")).isTrue();
    }

    @Test
    void an_event_dispatched_while_paused_is_dropped_rather_than_delivered_on_resume() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        model.stop();
        model.dispatch(List.of(cloudEvent("missed", "NameDefined")));
        model.resumeSubscription("sub");
        model.dispatch(List.of(cloudEvent("seen", "NameDefined")));

        assertThat(received).containsExactly("seen");
    }

    @Test
    void a_paused_subscription_is_skipped_while_its_siblings_still_receive() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> quiet = new ArrayList<>();
        List<String> noisy = new ArrayList<>();
        model.subscribe("quiet", cloudEvent -> quiet.add(cloudEvent.getId()));
        model.subscribe("noisy", cloudEvent -> noisy.add(cloudEvent.getId()));

        model.pauseSubscription("quiet");
        model.dispatch(List.of(cloudEvent("1", "NameDefined")));

        assertThat(quiet).isEmpty();
        assertThat(noisy).containsExactly("1");
    }

    @Test
    void registering_on_a_stopped_model_yields_a_paused_subscription() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();

        model.stop();
        model.subscribe("registered-while-stopped", cloudEvent -> received.add(cloudEvent.getId()));

        assertThat(model.isPaused("registered-while-stopped")).isTrue();
        model.resumeSubscription("registered-while-stopped");
        model.dispatch(List.of(cloudEvent("1", "NameDefined")));
        assertThat(received).containsExactly("1");
    }

    @Test
    void subscription_ids_lists_running_and_paused_subscriptions() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("running", cloudEvent -> {
        });
        model.subscribe("paused", cloudEvent -> {
        });
        model.pauseSubscription("paused");

        assertThat(model.subscriptionIds()).containsExactlyInAnyOrder("running", "paused");
    }

    @Test
    void resuming_a_subscription_that_is_not_paused_fails() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("sub"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("is not paused");
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
