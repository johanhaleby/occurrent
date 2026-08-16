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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
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
    void a_second_consumer_is_refused_and_the_first_still_works() {
        // PushSubscriptionModel feeds exactly one consumer (ADR 90): a push sink has one broker acknowledgement per
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

    /**
     * {@code RegisteringSubscribable.shutdown()} is final and documents itself as not reversible: the ids are released
     * and the handlers are gone, so a shut-down model delivers nothing even after {@code start(..)}. Nothing held it to
     * that, and it is the reason the register-only models decline the TCK's {@code RestartConformance}: there is no
     * durable state for a rebuilt model to pick up, and no way for an event to wait while nothing is running. So the
     * refusal is asserted here rather than left as a sentence in a javadoc.
     */
    @Test
    void a_shut_down_model_stays_shut_down_even_after_being_started_again() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));
        model.accept(cloudEvent("1", "NameDefined"));
        assertThat(received).containsExactly("1");

        model.shutdown();
        model.start(true);

        model.accept(cloudEvent("2", "NameWasChanged"));
        assertThat(received)
                .as("shutdown dropped the registration, so starting the model again brings back nothing to deliver to")
                .containsExactly("1");
        assertThat(model.subscriptionIds())
                .as("and the id is gone rather than held by a handler that no longer exists")
                .isEmpty();
    }

    @Test
    void registering_the_same_subscription_id_twice_is_rejected() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.subscribe("sub", cloudEvent -> {
        }));

        assertThat(thrown).isInstanceOf(DuplicateSubscriptionIdException.class);
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
    void registering_on_a_stopped_model_answers_not_started_and_the_handle_from_resuming_it_answers_started() {
        // RegisteringSubscribable has no background thread to wait for. Registering on a running model starts the
        // subscription there and then, and registering on a stopped one leaves it paused, so the handle it returns
        // must say so rather than claim success it has not delivered on yet.
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.stop();

        var registered = model.subscribe("sub", cloudEvent -> {
        });

        assertThat(registered.waitUntilStarted(Duration.ofMillis(1))).isFalse();

        var started = model.resumeSubscription("sub");

        assertThat(started.waitUntilStarted(Duration.ofMillis(1))).isTrue();
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

    @Test
    void the_observer_is_told_a_matched_event_before_the_handler_runs() {
        List<String> observed = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(
                (CloudEvent cloudEvent, boolean matched) -> observed.add(cloudEvent.getId() + ":" + matched + ":" + handled.size()));
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(observed).containsExactly("1:true:0");
        assertThat(handled).containsExactly("1");
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_when_nothing_is_registered() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> matches.add(matched));

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_while_the_model_is_stopped() {
        // A stopped model drops live events by design (ADR 85), and the observer contract mirrors that: matched
        // reflects what would actually be delivered, not merely what the filter would have accepted.
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", cloudEvent -> {
        });
        model.stop();

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_when_the_registered_filter_declines_it() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")), cloudEvent -> {
        });

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_still_sees_the_event_when_the_matching_handler_throws() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("boom", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(matches).containsExactly(true);
    }

    @Test
    void a_throwing_observer_is_swallowed_and_the_matching_handler_still_runs() {
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> {
            throw new IllegalStateException("observer failed");
        });
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isNull();
        assertThat(handled).containsExactly("1");
    }

    @Test
    void a_batch_stops_observing_once_a_handler_throws() {
        List<String> observed = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel((CloudEvent cloudEvent, boolean matched) -> observed.add(cloudEvent.getId()));
        model.subscribe("boom", cloudEvent -> {
            if (cloudEvent.getId().equals("2")) {
                throw new IllegalStateException("handler failed");
            }
        });

        Throwable thrown = catchThrowable(() -> model.accept(List.of(
                cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(observed).containsExactly("1", "2");
    }

    @Test
    void the_default_observer_is_a_no_op() {
        // No PushObserver constructor argument at all: PushObserver.noop() changes nothing for existing code.
        PushSubscriptionModel model = new PushSubscriptionModel();

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isNull();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
