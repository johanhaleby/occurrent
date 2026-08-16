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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

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

    @Test
    void a_subclass_that_delegates_accept_to_the_batch_overload_does_not_recurse() {
        // PushSubscriptionModel is public and not final, so a subclass overriding accept(CloudEvent) to hand a
        // singleton list to accept(Iterable) is a legitimate pattern. The batch pipeline must never call back into
        // the overridable accept(CloudEvent), or this would recurse until the stack overflows.
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel() {
            @Override
            public Mono<Void> accept(CloudEvent cloudEvent) {
                return accept(List.of(cloudEvent));
            }
        };
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(received).containsExactly("1");
    }

    @Test
    void the_observer_is_told_a_matched_event_before_the_handler_runs() {
        List<String> observed = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> observed.add(cloudEvent.getId() + ":" + matched + ":" + handled.size()));
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(observed).containsExactly("1:true:0");
        assertThat(handled).containsExactly("1");
    }

    @Test
    void the_observer_call_is_deferred_until_subscribe_not_when_the_mono_is_assembled() {
        // Registers only after accept(..) has already built the Mono, and before it is subscribed. Proves the
        // observer and the match check both run on subscribe, not on assembly, since an eager implementation would
        // see no registration yet and record false.
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));

        Mono<Void> pending = model.accept(cloudEvent("1", "NameDefined"));
        model.subscribe("sub", cloudEvent -> Mono.empty());

        StepVerifier.create(pending).verifyComplete();

        assertThat(matches).containsExactly(true);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_when_nothing_is_registered() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_while_the_model_is_stopped() {
        // A stopped model drops live events by design (ADR 85), and the observer contract mirrors that: matched
        // reflects what would actually be delivered, not merely what the filter would have accepted.
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", cloudEvent -> Mono.empty());
        model.stop();

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_while_the_subscription_is_paused_on_a_running_model() {
        // Distinct from the stopped case above. Here the model itself is running, only the one subscription is
        // paused, so hasMatchingRegistration(..) has to walk the paused set to see it, not just the running flag.
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", cloudEvent -> Mono.empty());
        model.pauseSubscription("sub");

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_is_told_an_event_is_unmatched_when_the_registered_filter_declines_it() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_still_sees_the_event_when_the_matching_handler_errors() {
        List<Boolean> matches = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("boom", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMessage("handler failed");

        assertThat(matches).containsExactly(true);
    }

    @Test
    void the_observer_still_sees_the_event_when_evaluating_the_filter_itself_throws() {
        // A supplied DataFieldReader can throw while reading the payload, the same hazard the shared dispatch loop
        // documents (routeIsolated). The "every event is observed" promise has to survive that too, not just a
        // handler that errors, and the original error still has to reach the caller afterward.
        List<Boolean> matches = new ArrayList<>();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMessage("payload unreadable");

        assertThat(matches).containsExactly(false);
    }

    @Test
    void the_observer_still_sees_the_event_when_evaluating_the_filter_itself_fails_an_assertion() {
        // Same as the RuntimeException case above, but for a DataFieldReader instrumented as a test double, which is
        // as likely to throw AssertionError as a spy observer is.
        List<Boolean> matches = new ArrayList<>();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new AssertionError("payload assertion failed");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, boolean matched) -> matches.add(matched));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(AssertionError.class)
                        .hasMessage("payload assertion failed"));

        assertThat(matches).containsExactly(false);
    }

    @Test
    void a_throwing_observer_is_swallowed_and_the_matching_handler_still_runs() {
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent cloudEvent, boolean matched) -> {
            throw new IllegalStateException("observer failed");
        });
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(handled).containsExactly("1");
    }

    @Test
    void an_observer_that_fails_an_assertion_is_swallowed_and_the_matching_handler_still_runs() {
        // A test spy used as an observer is the likely source of an AssertionError, not just a RuntimeException.
        // The same guarantee has to hold for it. Observing must never be what turns a delivered event into a
        // broker redelivery.
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent cloudEvent, boolean matched) -> {
            throw new AssertionError("observer assertion failed");
        });
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(handled).containsExactly("1");
    }

    @Test
    void a_batch_stops_observing_once_a_handler_errors() {
        List<String> observed = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, boolean matched) -> observed.add(cloudEvent.getId()));
        model.subscribe("boom", cloudEvent -> cloudEvent.getId().equals("2")
                ? Mono.error(new IllegalStateException("handler failed"))
                : Mono.empty());

        StepVerifier.create(model.accept(List.of(
                        cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))))
                .verifyErrorMessage("handler failed");

        assertThat(observed).containsExactly("1", "2");
    }

    @Test
    void the_default_observer_is_a_no_op_and_delivery_is_unaffected() {
        // No PushObserver constructor argument at all: PushObserver.noop() changes nothing for existing code,
        // including that the handler still receives the event.
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

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
