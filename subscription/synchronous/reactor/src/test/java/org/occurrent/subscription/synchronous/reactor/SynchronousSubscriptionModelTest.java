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
import org.occurrent.condition.Condition;
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

    @Test
    void registering_the_same_subscription_id_twice_is_rejected() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("sub", cloudEvent -> Mono.empty());

        Throwable thrown = catchThrowable(() -> model.subscribe("sub", cloudEvent -> Mono.empty()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already registered");
    }

    @Test
    void a_started_subscription_handle_is_returned() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();

        var subscription = model.subscribe("sub", cloudEvent -> Mono.empty());

        assertThat(subscription.id()).isEqualTo("sub");
        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();
        StepVerifier.create(subscription.waitUntilStarted(Duration.ofSeconds(5))).expectNext(true).verifyComplete();
    }

    @Test
    void a_stopped_model_dispatches_to_nobody_and_the_mono_still_completes() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        model.stop();

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"))))
                .verifyComplete();
        assertThat(received).isEmpty();
        assertThat(model.isPaused("sub")).isTrue();
    }

    @Test
    void an_event_dispatched_while_paused_is_dropped_rather_than_delivered_on_resume() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        model.stop();
        StepVerifier.create(model.dispatch(List.of(cloudEvent("missed", "NameDefined")))).verifyComplete();
        model.resumeSubscription("sub");
        StepVerifier.create(model.dispatch(List.of(cloudEvent("seen", "NameDefined")))).verifyComplete();

        assertThat(received).containsExactly("seen");
    }

    @Test
    void a_paused_subscription_is_skipped_while_its_siblings_still_receive() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> quiet = new ArrayList<>();
        List<String> noisy = new ArrayList<>();
        model.subscribe("quiet", cloudEvent -> Mono.fromRunnable(() -> quiet.add(cloudEvent.getId())));
        model.subscribe("noisy", cloudEvent -> Mono.fromRunnable(() -> noisy.add(cloudEvent.getId())));

        model.pauseSubscription("quiet");

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")))).verifyComplete();
        assertThat(quiet).isEmpty();
        assertThat(noisy).containsExactly("1");
    }

    @Test
    void the_running_check_happens_when_the_mono_is_subscribed_not_when_it_is_assembled() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        Mono<Void> assembledWhileRunning = model.dispatch(List.of(cloudEvent("1", "NameDefined")));
        model.stop();

        StepVerifier.create(assembledWhileRunning).verifyComplete();
        assertThat(received).isEmpty();
    }

    @Test
    void without_a_transaction_an_erroring_handler_does_not_stop_the_handlers_behind_it() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.fromRunnable(() -> folded.add("first")));
        model.subscribe("second", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));
        model.subscribe("third", cloudEvent -> Mono.fromRunnable(() -> folded.add("third")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                // A single failure is emitted exactly as it was, so a caller matching on a type still sees it.
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
                    assertThat(error.getSuppressed()).isEmpty();
                });

        assertThat(folded).containsExactly("first", "third");
    }

    @Test
    void without_a_transaction_several_failures_are_reported_together() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.error(new IllegalStateException("first failed")));
        model.subscribe("second", cloudEvent -> Mono.fromRunnable(() -> folded.add("second")));
        model.subscribe("third", cloudEvent -> Mono.error(new UnsupportedOperationException("third failed")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("first failed");
                    assertThat(error.getSuppressed()).hasSize(1);
                    assertThat(error.getSuppressed()[0]).isInstanceOf(UnsupportedOperationException.class).hasMessage("third failed");
                });

        assertThat(folded).containsExactly("second");
    }

    @Test
    void inside_a_transaction_an_erroring_handler_stops_the_handlers_behind_it() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.fromRunnable(() -> folded.add("first")));
        model.subscribe("second", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));
        model.subscribe("third", cloudEvent -> Mono.fromRunnable(() -> folded.add("third")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), true))
                .verifyErrorMessage("handler failed");

        // The write is about to roll back, so running the handlers behind the failure would only do discarded work.
        assertThat(folded).containsExactly("first");
    }

    @Test
    void a_single_erroring_handler_reaches_the_caller_either_way() {
        List.of(true, false).forEach(transactional -> {
            SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
            model.subscribe("only", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

            StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), transactional))
                    .verifyErrorSatisfies(error -> {
                        assertThat(error).as("transactional=%s", transactional)
                                .isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
                        assertThat(error.getSuppressed()).as("transactional=%s", transactional).isEmpty();
                    });
        });
    }

    @Test
    void without_a_transaction_a_handler_that_failed_is_skipped_for_the_rest_of_the_batch() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("failing", cloudEvent -> Mono.fromRunnable(() -> folded.add("failing:" + cloudEvent.getId()))
                .then(Mono.error(new IllegalStateException("handler failed"))));
        model.subscribe("healthy", cloudEvent -> Mono.fromRunnable(() -> folded.add("healthy:" + cloudEvent.getId())));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged")), false))
                .verifyErrorMessage("handler failed");

        // Event 2 would be folded onto state that never saw event 1, so the failing handler does not get it. The
        // healthy one is unaffected and receives both.
        assertThat(folded).containsExactly("failing:1", "healthy:1", "healthy:2");
    }

    @Test
    void without_a_transaction_a_handler_that_throws_instead_of_returning_an_error_is_still_isolated() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.fromRunnable(() -> folded.add("first")));
        // Throws while building the Mono rather than returning Mono.error, which is what eager validation in a handler
        // looks like.
        model.subscribe("second", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });
        model.subscribe("third", cloudEvent -> Mono.fromRunnable(() -> folded.add("third")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyErrorMessage("handler failed");

        assertThat(folded).containsExactly("first", "third");
    }

    @Test
    void without_a_transaction_a_handler_error_that_is_not_an_exception_stops_the_batch() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.error(new AssertionError("not recoverable")));
        model.subscribe("second", cloudEvent -> Mono.fromRunnable(() -> folded.add("second")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyErrorSatisfies(error -> assertThat(error).isInstanceOf(AssertionError.class).hasMessage("not recoverable"));

        // Matches the blocking stack, where only a RuntimeException is collected and an Error keeps propagating.
        assertThat(folded).isEmpty();
    }

    @Test
    void without_a_transaction_a_checked_exception_from_a_handler_is_collected_like_any_other_failure() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.error(new java.io.IOException("io failed")));
        model.subscribe("second", cloudEvent -> Mono.fromRunnable(() -> folded.add("second")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyErrorSatisfies(error -> assertThat(error).isInstanceOf(java.io.IOException.class).hasMessage("io failed"));

        // Only this stack can produce one, since a Consumer cannot throw a checked exception.
        assertThat(folded).containsExactly("second");
    }

    @Test
    void without_a_transaction_a_filter_that_cannot_be_answered_only_costs_its_own_subscription() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        // No DataFieldReader was supplied, so this filter throws when it is evaluated rather than when it is registered.
        model.subscribe("payload-filtered", StreamSubscriptionFilter.filter(Filter.data("amount", Condition.eq(42))),
                cloudEvent -> Mono.fromRunnable(() -> folded.add("payload-filtered")));
        model.subscribe("plain", cloudEvent -> Mono.fromRunnable(() -> folded.add("plain")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("cannot query the data field"));

        assertThat(folded).containsExactly("plain");
    }

    @Test
    void without_a_transaction_two_handlers_failing_with_one_shared_exception_report_that_exception() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        RuntimeException shared = new IllegalStateException("shared failure");
        model.subscribe("first", cloudEvent -> Mono.error(shared));
        model.subscribe("second", cloudEvent -> Mono.error(shared));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                // Attaching the instance to itself would raise "Self-suppression not permitted" and hide both failures.
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(shared);
                    assertThat(error.getSuppressed()).isEmpty();
                });
    }

    @Test
    void without_a_transaction_each_subscription_of_the_returned_mono_starts_from_a_clean_slate() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("failing", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));
        model.subscribe("healthy", cloudEvent -> Mono.fromRunnable(() -> folded.add("healthy")));

        Mono<Void> dispatch = model.dispatch(List.of(cloudEvent("1", "NameDefined")), false);
        StepVerifier.create(dispatch).verifyErrorMessage("handler failed");
        StepVerifier.create(dispatch).verifyErrorMessage("handler failed");

        // The failure record is built per subscription, so the second run offers the failing handler the event again
        // rather than treating it as already broken, and reports the same error instead of completing empty.
        assertThat(folded).containsExactly("healthy", "healthy");
    }

    @Test
    void without_a_transaction_a_paused_subscription_is_still_skipped() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("quiet", cloudEvent -> Mono.fromRunnable(() -> folded.add("quiet")));
        model.subscribe("loud", cloudEvent -> Mono.fromRunnable(() -> folded.add("loud")));
        model.pauseSubscription("quiet");

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyComplete();

        assertThat(folded).containsExactly("loud");
    }

    @Test
    void without_a_transaction_a_stopped_model_dispatches_to_nobody() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> folded.add("sub")));
        model.stop();

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined")), false))
                .verifyComplete();

        assertThat(folded).isEmpty();
    }

    @Test
    void dispatch_without_the_transaction_argument_keeps_stopping_at_the_first_failure() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> folded = new ArrayList<>();
        model.subscribe("first", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));
        model.subscribe("second", cloudEvent -> Mono.fromRunnable(() -> folded.add("second")));

        StepVerifier.create(model.dispatch(List.of(cloudEvent("1", "NameDefined"))))
                .verifyErrorMessage("handler failed");

        // The single-argument overload is unchanged, so anything driving the model directly behaves as it always did.
        assertThat(folded).isEmpty();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
