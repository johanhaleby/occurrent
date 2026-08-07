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
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelTest {

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
    void registering_on_a_stopped_model_yields_a_paused_subscription() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> received = new ArrayList<>();

        model.stop();
        var registered = model.subscribe("registered-while-stopped", cloudEvent -> received.add(cloudEvent.getId()));

        assertThat(model.isPaused("registered-while-stopped")).isTrue();
        assertThat(registered.waitUntilStarted(java.time.Duration.ofMillis(1)))
                .as("nothing has started yet: the registration only reserved the id and left it paused")
                .isFalse();
        var started = model.resumeSubscription("registered-while-stopped");
        assertThat(started.waitUntilStarted(java.time.Duration.ofMillis(1))).isTrue();
        model.dispatch(List.of(cloudEvent("1", "NameDefined")));
        assertThat(received).containsExactly("1");
    }

    @Test
    void resuming_a_subscription_that_is_not_paused_fails() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("sub", cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("sub"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("is not paused");
    }

    @Test
    void without_a_transaction_a_throwing_handler_does_not_stop_the_handlers_behind_it() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("first", cloudEvent -> handled.add("first"));
        model.subscribe("second", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });
        model.subscribe("third", cloudEvent -> handled.add("third"));

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), false));

        assertThat(handled).containsExactly("first", "third");
        // A single failure is rethrown exactly as it was, so a caller catching a specific type still sees it.
        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void without_a_transaction_several_failures_are_reported_together() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("first", cloudEvent -> {
            throw new IllegalStateException("first failed");
        });
        model.subscribe("second", cloudEvent -> handled.add("second"));
        model.subscribe("third", cloudEvent -> {
            throw new UnsupportedOperationException("third failed");
        });

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), false));

        assertThat(handled).containsExactly("second");
        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("first failed");
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertThat(thrown.getSuppressed()[0]).isInstanceOf(UnsupportedOperationException.class).hasMessage("third failed");
    }

    @Test
    void inside_a_transaction_a_throwing_handler_stops_the_handlers_behind_it() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("first", cloudEvent -> handled.add("first"));
        model.subscribe("second", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });
        model.subscribe("third", cloudEvent -> handled.add("third"));

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), true));

        // The write is about to roll back, so running the handlers behind the failure would only do discarded work.
        assertThat(handled).containsExactly("first");
        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
    }

    @Test
    void a_single_throwing_handler_reaches_the_caller_either_way() {
        List.of(true, false).forEach(transactional -> {
            SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
            model.subscribe("only", cloudEvent -> {
                throw new IllegalStateException("handler failed");
            });

            Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), transactional));

            assertThat(thrown).as("transactional=%s", transactional)
                    .isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
            assertThat(thrown.getSuppressed()).as("transactional=%s", transactional).isEmpty();
        });
    }

    @Test
    void without_a_transaction_a_handler_that_failed_is_skipped_for_the_rest_of_the_batch() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("failing", cloudEvent -> {
            handled.add("failing:" + cloudEvent.getId());
            throw new IllegalStateException("handler failed");
        });
        model.subscribe("healthy", cloudEvent -> handled.add("healthy:" + cloudEvent.getId()));

        catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged")), false));

        // The failing handler does not get event 2, because it would update its read model from event 2 without event 1. The
        // healthy one is unaffected and receives both.
        assertThat(handled).containsExactly("failing:1", "healthy:1", "healthy:2");
    }

    @Test
    void without_a_transaction_a_filter_that_cannot_be_answered_only_costs_its_own_subscription() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        // No DataFieldReader was supplied, so this filter throws when it is evaluated rather than when it is registered.
        model.subscribe("payload-filtered", StreamSubscriptionFilter.filter(Filter.data("amount", Condition.eq(42))),
                cloudEvent -> handled.add("payload-filtered"));
        model.subscribe("plain", cloudEvent -> handled.add("plain"));

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), false));

        assertThat(handled).containsExactly("plain");
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot query the data field");
    }

    @Test
    void without_a_transaction_two_handlers_failing_with_one_shared_exception_report_that_exception() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        RuntimeException shared = new IllegalStateException("shared failure");
        model.subscribe("first", cloudEvent -> {
            throw shared;
        });
        model.subscribe("second", cloudEvent -> {
            throw shared;
        });

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), false));

        // Attaching the instance to itself would raise "Self-suppression not permitted" and hide both failures.
        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void without_a_transaction_a_handler_error_that_is_not_an_exception_stops_the_batch() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("first", cloudEvent -> {
            throw new AssertionError("not recoverable");
        });
        model.subscribe("second", cloudEvent -> handled.add("second"));

        Throwable thrown = catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined")), false));

        assertThat(thrown).isInstanceOf(AssertionError.class).hasMessage("not recoverable");
        assertThat(handled).isEmpty();
    }

    @Test
    void without_a_transaction_a_handler_resubscribed_under_a_freed_id_does_not_inherit_the_failure() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("swapped", cloudEvent -> {
            handled.add("original:" + cloudEvent.getId());
            throw new IllegalStateException("handler failed");
        });
        // Cancelling frees the id, so the replacement registers under the same one.
        model.subscribe("swapper", cloudEvent -> {
            if (model.subscriptionIds().contains("swapped")) {
                model.cancelSubscription("swapped");
                model.subscribe("swapped", replayed -> handled.add("replacement:" + replayed.getId()));
            }
        });

        catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "NameWasChanged")), false));

        assertThat(handled).containsExactly("original:1", "replacement:2");
    }

    @Test
    void without_a_transaction_a_paused_subscription_is_still_skipped() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("quiet", cloudEvent -> handled.add("quiet"));
        model.subscribe("loud", cloudEvent -> handled.add("loud"));
        model.pauseSubscription("quiet");

        model.dispatch(List.of(cloudEvent("1", "NameDefined")), false);

        assertThat(handled).containsExactly("loud");
    }

    @Test
    void without_a_transaction_a_stopped_model_dispatches_to_nobody() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add("sub"));
        model.stop();

        model.dispatch(List.of(cloudEvent("1", "NameDefined")), false);

        assertThat(handled).isEmpty();
    }

    @Test
    void dispatch_without_the_transaction_argument_keeps_stopping_at_the_first_failure() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        List<String> handled = new ArrayList<>();
        model.subscribe("first", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });
        model.subscribe("second", cloudEvent -> handled.add("second"));

        catchThrowable(() -> model.dispatch(List.of(cloudEvent("1", "NameDefined"))));

        // The single-argument overload is unchanged, so anything driving the model directly behaves as it always did.
        assertThat(handled).isEmpty();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
