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
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.subscription.RoutingOutcome.DELIVERED;
import static org.occurrent.subscription.RoutingOutcome.FILTERED;
import static org.occurrent.subscription.RoutingOutcome.NOT_DELIVERABLE;
import static org.occurrent.subscription.RoutingOutcome.UNAVAILABLE;

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
    void the_observer_is_told_delivered_once_the_handler_has_run() {
        // Reporting after the action runs, rather than before it, is what lets a catch-up-then-live engine tell
        // DELIVERED and DEFERRED apart accurately instead of assuming delivery ahead of the fold. A direct
        // dispatch such as this one has already run its handler by the time the observer is told.
        List<String> observed = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> observed.add(cloudEvent.getId() + ":" + outcome + ":" + handled.size()));
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(observed).containsExactly("1:DELIVERED:1");
        assertThat(handled).containsExactly("1");
    }

    @Test
    void the_observer_call_is_deferred_until_subscribe_not_when_the_mono_is_assembled() {
        // Registers only after accept(..) has already built the Mono, and before it is subscribed. Proves the
        // observer and the match check both run on subscribe, not on assembly, since an eager implementation would
        // see no registration yet and record UNAVAILABLE.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));

        Mono<Void> pending = model.accept(cloudEvent("1", "NameDefined"));
        model.subscribe("sub", cloudEvent -> Mono.empty());

        StepVerifier.create(pending).verifyComplete();

        assertThat(outcomes).containsExactly(DELIVERED);
    }

    @Test
    void resubscribing_the_same_mono_observes_and_dispatches_the_event_again() {
        // The Mono accept(..) returns is cold, the same way route(CloudEvent)'s own Mono already is, so "once per
        // event" means once per subscription to it, not once per event handed to accept(..).
        List<String> observed = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> observed.add(cloudEvent.getId()));
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        Mono<Void> pending = model.accept(cloudEvent("1", "NameDefined"));
        StepVerifier.create(pending).verifyComplete();
        StepVerifier.create(pending).verifyComplete();

        assertThat(observed).containsExactly("1", "1");
        assertThat(handled).containsExactly("1", "1");
    }

    @Test
    void the_observer_is_told_unavailable_when_nothing_is_registered() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(outcomes).containsExactly(UNAVAILABLE);
    }

    @Test
    void the_observer_is_told_unavailable_while_the_model_is_stopped() {
        // A stopped model drops live events by design (ADR 85), and the observer contract mirrors that: the
        // outcome reflects what would actually be delivered, not merely what the filter would have accepted.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", cloudEvent -> Mono.empty());
        model.stop();

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(outcomes).containsExactly(UNAVAILABLE);
    }

    @Test
    void the_observer_is_told_unavailable_while_the_subscription_is_paused_on_a_running_model() {
        // Distinct from the stopped case above, and distinct from FILTERED: a paused subscription's filter is never
        // consulted, so reporting FILTERED here would tell a caller the event was this subscription's and it was
        // declined, when in truth nothing decided that.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", cloudEvent -> Mono.empty());
        model.pauseSubscription("sub");

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(outcomes).containsExactly(UNAVAILABLE);
    }

    @Test
    void the_observer_is_told_filtered_when_the_registered_filter_declines_it() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(outcomes).containsExactly(FILTERED);
    }

    @Test
    void the_observer_still_sees_the_event_when_the_matching_handler_errors() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("boom", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMessage("handler failed");

        assertThat(outcomes).containsExactly(DELIVERED);
    }

    @Test
    void the_observer_still_sees_the_event_when_evaluating_the_filter_itself_throws() {
        // A supplied DataFieldReader can throw while reading the payload, the same hazard the shared dispatch loop
        // documents (routeIsolated). The "every event is observed" promise has to survive that too, not just a
        // handler that errors, and the original error still has to reach the caller afterward. Reported as
        // NOT_DELIVERABLE rather than FILTERED, since a filter that failed to answer did not decline the event.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMessage("payload unreadable");

        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void the_observer_still_sees_the_event_when_evaluating_the_filter_itself_fails_an_assertion() {
        // Same as the RuntimeException case above, but for a DataFieldReader instrumented as a test double, which is
        // as likely to throw AssertionError as a spy observer is.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new AssertionError("payload assertion failed");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(AssertionError.class)
                        .hasMessage("payload assertion failed"));

        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void the_observer_is_told_nothing_when_evaluating_the_filter_throws_a_checked_exception() {
        // The two tests above cover what the RuntimeException | AssertionError catch around the filter reaches. A
        // DataFieldReader written in Kotlin can throw a checked exception it never declared, which that catch
        // misses, so the observer is not told at all. The PushObserver javadoc says so, and this is what holds it
        // to that rather than a reading of the catch clause.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        Exception checked = new IOException("payload unreadable");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            sneakyThrow(checked);
            return Optional.empty();
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorMatches(error -> error == checked);

        assertThat(outcomes).as("the catch around the filter misses a checked exception, so nothing is reported")
                .isEmpty();
    }

    @Test
    void an_observer_error_while_reporting_a_filter_failure_is_suppressed_rather_than_replacing_it() {
        // A badly behaved observer must never be able to swap out the filter's own exception for its own. That
        // exception is the caller's redelivery signal, and reporting it to the observer must not risk losing it.
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader, (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
            throw new Error("observer blew up too");
        });
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("payload unreadable");
                    assertThat(error.getSuppressed()).hasSize(1);
                    assertThat(error.getSuppressed()[0]).isInstanceOf(Error.class).hasMessage("observer blew up too");
                });
    }

    @Test
    void a_shared_exception_instance_thrown_by_both_the_filter_and_the_observer_is_not_self_suppressed() {
        // Throwable.addSuppressed refuses to suppress an exception onto itself, throwing an IllegalArgumentException
        // instead. Left unguarded, that would replace the filter's own exception with an unrelated one, exactly the
        // failure the suppression in the test above exists to prevent.
        RuntimeException shared = new IllegalStateException("shared failure");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw shared;
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader, (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
            throw shared;
        });
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(shared);
                    assertThat(error.getSuppressed()).isEmpty();
                });
    }

    @Test
    void an_observer_error_while_reporting_delivered_for_a_handler_that_errored_is_suppressed_rather_than_replacing_it() {
        // DELIVERED reaches the observer both from the plain success path and from a handler that errored, and only
        // the second one already has a failure to propagate. The outcome alone does not say which, so what the observer is
        // told cannot decide where its own Error goes.
        RuntimeException handlerFailure = new IllegalStateException("handler failed");
        Error observerFailure = new Error("observer blew up too");
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
                    outcomes.add(outcome);
                    throw observerFailure;
                });
        model.subscribe("sub", cloudEvent -> Mono.error(handlerFailure));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).as("the handler failure is what the caller sees").isSameAs(handlerFailure);
                    assertThat(error.getSuppressed()).containsExactly(observerFailure);
                });

        assertThat(outcomes).containsExactly(DELIVERED);
    }

    @Test
    void an_observer_error_while_reporting_delivered_for_a_handler_that_completed_propagates_on_its_own() {
        // The other half of the same property. Nothing else is in flight, so the observer's Error is what the
        // caller sees, told the same DELIVERED as the test above.
        Error observerFailure = new Error("observer blew up");
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
                    outcomes.add(outcome);
                    throw observerFailure;
                });
        model.subscribe("sub", cloudEvent -> Mono.empty());

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined")))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(observerFailure);
                    assertThat(error.getSuppressed()).isEmpty();
                });

        assertThat(outcomes).containsExactly(DELIVERED);
    }

    @Test
    void a_report_in_a_batch_does_not_run_on_the_calling_thread() {
        // Event 1's DELIVERED is decided in the flatMap, on the thread the handler's Mono signalled. Event 2's
        // FILTERED is decided in the defer body, subscribed by concatMap on the thread event 1 finished on. Both
        // mechanisms run off the caller, which is why an interrupt flag a report sets cannot be promised to it.
        // The delay keeps this off a race with the calling thread's own drain loop, since the caller is parked in
        // block() once the handler ends.
        String callingThread = Thread.currentThread().getName();
        List<String> reportThreads = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> reportThreads.add(Thread.currentThread().getName()));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("NameDefined")),
                cloudEvent -> Mono.delay(Duration.ofMillis(50)).then());

        model.accept(List.of(cloudEvent("1", "NameDefined"), cloudEvent("2", "SomethingElseHappened"))).block();

        assertThat(reportThreads).as("both the DELIVERED report and the FILTERED report after it")
                .hasSize(2)
                .allSatisfy(reportThread -> assertThat(reportThread).isNotEqualTo(callingThread));
    }

    @Test
    void a_throwing_observer_is_swallowed_and_the_matching_handler_still_runs() {
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
            throw new AssertionError("observer assertion failed");
        });
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(handled).containsExactly("1");
    }

    @Test
    void an_observer_throwing_a_checked_exception_does_not_stop_the_rest_of_the_batch_from_being_routed() {
        List<String> handled = new ArrayList<>();
        Exception checked = new IOException("observer failed");
        // An observer written in Kotlin throws a checked exception like this without declaring it
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> sneakyThrow(checked));
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        Throwable thrown = catchThrowable(() -> model.accept(List.of(
                cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))).block());

        assertThat(handled).as("every event in the batch reaches the handler although the observer threw")
                .containsExactly("1", "2", "3");
        assertThat(thrown).as("the observer failure does not reach the caller, which would tell a broker to redeliver")
                .isNull();
    }

    @Test
    void an_observer_throwing_a_runtime_exception_does_not_stop_the_rest_of_the_batch_from_being_routed() {
        List<String> handled = new ArrayList<>();
        Exception unchecked = new IllegalStateException("observer failed");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> sneakyThrow(unchecked));
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));

        Throwable thrown = catchThrowable(() -> model.accept(List.of(
                cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))).block());

        assertThat(handled).as("every event in the batch reaches the handler although the observer threw")
                .containsExactly("1", "2", "3");
        assertThat(thrown).as("the observer failure does not reach the caller, which would tell a broker to redeliver")
                .isNull();
    }

    @Test
    void an_observer_throwing_a_checked_exception_while_being_told_filtered_does_not_stop_the_batch() {
        // The FILTERED report goes through the same notifyObserver as the delivered one, but from a call site
        // that does not suppress what the observer throws onto another failure, and nothing covered a
        // throwing observer there.
        List<RoutingOutcome> observed = new ArrayList<>();
        Exception checked = new IOException("observer failed");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
                    observed.add(outcome);
                    sneakyThrow(checked);
                });
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")), cloudEvent -> Mono.empty());

        Throwable thrown = catchThrowable(() -> model.accept(List.of(
                cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))).block());

        assertThat(observed).as("every event in the batch is still evaluated although the observer threw")
                .containsExactly(FILTERED, FILTERED, FILTERED);
        assertThat(thrown).as("the observer failure does not reach the caller, which would tell a broker to redeliver")
                .isNull();
    }

    @Test
    void an_observer_throwing_a_checked_exception_while_being_told_unavailable_does_not_stop_the_batch() {
        // The paused call site, which reports UNAVAILABLE before the matcher runs at all, is a third unguarded
        // notifyObserver call and was uncovered for a throwing observer too.
        List<RoutingOutcome> observed = new ArrayList<>();
        Exception checked = new IOException("observer failed");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
                    observed.add(outcome);
                    sneakyThrow(checked);
                });
        model.subscribe("sub", cloudEvent -> Mono.empty());
        model.pauseSubscription("sub");

        Throwable thrown = catchThrowable(() -> model.accept(List.of(
                cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))).block());

        assertThat(observed).as("every event in the batch is still evaluated although the observer threw")
                .containsExactly(UNAVAILABLE, UNAVAILABLE, UNAVAILABLE);
        assertThat(thrown).as("the observer failure does not reach the caller, which would tell a broker to redeliver")
                .isNull();
    }

    @Test
    void a_batch_stops_observing_once_a_handler_errors() {
        List<String> observed = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> observed.add(cloudEvent.getId()));
        model.subscribe("boom", cloudEvent -> cloudEvent.getId().equals("2")
                ? Mono.error(new IllegalStateException("handler failed"))
                : Mono.empty());

        StepVerifier.create(model.accept(List.of(
                        cloudEvent("1", "NameDefined"), cloudEvent("2", "NameDefined"), cloudEvent("3", "NameDefined"))))
                .verifyErrorMessage("handler failed");

        assertThat(observed).containsExactly("1", "2");
    }

    @Test
    void a_resume_landing_immediately_after_the_evaluation_does_not_change_the_outcome_already_reported() {
        // The race #848 names: a caller that checked isRunning(subscriptionId) *after* accept() completes, instead
        // of reading the outcome the observer was told *during* the one routing evaluation, could see a concurrent
        // resume make isRunning() answer true for an event that was actually dropped while paused. The observer
        // callback runs synchronously inside the same evaluation that decided UNAVAILABLE, so triggering the
        // resume from inside it is the earliest a "concurrent" resume could possibly land relative to accept()
        // completing, and the already-reported outcome must not be retroactively correct about a state that didn't
        // hold at evaluation time.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        var modelRef = new java.util.concurrent.atomic.AtomicReference<PushSubscriptionModel>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (cloudEvent, outcome) -> {
            outcomes.add(outcome);
            modelRef.get().resumeSubscription("sub");
        });
        modelRef.set(model);
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent.getId())));
        model.pauseSubscription("sub");

        StepVerifier.create(model.accept(cloudEvent("1", "NameDefined"))).verifyComplete();

        assertThat(outcomes).as("the outcome reported during evaluation reflects the paused state at that moment")
                .containsExactly(UNAVAILABLE);
        assertThat(handled).as("the event was genuinely dropped, never handed to the handler")
                .isEmpty();
        assertThat(model.isRunning("sub")).as("a caller checking isRunning(..) *after* accept() completes would now "
                        + "wrongly see true, which is exactly why the ack decision must come from the reported "
                        + "outcome and never from a state check taken after the fact")
                .isTrue();
    }

    @Test
    void concurrent_pause_and_resume_never_makes_the_reported_outcome_disagree_with_what_was_actually_delivered() throws InterruptedException {
        // A broader, genuinely multi-threaded version of the race above: one thread hammers accept() (subscribing
        // to each returned Mono synchronously) while another toggles pause/resume on the same subscription. Every
        // event pushed is one of two types, only one of which matches the subscription's filter, so a run exercises
        // FILTERED as well as DELIVERED and UNAVAILABLE, not just the two outcomes a filter that always matches
        // would produce. Whatever RoutingOutcome the observer is told for a given event must agree both with
        // whether that event actually reached the handler and with whether its type was one the filter accepts,
        // for every one of many interleavings, not just the hand-picked one above.
        int eventCount = 2_000;
        String matchingType = "NameDefined";
        String nonMatchingType = "SomethingElseHappened";
        List<RoutingOutcome> outcomes = new ArrayList<>(eventCount);
        List<String> types = new ArrayList<>(eventCount);
        Set<String> deliveredIds = ConcurrentHashMap.newKeySet();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (cloudEvent, outcome) -> {
            synchronized (outcomes) {
                outcomes.add(outcome);
                types.add(cloudEvent.getType());
            }
        });
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type(matchingType)),
                cloudEvent -> Mono.fromRunnable(() -> deliveredIds.add(cloudEvent.getId())));

        // A deterministic warm-up, run unpaused before the race starts, so DELIVERED and FILTERED are proven to
        // occur regardless of how the toggler and pusher threads happen to interleave below. Left to the race
        // alone, an unlucky schedule (the toggler pauses once and is never rescheduled before the pusher finishes)
        // could leave the subscription paused for the whole run and report every event UNAVAILABLE, which
        // would fail the two-outcome assertion further down despite nothing being wrong.
        model.accept(cloudEvent("warmup-match", matchingType)).block();
        model.accept(cloudEvent("warmup-no-match", nonMatchingType)).block();
        assertThat(outcomes).containsExactly(DELIVERED, FILTERED);
        outcomes.clear();
        types.clear();
        deliveredIds.clear();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);

            var toggler = executor.submit(() -> {
                ready.countDown();
                await(go);
                for (int i = 0; i < eventCount; i++) {
                    if (model.isPaused("sub")) {
                        model.resumeSubscription("sub");
                    } else {
                        model.pauseSubscription("sub");
                    }
                }
            });
            var pusher = executor.submit(() -> {
                ready.countDown();
                await(go);
                for (int i = 0; i < eventCount; i++) {
                    String type = i % 2 == 0 ? matchingType : nonMatchingType;
                    model.accept(cloudEvent(String.valueOf(i), type)).block();
                }
            });

            ready.await();
            go.countDown();
            toggler.get(30, TimeUnit.SECONDS);
            pusher.get(30, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new AssertionError(e);
        } finally {
            executor.shutdownNow();
        }

        assertThat(outcomes).hasSize(eventCount);
        for (int i = 0; i < eventCount; i++) {
            RoutingOutcome outcome = outcomes.get(i);
            boolean typeMatches = types.get(i).equals(matchingType);
            boolean wasDelivered = deliveredIds.contains(String.valueOf(i));
            if (typeMatches) {
                assertThat(outcome).as("event %d has the matching type, so its filter is never the reason it is not delivered", i)
                        .isIn(DELIVERED, UNAVAILABLE);
            } else {
                assertThat(outcome).as("event %d has the non-matching type, so a running subscription always declines it", i)
                        .isIn(FILTERED, UNAVAILABLE);
            }
            assertThat(wasDelivered).as("event %d: whether the handler actually ran must agree with a reported outcome of DELIVERED", i)
                    .isEqualTo(outcome == DELIVERED);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
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

    // A broker listener acknowledges when acceptRedeliverable completes, so every event nothing applied has to error
    @Test
    void accept_redeliverable_errors_when_nothing_is_registered() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined")))
                .expectErrorSatisfies(error -> assertNotAccepted(error, UNAVAILABLE))
                .verify(Duration.ofSeconds(5));

        assertThat(outcomes).containsExactly(UNAVAILABLE);
    }

    @Test
    void accept_redeliverable_errors_while_the_model_is_stopped() {
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
        model.stop();

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined")))
                .expectErrorSatisfies(error -> assertNotAccepted(error, UNAVAILABLE))
                .verify(Duration.ofSeconds(5));

        assertThat(received).isEmpty();
    }

    @Test
    void accept_redeliverable_errors_while_the_subscription_is_paused() {
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
        model.pauseSubscription("sub");

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined")))
                .expectErrorSatisfies(error -> assertNotAccepted(error, UNAVAILABLE))
                .verify(Duration.ofSeconds(5));

        assertThat(received).isEmpty();
    }

    @Test
    void accept_redeliverable_completes_only_after_the_handler_has_run() {
        List<String> received = new CopyOnWriteArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel();
        // The delay puts the handler's side effect after the call returns, so completing any earlier shows up as a
        // missing id
        model.subscribe("sub", cloudEvent -> Mono.delay(Duration.ofMillis(50))
                .then(Mono.fromRunnable(() -> received.add(cloudEvent.getId()))));

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined"))).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(received).containsExactly("1");
    }

    @Test
    void accept_redeliverable_completes_for_an_event_the_filter_declines() {
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined"))).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(received).isEmpty();
    }

    @Test
    void accept_redeliverable_errors_with_the_handlers_own_failure() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribe("boom", cloudEvent -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined")))
                .expectErrorSatisfies(error -> assertThat(error).isNotInstanceOf(EventNotAcceptedException.class).hasMessage("handler failed"))
                .verify(Duration.ofSeconds(5));
    }

    // A routing action that completes empty reports no outcome, and no outcome proves nothing was applied
    @Test
    void accept_redeliverable_errors_when_the_routing_action_reports_no_outcome() {
        PushSubscriptionModel model = new PushSubscriptionModel();
        model.subscribeCatchupThenPush("sub", null, StartAt.subscriptionModelDefault(), (cloudEvent, bufferIfNotLive) -> Mono.empty());

        StepVerifier.create(model.acceptRedeliverable(cloudEvent("1", "NameDefined")))
                .expectErrorSatisfies(error -> assertThat(error).isExactlyInstanceOf(IllegalStateException.class).hasMessageContaining("No routing outcome"))
                .verify(Duration.ofSeconds(5));
    }

    private static void assertNotAccepted(Throwable error, RoutingOutcome expected) {
        assertThat(error).isInstanceOfSatisfying(EventNotAcceptedException.class, notAccepted -> assertThat(notAccepted.outcome()).isEqualTo(expected));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
