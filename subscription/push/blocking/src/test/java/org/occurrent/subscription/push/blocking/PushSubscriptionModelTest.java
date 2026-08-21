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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.occurrent.subscription.RoutingOutcome.DELIVERED;
import static org.occurrent.subscription.RoutingOutcome.FILTERED;
import static org.occurrent.subscription.RoutingOutcome.NOT_DELIVERABLE;

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
    void a_subclass_that_delegates_accept_to_the_batch_overload_does_not_recurse() {
        // PushSubscriptionModel is public and not final, so a subclass overriding accept(CloudEvent) to hand a
        // singleton list to accept(Iterable) is a legitimate pattern. The batch loop must never call back into the
        // overridable accept(CloudEvent), or this would recurse until the stack overflows.
        List<String> received = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel() {
            @Override
            public void accept(CloudEvent cloudEvent) {
                accept(List.of(cloudEvent));
            }
        };
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        model.accept(cloudEvent("1", "NameDefined"));

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
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(observed).containsExactly("1:DELIVERED:1");
        assertThat(handled).containsExactly("1");
    }

    @Test
    void the_observer_is_still_told_delivered_when_the_handler_throws_and_the_original_exception_still_propagates() {
        RuntimeException handlerFailure = new IllegalStateException("handler failed");
        List<RoutingOutcome> observed = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> observed.add(outcome));
        model.subscribe("sub", cloudEvent -> {
            throw handlerFailure;
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
        assertThat(thrown).isSameAs(handlerFailure);
    }

    @Test
    void the_observer_is_told_not_deliverable_when_nothing_is_registered() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void the_observer_is_told_not_deliverable_while_the_model_is_stopped() {
        // A stopped model drops live events by design (ADR 85), and the observer contract mirrors that: the
        // outcome reflects what would actually be delivered, not merely what the filter would have accepted.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", cloudEvent -> {
        });
        model.stop();

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void the_observer_is_told_not_deliverable_while_the_subscription_is_paused_on_a_running_model() {
        // Distinct from the stopped case above, and distinct from FILTERED: a paused subscription's filter is never
        // consulted, so reporting FILTERED here would tell a caller the event was this subscription's and it was
        // declined, when in truth nothing decided that.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", cloudEvent -> {
        });
        model.pauseSubscription("sub");

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void the_observer_is_told_filtered_when_the_registered_filter_declines_it() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("SomethingElseHappened")), cloudEvent -> {
        });

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(outcomes).containsExactly(FILTERED);
    }

    @Test
    void the_observer_still_sees_the_event_when_the_matching_handler_throws() {
        List<RoutingOutcome> outcomes = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("boom", cloudEvent -> {
            throw new IllegalStateException("handler failed");
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(outcomes).containsExactly(DELIVERED);
    }

    @Test
    void the_observer_still_sees_the_event_when_evaluating_the_filter_itself_throws() {
        // A supplied DataFieldReader can throw while reading the payload, the same hazard the shared dispatch loop
        // documents (routeIsolated). The "every event is observed" promise has to survive that too, not just a
        // handler that throws, and the original exception still has to reach the caller afterward. Reported as
        // NOT_DELIVERABLE rather than FILTERED, since a filter that failed to answer did not decline the event.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        PushSubscriptionModel model = new PushSubscriptionModel(throwingReader,
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> outcomes.add(outcome));
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("payload unreadable");
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
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(AssertionError.class).hasMessage("payload assertion failed");
        assertThat(outcomes).containsExactly(NOT_DELIVERABLE);
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
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("payload unreadable");
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertThat(thrown.getSuppressed()[0]).isInstanceOf(Error.class).hasMessage("observer blew up too");
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
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> {
        });

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void a_throwing_observer_is_swallowed_and_the_matching_handler_still_runs() {
        List<String> handled = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent cloudEvent, RoutingOutcome outcome) -> {
            throw new IllegalStateException("observer failed");
        });
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isNull();
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
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isNull();
        assertThat(handled).containsExactly("1");
    }

    @Test
    void a_batch_stops_observing_once_a_handler_throws() {
        List<String> observed = new ArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(),
                (CloudEvent cloudEvent, RoutingOutcome outcome) -> observed.add(cloudEvent.getId()));
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
    void a_resume_landing_immediately_after_the_evaluation_does_not_change_the_outcome_already_reported() {
        // The race #848 names: a caller that checked isRunning(subscriptionId) *after* accept() returns, instead of
        // reading the outcome the observer was told *during* the one routing evaluation, could see a concurrent
        // resume make isRunning() answer true for an event that was actually dropped while paused. The observer
        // callback runs synchronously inside the same evaluation that decided NOT_DELIVERABLE, so triggering the
        // resume from inside it is the earliest a "concurrent" resume could possibly land relative to accept()
        // returning, and the already-reported outcome must not be retroactively correct about a state that didn't
        // hold at evaluation time.
        List<RoutingOutcome> outcomes = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        var modelRef = new java.util.concurrent.atomic.AtomicReference<PushSubscriptionModel>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), (cloudEvent, outcome) -> {
            outcomes.add(outcome);
            modelRef.get().resumeSubscription("sub");
        });
        modelRef.set(model);
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));
        model.pauseSubscription("sub");

        model.accept(cloudEvent("1", "NameDefined"));

        assertThat(outcomes).as("the outcome reported during evaluation reflects the paused state at that moment")
                .containsExactly(NOT_DELIVERABLE);
        assertThat(handled).as("the event was genuinely dropped, never handed to the handler")
                .isEmpty();
        assertThat(model.isRunning("sub")).as("a caller checking isRunning(..) *after* accept() returns would now "
                        + "wrongly see true, which is exactly why the ack decision must come from the reported "
                        + "outcome and never from a state check taken after the fact")
                .isTrue();
    }

    @Test
    void concurrent_pause_and_resume_never_makes_the_reported_outcome_disagree_with_what_was_actually_delivered() throws InterruptedException {
        // A broader, genuinely multi-threaded version of the race above: one thread hammers accept() while another
        // toggles pause/resume on the same subscription. Every event pushed is one of two types, only one of which
        // matches the subscription's filter, so a run exercises FILTERED as well as DELIVERED and NOT_DELIVERABLE,
        // not just the two outcomes a filter that always matches would produce. Whatever RoutingOutcome the observer
        // is told for a given event must agree both with whether that event actually reached the handler and with
        // whether its type was one the filter accepts, for every one of many interleavings, not just the
        // hand-picked one above.
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
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type(matchingType)), cloudEvent -> deliveredIds.add(cloudEvent.getId()));

        // A deterministic warm-up, run unpaused before the race starts, so DELIVERED and FILTERED are proven to
        // occur regardless of how the toggler and pusher threads happen to interleave below. Left to the race
        // alone, an unlucky schedule (the toggler pauses once and is never rescheduled before the pusher finishes)
        // could leave the subscription paused for the whole run and report every event NOT_DELIVERABLE, which
        // would fail the two-outcome assertion further down despite nothing being wrong.
        model.accept(cloudEvent("warmup-match", matchingType));
        model.accept(cloudEvent("warmup-no-match", nonMatchingType));
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
                    model.accept(cloudEvent(String.valueOf(i), type));
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
                        .isIn(DELIVERED, NOT_DELIVERABLE);
            } else {
                assertThat(outcome).as("event %d has the non-matching type, so a running subscription always declines it", i)
                        .isIn(FILTERED, NOT_DELIVERABLE);
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
        model.subscribe("sub", cloudEvent -> received.add(cloudEvent.getId()));

        Throwable thrown = catchThrowable(() -> model.accept(cloudEvent("1", "NameDefined")));

        assertThat(thrown).isNull();
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
