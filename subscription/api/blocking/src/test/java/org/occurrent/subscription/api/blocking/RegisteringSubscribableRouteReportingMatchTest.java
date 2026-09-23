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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

/**
 * Exercises {@link RegisteringSubscribable#routeReportingMatch(CloudEvent, boolean, BiConsumer)} directly, with a
 * raw {@code matchObserver} that has no swallowing of its own. {@code PushSubscriptionModel}'s own
 * {@code notifyObserver} already catches any {@code Exception} or an {@code AssertionError} from the configured
 * {@code PushObserver} before it could ever reach {@code routeReportingMatch}'s own guard against a shared
 * exception instance, so that guard is unreachable through {@code PushSubscriptionModel} and needs a caller here
 * that does not have PushSubscriptionModel's own protection layer in the way.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RegisteringSubscribableRouteReportingMatchTest {

    @Test
    void a_shared_exception_instance_thrown_by_the_matcher_and_the_matchObserver_is_not_self_suppressed() {
        RuntimeException shared = new IllegalStateException("shared failure");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw shared;
        };
        RawConsumersOneModel model = new RawConsumersOneModel(throwingReader);
        model.subscribeRaw("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), (cloudEvent, bufferIfNotLive) -> true);

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), true, (cloudEvent, outcome) -> {
            throw shared;
        }));

        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_matchers_exception() {
        RuntimeException matcherFailure = new IllegalStateException("matcher failed");
        Error observerFailure = new Error("matchObserver failed too");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw matcherFailure;
        };
        RawConsumersOneModel model = new RawConsumersOneModel(throwingReader);
        model.subscribeRaw("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), (cloudEvent, bufferIfNotLive) -> true);

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), true, (cloudEvent, outcome) -> {
            throw observerFailure;
        }));

        assertThat(thrown).isSameAs(matcherFailure);
        assertThat(thrown.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void a_matchObserver_throwing_a_checked_exception_is_attached_to_the_matchers_exception() {
        RuntimeException matcherFailure = new IllegalStateException("matcher failed");
        Exception observerFailure = new IOException("matchObserver failed too");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw matcherFailure;
        };
        RawConsumersOneModel model = new RawConsumersOneModel(throwingReader);
        model.subscribeRaw("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), (cloudEvent, bufferIfNotLive) -> true);

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), true, (cloudEvent, outcome) -> sneakyThrow(observerFailure)));

        assertThat(thrown).as("the matcher failure is what the caller sees").isSameAs(matcherFailure);
        assertThat(thrown.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void reports_delivered_when_the_action_reports_it_landed() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> true);

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    @Test
    void reports_deferred_when_the_action_declines_to_hand_the_event_over() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> false);

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(observed).containsExactly(RoutingOutcome.DEFERRED);
    }

    @Test
    void the_bufferIfNotLive_argument_reaches_the_registered_action_unchanged() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        List<Boolean> seen = new ArrayList<>();
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            seen.add(bufferIfNotLive);
            return true;
        });

        model.acceptRaw(cloudEvent("1"), true, (cloudEvent, outcome) -> {
        });
        model.acceptRaw(cloudEvent("2"), false, (cloudEvent, outcome) -> {
        });

        assertThat(seen).containsExactly(true, false);
    }

    @Test
    void the_matchObserver_still_reports_delivered_and_the_original_exception_still_propagates_when_the_action_throws() {
        RuntimeException actionFailure = new IllegalStateException("action failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
        assertThat(thrown).isSameAs(actionFailure);
    }

    @Test
    void the_matchObserver_still_reports_delivered_and_the_original_error_still_propagates_when_the_action_throws_an_assertion_error() {
        AssertionError actionFailure = new AssertionError("action failed an assertion");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
        assertThat(thrown).isSameAs(actionFailure);
    }

    /**
     * The regression this guards: a Copilot review of this PR found that a throwing {@code matchObserver} here
     * replaced {@code actionFailure} with its own failure, since {@code throw e} was never reached. The original
     * failure must still propagate, with the observer's failure attached rather than discarded.
     */
    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_actions_exception() {
        RuntimeException actionFailure = new IllegalStateException("action failed");
        Error observerFailure = new Error("matchObserver failed too");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> {
            throw observerFailure;
        }));

        assertThat(thrown).isSameAs(actionFailure);
        assertThat(thrown.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void a_matchObserver_throwing_a_checked_exception_is_attached_to_the_actions_exception() {
        RuntimeException actionFailure = new IllegalStateException("action failed");
        Exception observerFailure = new IOException("matchObserver failed too");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> sneakyThrow(observerFailure)));

        assertThat(thrown).as("the action failure is what the caller sees").isSameAs(actionFailure);
        assertThat(thrown.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void a_shared_exception_instance_thrown_by_the_action_and_the_matchObserver_is_not_self_suppressed() {
        RuntimeException shared = new IllegalStateException("shared failure");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw shared;
        });

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> {
            throw shared;
        }));

        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    /**
     * The regression this guards: a refusal decided before any dispatch was attempted (an engine-level guard, not
     * a handler that ran) must never be reported as {@link RoutingOutcome#DELIVERED}, the mistake a Copilot review
     * of this PR caught in {@code BlockingHandover.acceptIfLive}'s {@code catchUpFailure} case, which reaches this
     * exact path through {@code CatchupThenPushSubscriptionModel}'s registered action.
     */
    @Test
    void a_permanent_routing_action_refusal_reports_refused_and_the_wrapped_cause_still_propagates() {
        RuntimeException refusalCause = new IllegalStateException("catch-up has failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, true);
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(observed).containsExactly(RoutingOutcome.REFUSED);
        assertThat(thrown).isSameAs(refusalCause);
    }

    /**
     * The regression this guards: a Copilot review of this PR found that a throwing {@code matchObserver} here
     * replaced the wrapped {@code refusalCause} with its own failure, since {@code throw refusalCause} was never
     * reached. The wrapped cause must still propagate, with the observer's failure attached rather than discarded.
     */
    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_refusals_wrapped_cause() {
        RuntimeException refusalCause = new IllegalStateException("catch-up has failed");
        Error observerFailure = new Error("matchObserver failed too");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, true);
        });

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> {
            throw observerFailure;
        }));

        assertThat(thrown).isSameAs(refusalCause);
        assertThat(thrown.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void a_shared_exception_instance_thrown_by_the_refusals_wrapped_cause_and_the_matchObserver_is_not_self_suppressed() {
        RuntimeException shared = new IllegalStateException("catch-up has failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw new RegisteringSubscribable.RoutingAction.Refusal(shared, true);
        });

        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> {
            throw shared;
        }));

        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void subscribe_ignores_bufferIfNotLive_and_always_reports_delivered() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        List<CloudEvent> received = new ArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), received::add);

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(received).extracting(CloudEvent::getId).containsExactly("1");
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    /**
     * Nothing covered the three lifecycle states before this. They are the only outcomes a caller can see without
     * an exception coming with them, which is what lets a broker bridge hold and pace them instead of sending them
     * through a failure policy, so each one is asserted on its own rather than through a shared helper.
     */
    @Test
    void nothing_registered_reports_unavailable_and_throws_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(observed).containsExactly(RoutingOutcome.UNAVAILABLE);
    }

    @Test
    void a_stopped_model_reports_unavailable_and_throws_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            throw new IllegalStateException("the handler must never run");
        });
        model.stop();

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(observed).containsExactly(RoutingOutcome.UNAVAILABLE);
    }

    @Test
    void a_paused_subscription_reports_unavailable_and_throws_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            throw new IllegalStateException("the handler must never run");
        });
        model.pauseSubscription("sub");

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

        assertThat(observed).containsExactly(RoutingOutcome.UNAVAILABLE);
    }

    /**
     * The matcher throwing is the one case that reports NOT_DELIVERABLE, and it always comes with the matcher's own
     * exception. A caller telling a lifecycle state from a broken filter reads exactly that difference.
     */
    @Test
    void a_matcher_that_throws_reports_not_deliverable_and_its_exception_propagates() {
        RuntimeException matcherFailure = new IllegalStateException("the filter cannot answer");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw matcherFailure;
        };
        RawConsumersOneModel model = new RawConsumersOneModel(throwingReader);
        model.subscribeRaw("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), (cloudEvent, bufferIfNotLive) -> true);

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
        assertThat(thrown).isSameAs(matcherFailure);
    }

    /**
     * A refusal the action does not promise is permanent, a full live buffer while a replay is still running, say.
     * It reports NOT_DELIVERABLE rather than REFUSED, so a caller sends it through its failure policy instead of
     * stopping for good.
     */
    @Test
    void a_transient_routing_action_refusal_reports_not_deliverable_and_the_wrapped_cause_still_propagates() {
        RuntimeException refusalCause = new IllegalStateException("the live buffer is full");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, false);
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
        assertThat(thrown).isSameAs(refusalCause);
    }

    /**
     * #1116's other half. This is the behaviour the reactor stack was changed to match. The
     * {@code RuntimeException | AssertionError} catch misses an {@link Error}, so {@code matchObserver} is told
     * nothing about an action that ended in one.
     */
    @Test
    void an_action_that_throws_an_error_reports_nothing_and_that_error_still_propagates() {
        Error actionFailure = new NotAVirtualMachineError();
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(thrown).isSameAs(actionFailure);
        assertThat(observed).as("an action that ended in an Error delivered nothing, so DELIVERED would be a lie")
                .isEmpty();
    }

    /**
     * The control for the test above, and the reason the reactor skip excludes {@link AssertionError} too.
     */
    @Test
    void an_action_that_throws_an_assertion_error_is_still_reported_delivered() {
        AssertionError actionFailure = new AssertionError("the handler's own assertion failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, (cloudEvent, bufferIfNotLive) -> {
            throw actionFailure;
        });

        List<RoutingOutcome> observed = new ArrayList<>();
        Throwable thrown = catchThrowable(() -> model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome)));

        assertThat(thrown).isSameAs(actionFailure);
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("NameDefined")
                .build();
    }

    // A plain Error, deliberately not a VirtualMachineError, ThreadDeath or LinkageError: Reactor's
    // Exceptions.throwIfFatal rethrows those three rather than turning them into an error signal, which
    // would prove nothing about what matchObserver is told.
    private static final class NotAVirtualMachineError extends Error {
        NotAVirtualMachineError() {
            super("the action failed with an Error");
        }
    }

    private static final class RawConsumersOneModel extends RegisteringSubscribable {
        RawConsumersOneModel(DataFieldReader dataFieldReader) {
            super(Consumers.ONE, dataFieldReader);
        }

        void subscribeRaw(String subscriptionId, @Nullable SubscriptionFilter filter, RoutingAction action) {
            subscribeReportingDelivery(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
        }

        void acceptRaw(CloudEvent cloudEvent, boolean bufferIfNotLive, BiConsumer<CloudEvent, RoutingOutcome> matchObserver) {
            routeReportingMatch(cloudEvent, bufferIfNotLive, matchObserver);
        }
    }
}
