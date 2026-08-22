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

package org.occurrent.subscription.api.reactor;

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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;

/**
 * Exercises {@link RegisteringSubscribable#routeReportingMatch(CloudEvent, BiConsumer)} directly, with a raw
 * {@code matchObserver} that has no swallowing of its own. {@code PushSubscriptionModel}'s own
 * {@code notifyObserver} already catches a {@code RuntimeException} or {@code AssertionError} from the configured
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
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw shared;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(shared);
                    assertThat(error.getSuppressed()).isEmpty();
                });
    }

    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_matchers_exception() {
        RuntimeException matcherFailure = new IllegalStateException("matcher failed");
        Error observerFailure = new Error("matchObserver failed too");
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw matcherFailure;
        };
        RawConsumersOneModel model = new RawConsumersOneModel(throwingReader);
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.empty());

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw observerFailure;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(matcherFailure);
                    assertThat(error.getSuppressed()).containsExactly(observerFailure);
                });
    }

    @Test
    void reports_delivered_when_the_action_reports_it_landed() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.just(true));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    @Test
    void reports_deferred_when_the_action_declines_to_hand_the_event_over() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.just(false));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

        assertThat(observed).containsExactly(RoutingOutcome.DEFERRED);
    }

    /**
     * The regression this guards. A Copilot review of this PR found that the success-path {@code flatMap} used to
     * sit inside the same {@code onErrorResume} that classifies an action failure, so a throwing
     * {@code matchObserver} here was caught by that handler, reclassified as though the action itself had failed,
     * and told a second time with {@link RoutingOutcome#DELIVERED}. Told once, and the observer's own failure
     * propagates as itself.
     */
    @Test
    void the_matchObserver_is_told_once_on_the_success_path_and_its_own_failure_propagates_without_a_second_notification() {
        RuntimeException observerFailure = new IllegalStateException("observer failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.just(true));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    observed.add(outcome);
                    throw observerFailure;
                }))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(observerFailure));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    /**
     * As above, for a {@link RoutingOutcome#DEFERRED} result. The old bug reported it once correctly, then a second
     * time as {@link RoutingOutcome#DELIVERED} once the reclassification ran, changing the outcome as well as the
     * count.
     */
    @Test
    void the_matchObserver_is_told_once_after_a_deferred_result_and_its_own_failure_propagates_without_a_second_notification() {
        RuntimeException observerFailure = new IllegalStateException("observer failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.just(false));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    observed.add(outcome);
                    throw observerFailure;
                }))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(observerFailure));

        assertThat(observed).containsExactly(RoutingOutcome.DEFERRED);
    }

    @Test
    void the_matchObserver_still_reports_delivered_and_the_original_error_still_propagates_when_the_action_errors() {
        RuntimeException actionFailure = new IllegalStateException("action failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(actionFailure));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(actionFailure));

        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    /**
     * The regression this guards. A Copilot review of the blocking PR this mirrors found that a throwing
     * {@code matchObserver} here replaced {@code actionFailure} with its own failure instead. The original failure must
     * still propagate, with the observer's failure attached rather than discarded.
     */
    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_actions_exception() {
        RuntimeException actionFailure = new IllegalStateException("action failed");
        Error observerFailure = new Error("matchObserver failed too");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(actionFailure));

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw observerFailure;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(actionFailure);
                    assertThat(error.getSuppressed()).containsExactly(observerFailure);
                });
    }

    @Test
    void a_shared_exception_instance_thrown_by_the_action_and_the_matchObserver_is_not_self_suppressed() {
        RuntimeException shared = new IllegalStateException("shared failure");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(shared));

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw shared;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(shared);
                    assertThat(error.getSuppressed()).isEmpty();
                });
    }

    /**
     * The regression this guards. A refusal decided before any dispatch was attempted (an engine-level guard, not
     * a handler that ran) must never be reported as {@link RoutingOutcome#DELIVERED}, mirroring the same guard the
     * blocking stack's {@code BlockingHandover.acceptIfLive}'s {@code catchUpFailure} case needs, reached through
     * {@code CatchupThenPushSubscriptionModel}'s registered action.
     */
    @Test
    void a_permanent_routing_action_refusal_reports_refused_and_the_wrapped_cause_still_propagates() {
        RuntimeException refusalCause = new IllegalStateException("catch-up has failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, true)));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(refusalCause));

        assertThat(observed).containsExactly(RoutingOutcome.REFUSED);
    }

    /**
     * The regression this guards. A throwing {@code matchObserver} here must not replace the wrapped
     * {@code refusalCause} with its own failure. The wrapped cause must still propagate, with the observer's
     * failure attached rather than discarded.
     */
    @Test
    void a_distinct_matchObserver_failure_is_attached_to_the_refusals_wrapped_cause() {
        RuntimeException refusalCause = new IllegalStateException("catch-up has failed");
        Error observerFailure = new Error("matchObserver failed too");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, true)));

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw observerFailure;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(refusalCause);
                    assertThat(error.getSuppressed()).containsExactly(observerFailure);
                });
    }

    @Test
    void a_shared_exception_instance_thrown_by_the_refusals_wrapped_cause_and_the_matchObserver_is_not_self_suppressed() {
        RuntimeException shared = new IllegalStateException("catch-up has failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(shared, true)));

        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> {
                    throw shared;
                }))
                .verifyErrorSatisfies(error -> {
                    assertThat(error).isSameAs(shared);
                    assertThat(error.getSuppressed()).isEmpty();
                });
    }

    @Test
    void subscribe_always_reports_delivered() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        List<CloudEvent> received = new ArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> received.add(ce)));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

        assertThat(received).extracting(CloudEvent::getId).containsExactly("1");
        assertThat(observed).containsExactly(RoutingOutcome.DELIVERED);
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("NameDefined")
                .build();
    }

    /**
     * Nothing covered the three lifecycle states before this. They are the only outcomes a caller can see without
     * an error coming with them, which is what lets a broker bridge hold and pace them instead of sending them
     * through a failure policy, so each one is asserted on its own rather than through a shared helper.
     */
    @Test
    void nothing_registered_reports_unavailable_and_errors_with_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

        assertThat(observed).containsExactly(RoutingOutcome.UNAVAILABLE);
    }

    @Test
    void a_stopped_model_reports_unavailable_and_errors_with_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new IllegalStateException("the handler must never run")));
        model.stop();

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

        assertThat(observed).containsExactly(RoutingOutcome.UNAVAILABLE);
    }

    @Test
    void a_paused_subscription_reports_unavailable_and_errors_with_nothing() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new IllegalStateException("the handler must never run")));
        model.pauseSubscription("sub");

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyComplete();

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
        model.subscribeRaw("sub", StreamSubscriptionFilter.filter(Filter.data("amount", eq(42))), cloudEvent -> Mono.just(true));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(matcherFailure));

        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
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
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(refusalCause, false)));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(refusalCause));

        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
    }

    private static final class RawConsumersOneModel extends RegisteringSubscribable {
        RawConsumersOneModel(DataFieldReader dataFieldReader) {
            super(Consumers.ONE, dataFieldReader);
        }

        void subscribeRaw(String subscriptionId, @Nullable SubscriptionFilter filter, RoutingAction action) {
            subscribeReportingDelivery(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
        }

        Mono<Void> acceptRaw(CloudEvent cloudEvent, BiConsumer<CloudEvent, RoutingOutcome> matchObserver) {
            return routeReportingMatch(cloudEvent, matchObserver);
        }
    }
}
