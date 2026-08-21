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
    void a_routing_action_refusal_reports_not_deliverable_and_the_wrapped_cause_still_propagates() {
        RuntimeException refusalCause = new IllegalStateException("catch-up has failed");
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(refusalCause)));

        List<RoutingOutcome> observed = new ArrayList<>();
        StepVerifier.create(model.acceptRaw(cloudEvent("1"), (cloudEvent, outcome) -> observed.add(outcome)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(refusalCause));

        assertThat(observed).containsExactly(RoutingOutcome.NOT_DELIVERABLE);
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
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(refusalCause)));

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
        model.subscribeRaw("sub", null, cloudEvent -> Mono.error(new RegisteringSubscribable.RoutingAction.Refusal(shared)));

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
