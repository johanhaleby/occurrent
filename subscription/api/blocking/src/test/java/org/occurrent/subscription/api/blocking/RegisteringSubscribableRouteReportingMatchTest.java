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
    void subscribe_ignores_bufferIfNotLive_and_always_reports_delivered() {
        RawConsumersOneModel model = new RawConsumersOneModel(DataFieldReader.refusing());
        List<CloudEvent> received = new ArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), received::add);

        List<RoutingOutcome> observed = new ArrayList<>();
        model.acceptRaw(cloudEvent("1"), false, (cloudEvent, outcome) -> observed.add(outcome));

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

        void acceptRaw(CloudEvent cloudEvent, boolean bufferIfNotLive, BiConsumer<CloudEvent, RoutingOutcome> matchObserver) {
            routeReportingMatch(cloudEvent, bufferIfNotLive, matchObserver);
        }
    }
}
