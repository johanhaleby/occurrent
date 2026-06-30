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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.DcbSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionModelAdapterTest {

    @Test
    void delivers_only_dcb_events_matching_the_query_and_passes_the_start_position_through() {
        CloudEvent matching = dcbEvent("NameDefined", 1, "name:1");
        CloudEvent withoutPosition = DcbCloudEvents.withTags(event("NameDefined"), Set.of("name:1"));
        CloudEvent otherBoundary = dcbEvent("OrderPlaced", 2, "order:1");

        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel(Flux.just(matching, withoutPosition, otherBoundary));
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        StepVerifier.create(adapter.subscribe(DcbQuery.tags("name:1"), DcbStartAt.afterPosition(5)))
                .expectNext(matching)
                .verifyComplete();

        // The in-process floor drops the event with no dcbposition and the one whose tags do not match the query, so
        // the subscription stays scoped to its own query even if a backend ignores the server-side filter.
        assertThat(delegate.capturedFilter).isInstanceOf(DcbSubscriptionFilter.class);
        // The DcbStartAt is converted to a generic StartAt and passed straight to the delegate.
        assertThat(delegate.capturedStartAt).isInstanceOfSatisfying(StartAt.StartAtSubscriptionPosition.class,
                start -> assertThat(start.subscriptionPosition).isEqualTo(DcbSubscriptionPosition.of(5)));
    }

    private static CloudEvent dcbEvent(String type, long position, String... tags) {
        return DcbCloudEvents.withPosition(DcbCloudEvents.withTags(event(type), Set.of(tags)), position);
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:test"))
                .withType(type)
                .build();
    }

    private static final class RecordingSubscriptionModel implements SubscriptionModel {
        private final Flux<CloudEvent> events;
        @Nullable
        private SubscriptionFilter capturedFilter;
        @Nullable
        private StartAt capturedStartAt;

        private RecordingSubscriptionModel(Flux<CloudEvent> events) {
            this.events = events;
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            this.capturedFilter = filter;
            this.capturedStartAt = startAt;
            return events;
        }
    }
}
