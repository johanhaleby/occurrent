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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} is the one place a {@link SubscriptionFilter} is
 * unwrapped into the {@link Filter} a resolver actually narrows, so what a resolver returns for a wrapped filter
 * and for the bare {@link Filter} inside it can only agree if that unwrapping is right. These cases are what fails
 * if it stops delegating.
 */
class DestinationResolverTest {

    private static final String EVENT_A_TYPE = "com.example.EventA";
    private static final String EVENT_B_TYPE = "com.example.EventB";

    private static final List<Filter> FILTERS = List.of(
            Filter.type(EVENT_A_TYPE),
            Filter.type(EVENT_A_TYPE).or(Filter.type(EVENT_B_TYPE)),
            Filter.subject("some-subject"),
            Filter.type(EVENT_A_TYPE).or(Filter.streamId("some-stream")));

    @Test
    void an_agnostic_subscription_filter_resolves_to_what_the_filter_it_wraps_resolves_to() {
        RecordingResolver resolver = new RecordingResolver();

        for (Filter filter : FILTERS) {
            assertThat(resolver.destinationsFor(AgnosticSubscriptionFilter.filter(filter)))
                    .isEqualTo(resolver.destinationsFor(filter));
        }
    }

    @Test
    void a_stream_subscription_filter_resolves_the_same_way_an_agnostic_one_does() {
        RecordingResolver resolver = new RecordingResolver();

        for (Filter filter : FILTERS) {
            assertThat(resolver.destinationsFor(StreamSubscriptionFilter.filter(filter)))
                    .isEqualTo(resolver.destinationsFor(AgnosticSubscriptionFilter.filter(filter)));
        }
    }

    @Test
    void an_agnostic_subscription_filter_hands_the_resolver_the_very_filter_it_wraps() {
        RecordingResolver resolver = new RecordingResolver();
        Filter filter = Filter.type(EVENT_A_TYPE);

        resolver.destinationsFor(AgnosticSubscriptionFilter.filter(filter));

        assertThat(resolver.received).isSameAs(filter);
    }

    @Test
    void a_stream_subscription_filter_hands_the_resolver_the_very_filter_it_wraps() {
        RecordingResolver resolver = new RecordingResolver();
        Filter filter = Filter.type(EVENT_A_TYPE);

        resolver.destinationsFor(StreamSubscriptionFilter.filter(filter));

        assertThat(resolver.received).isSameAs(filter);
    }

    @Test
    void a_dcb_subscription_filter_cannot_narrow_because_it_carries_no_filter() {
        RecordingResolver resolver = new RecordingResolver();

        Optional<Set<NamedDestination>> destinations = resolver.destinationsFor(DcbSubscriptionFilter.filter(DcbCriteria.all()));

        assertThat(destinations).isEmpty();
        assertThat(resolver.received).isNull();
    }

    @Test
    void a_subscription_filter_this_interface_does_not_understand_cannot_narrow() {
        RecordingResolver resolver = new RecordingResolver();

        Optional<Set<NamedDestination>> destinations = resolver.destinationsFor(new SubscriptionFilter() {
        });

        assertThat(destinations).isEmpty();
        assertThat(resolver.received).isNull();
    }

    private record NamedDestination(String name) implements EventDestination {
    }

    /**
     * Narrows through {@link EventTypeNarrowing} the way both shipped narrowing resolvers do, and keeps the
     * {@link Filter} it was handed so a test can assert which one arrived.
     */
    private static final class RecordingResolver implements DestinationResolver<NamedDestination> {

        private Filter received;

        @Override
        public NamedDestination destinationFor(CloudEvent cloudEvent) {
            return new NamedDestination(cloudEvent.getType());
        }

        @Override
        public Optional<Set<NamedDestination>> destinationsFor(Filter filter) {
            received = filter;
            return EventTypeNarrowing.narrow(filter)
                    .map(types -> types.stream().map(NamedDestination::new).collect(Collectors.toUnmodifiableSet()));
        }

        @Override
        public NamedDestination catchAllDestination() {
            return new NamedDestination("catch-all");
        }
    }
}
