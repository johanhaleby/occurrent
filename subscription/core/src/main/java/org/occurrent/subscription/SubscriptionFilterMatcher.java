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

package org.occurrent.subscription;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.DataFieldReader;

import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static org.occurrent.inmemory.filtermatching.FilterMatcher.matchesFilter;

/**
 * Translates a {@link SubscriptionFilter} into an in-process {@link Predicate} over {@link CloudEvent}.
 * <p>
 * Shared by every subscription model that dispatches in-process (the in-memory model and the synchronous
 * model), so the capability-routing rules (a {@link StreamSubscriptionFilter} matches by plain filter, a
 * {@link DcbSubscriptionFilter} matches only DCB events, an {@link AgnosticSubscriptionFilter} matches both
 * capabilities by plain filter) live in exactly one place.
 */
public final class SubscriptionFilterMatcher {

    private SubscriptionFilterMatcher() {
    }

    /**
     * Build a predicate that decides whether a cloud event matches the supplied subscription filter.
     *
     * @param filter The subscription filter, or {@code null} to match every event.
     * @return A predicate over cloud events.
     */
    public static Predicate<CloudEvent> matcherFor(@Nullable SubscriptionFilter filter) {
        return matcherFor(filter, DataFieldReader.refusing());
    }

    /**
     * Build a predicate that decides whether a cloud event matches the supplied subscription filter, reading a data
     * payload field through the supplied reader when the filter asks for one.
     *
     * @param filter          The subscription filter, or {@code null} to match every event.
     * @param dataFieldReader Reads a field out of an event's payload. {@link DataFieldReader#refusing()} to refuse.
     * @return A predicate over cloud events.
     */
    public static Predicate<CloudEvent> matcherFor(@Nullable SubscriptionFilter filter, DataFieldReader dataFieldReader) {
        requireNonNull(dataFieldReader, "DataFieldReader cannot be null");
        switch (filter) {
            case null -> {
                return cloudEvent -> matchesFilter(cloudEvent, Filter.all(), dataFieldReader);
            }
            case StreamSubscriptionFilter streamSubscriptionFilter -> {
                Filter f = streamSubscriptionFilter.filter();
                return cloudEvent -> matchesFilter(cloudEvent, f, dataFieldReader);
            }
            case AgnosticSubscriptionFilter agnosticSubscriptionFilter -> {
                // Capability-agnostic delivery: match only the plain Filter, with no capability guard, so both stream and
                // DCB events are delivered. A plain Filter (no CapabilityFilter) matches events of every capability.
                Filter f = agnosticSubscriptionFilter.filter();
                return cloudEvent -> matchesFilter(cloudEvent, f, dataFieldReader);
            }
            case DcbSubscriptionFilter dcbSubscriptionFilter -> {
                // Requires isDcbEvent (the DCB tags extension) rather than a positive position, since with
                // stream position on by default, stream events also carry a global position. A "position > 0"
                // guard would leak stream events into a DCB subscription.
                DcbCriteria criteria = dcbSubscriptionFilter.criteria();
                return cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, criteria);
            }
            default ->
                    throw new IllegalArgumentException("Unsupported " + SubscriptionFilter.class.getSimpleName() + " type: " + filter.getClass().getName() + ". Only " + StreamSubscriptionFilter.class.getSimpleName() + ", " + AgnosticSubscriptionFilter.class.getSimpleName() + ", and " + DcbSubscriptionFilter.class.getSimpleName() + " are supported.");
        }
    }
}
