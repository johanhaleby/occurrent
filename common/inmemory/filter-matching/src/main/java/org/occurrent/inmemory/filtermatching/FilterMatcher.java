/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.EventStoreCloudEventExtensions;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.CompositionFilter;

import java.util.function.Predicate;

import static org.occurrent.filter.Filter.All;
import static org.occurrent.filter.Filter.CapabilityFilter;
import static org.occurrent.filter.Filter.SingleConditionFilter;

/**
 * Check if a cloud event matching a given filter
 */
@NullMarked
public class FilterMatcher {

    public static boolean matchesFilter(CloudEvent cloudEvent, Filter filter) {
        return matchesFilter(cloudEvent, filter, DataFieldReader.refusing());
    }

    /**
     * A predicate that checks everything in {@code filter} except a condition on a field inside an event's {@code data}
     * payload, which it treats as already satisfied.
     * <p>
     * For a caller re-checking a filter against an event a store has already matched, where re-reading the payload is
     * not possible without a {@link DataFieldReader} and not necessary either, because the store applied the real
     * condition to have delivered the event. An attribute or extension is still checked, so a store that honors no
     * filter at all is still held to the part that can be checked here.
     * <p>
     * A predicate rather than a rewritten {@link Filter}, so the widened filter cannot escape and reach a store query,
     * where it would match more than the filter that was written. The widening also happens once, here, rather than per
     * event.
     */
    public static Predicate<CloudEvent> matcherIgnoringPayloadConditions(Filter filter) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }
        Filter withoutPayloadConditions = PayloadConditions.assumingPayloadConditionsMatch(filter);
        return cloudEvent -> matchesFilter(cloudEvent, withoutPayloadConditions);
    }

    public static boolean matchesFilter(CloudEvent cloudEvent, Filter filter, DataFieldReader dataFieldReader) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }

        return switch (filter) {
            case All ignored -> true;
            case SingleConditionFilter scf -> ConditionMatcher.matchesCondition(cloudEvent, scf.fieldName(), scf.condition(), dataFieldReader);
            case CapabilityFilter cpf -> matchesCapabilityFilter(cloudEvent, cpf);
            case CompositionFilter cf -> {
                Predicate<Filter> matchingPredicate = f -> matchesFilter(cloudEvent, f, dataFieldReader);
                yield switch (cf.operator()) {
                    case AND -> cf.filters().stream().allMatch(matchingPredicate);
                    case OR -> cf.filters().stream().anyMatch(matchingPredicate);
                };
            }
        };
    }

    private static boolean matchesCapabilityFilter(CloudEvent cloudEvent, CapabilityFilter cpf) {
        // A DCB append always stamps the dcbtags extension on the live CloudEvent; a stream event never carries it.
        boolean isDcbEvent = cloudEvent.getExtension(EventStoreCloudEventExtensions.DCB_TAGS) != null;
        // Exhaustive switch so a new EventStoreCapability constant forces a compile error here rather than being
        // silently treated as a stream event.
        boolean shouldBeDcbEvent = switch (cpf.capability()) {
            case DCB -> true;
            case STREAM -> false;
        };
        return isDcbEvent == shouldBeDcbEvent;
    }
}
