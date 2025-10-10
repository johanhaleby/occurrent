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
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.CompositionFilter;

import java.util.function.Predicate;

import static org.occurrent.filter.Filter.All;
import static org.occurrent.filter.Filter.SingleConditionFilter;

/**
 * Check if a cloud event matching a given filter
 */
@NullMarked
public class FilterMatcher {

    public static boolean matchesFilter(CloudEvent cloudEvent, Filter filter) {
        if (filter == null) {
            throw new IllegalArgumentException(Filter.class.getSimpleName() + " cannot be null");
        }

        final boolean matches;
        if (filter instanceof All) {
            matches = true;
        } else if (filter instanceof SingleConditionFilter scf) {
            matches = ConditionMatcher.matchesCondition(cloudEvent, scf.fieldName(), scf.condition());
        } else if (filter instanceof CompositionFilter cf) {
            Predicate<Filter> matchingPredicate = f -> matchesFilter(cloudEvent, f);
            matches = switch (cf.operator()) {
                case AND -> cf.filters().stream().allMatch(matchingPredicate);
                case OR -> cf.filters().stream().anyMatch(matchingPredicate);
            };
        } else {
            throw new IllegalArgumentException("Unrecognized filter: " + filter.getClass().getName());
        }

        return matches;
    }
}