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

package org.occurrent.inmemory.filtermatching;

import org.jspecify.annotations.NullMarked;
import org.occurrent.filter.Filter;

import java.util.List;

import static org.occurrent.filter.Filter.*;

/**
 * Rewrites a {@link Filter} so that a condition on a field inside an event's {@code data} payload is treated as already
 * satisfied, leaving every other condition to be checked as before.
 * <p>
 * Package-private on purpose. The rewritten filter matches more events than the one it came from, which is right for
 * matching an event a store has already filtered and wrong for anything else. Handed out publicly it could reach a
 * store query, which would then be quietly wider than the filter that was written. {@link
 * FilterMatcher#matcherIgnoringPayloadConditions(Filter)} exposes the matching without exposing the widened filter.
 */
@NullMarked
final class PayloadConditions {

    private static final String DATA_PREFIX = Filter.DATA + ".";

    private PayloadConditions() {
    }

    /**
     * Returns {@code filter} with every condition on a {@code data} payload field replaced by one that matches
     * anything, so the rest of the filter still decides.
     * <p>
     * Replaced rather than removed. Removing a payload condition from an
     * {@code OR} would change what the filter means. {@code type = X OR data.amount = 42} would become
     * {@code type = X} and discard an event that matched only on the amount. Matching anything is correct under both
     * {@code AND} and {@code OR}.
     */
    // The one caller, FilterMatcher.matcherIgnoringPayloadConditions, rejects null before this is reached.
    static Filter assumingPayloadConditionsMatch(Filter filter) {
        return switch (filter) {
            case SingleConditionFilter scf -> isPayloadCondition(scf) ? new All() : scf;
            case CompositionFilter cf -> {
                List<Filter> rewritten = cf.filters().stream().map(PayloadConditions::assumingPayloadConditionsMatch).toList();
                yield new CompositionFilter(cf.operator(), rewritten);
            }
            case All all -> all;
            case CapabilityFilter cpf -> cpf;
        };
    }

    /**
     * Whether {@code filter} contains a condition on a {@code data} payload field anywhere in its tree.
     */
    static boolean hasPayloadCondition(Filter filter) {
        return switch (filter) {
            case SingleConditionFilter scf -> isPayloadCondition(scf);
            case CompositionFilter cf -> cf.filters().stream().anyMatch(PayloadConditions::hasPayloadCondition);
            case All ignored -> false;
            case CapabilityFilter ignored -> false;
        };
    }

    private static boolean isPayloadCondition(SingleConditionFilter scf) {
        return scf.fieldName().startsWith(DATA_PREFIX);
    }
}
