/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api.internal;

import org.jspecify.annotations.NullMarked;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.StreamReadFilter.AttributeFilter;
import org.occurrent.eventstore.api.StreamReadFilter.DataFilter;
import org.occurrent.eventstore.api.StreamReadFilter.ExtensionFilter;
import org.occurrent.filter.Filter;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Maps {@link StreamReadFilter} to {@link Filter} so existing datastore mapping logic can be reused.
 * <p>
 * This mapper does NOT validate. Call {@link StreamReadFilterValidator#validate(StreamReadFilter)} in the stream-read path.
 */
@NullMarked
public final class StreamReadFilterToFilterMapper {

    private StreamReadFilterToFilterMapper() {
    }

    /**
     * Map {@link StreamReadFilter} into {@link Filter}.
     * <p>
     * Note: This does not apply the streamId constraint. The caller typically does:
     * Filter.streamId(streamId).and(StreamReadFilterToFilterMapper.map(streamReadFilter))
     */
    public static Filter map(StreamReadFilter filter) {
        requireNonNull(filter, "StreamReadFilter cannot be null");
        return mapInternal(filter);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Filter mapInternal(StreamReadFilter filter) {
        if (filter instanceof AttributeFilter<?> af) {
            return Filter.filter(af.attributeName(), (Condition) af.condition());
        }

        if (filter instanceof ExtensionFilter<?> ef) {
            return Filter.filter(ef.extensionName(), (Condition) ef.condition());
        }

        if (filter instanceof DataFilter<?> df) {
            return Filter.data(df.path(), (Condition) df.condition());
        }

        if (filter instanceof StreamReadFilter.CompositionFilter cf) {

            List<Filter> mapped = new ArrayList<>(cf.filters().size());
            for (StreamReadFilter f : cf.filters()) {
                mapped.add(mapInternal(f));
            }

            Filter.CompositionOperator op = cf.operator() == StreamReadFilter.CompositionOperator.AND
                    ? Filter.CompositionOperator.AND
                    : Filter.CompositionOperator.OR;

            return new Filter.CompositionFilter(op, mapped);
        }

        throw new IllegalArgumentException("Unknown StreamReadFilter implementation: " + filter.getClass().getName());
    }
}