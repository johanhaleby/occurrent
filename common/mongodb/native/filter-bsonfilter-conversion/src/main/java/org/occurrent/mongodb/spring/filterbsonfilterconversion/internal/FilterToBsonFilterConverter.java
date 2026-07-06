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

package org.occurrent.mongodb.spring.filterbsonfilterconversion.internal;

import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.All;
import org.occurrent.filter.Filter.CapabilityFilter;
import org.occurrent.filter.Filter.CompositionFilter;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import static java.util.Objects.requireNonNull;
import static org.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling.resolveSpecialCases;
import static org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.ConditionConverter.convertConditionToBsonCriteria;

/**
 * Converts a {@link Filter} into a {@link Bson} filter that can be used when querying MongoDB.
 */
public class FilterToBsonFilterConverter {

    // Mirror of DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, duplicated as a literal so this module keeps no DCB dependency.
    private static final String DCB_TAGS_FIELD = "dcbTags";

    public static Bson convertFilterToBsonFilter(TimeRepresentation timeRepresentation, Filter filter) {
        return convertFilterToBsonFilter(null, timeRepresentation, filter);
    }

    public static Bson convertFilterToBsonFilter(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(timeRepresentation, "TimeRepresentation cannot be null");

        final Bson query;
        if (filter instanceof All) {
            query = new BsonDocument();
        } else {
            query = innerConvert(fieldNamePrefix, timeRepresentation, filter);
        }
        return query;
    }

    private static Bson innerConvert(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        final Bson criteria;
        if (filter instanceof All) {
            criteria = new BsonDocument();
        } else if (filter instanceof SingleConditionFilter scf) {
            Condition<?> conditionToUse = resolveSpecialCases(timeRepresentation, scf);
            String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName());
            criteria = convertConditionToBsonCriteria(fieldName, conditionToUse);
        } else if (filter instanceof CapabilityFilter cpf) {
            // Match on the sparse-indexed dcbTags array field (DcbDocumentMapper.DCB_TAGS_INDEX_FIELD; the literal is
            // duplicated here to keep this module free of any DCB dependency) so the capability filter uses the ADR 49
            // index. A DCB append always writes this array (an empty array for zero tags), while a stream write never
            // does, so its presence is the discriminator: DCB events have it, stream events do not. This is equivalent
            // to keying off the dcbtags CloudEvent extension because the stream write path now rejects dcbtags-carrying
            // events, so the array and the extension always agree.
            // Exhaustive switch so a new EventStoreCapability constant forces a compile error here rather than being
            // silently treated as a stream event.
            boolean shouldHaveDcbTags = switch (cpf.capability()) {
                case DCB -> true;
                case STREAM -> false;
            };
            String fieldName = fieldNameOf(fieldNamePrefix, DCB_TAGS_FIELD);
            criteria = Filters.exists(fieldName, shouldHaveDcbTags);
        } else if (filter instanceof CompositionFilter cf) {
            Bson[] composedBson = cf.filters().stream().map(f -> innerConvert(fieldNamePrefix, timeRepresentation, f)).toArray(Bson[]::new);
            criteria = switch (cf.operator()) {
                case AND -> Filters.and(composedBson);
                case OR -> Filters.or(composedBson);
            };
        } else {
            throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
        }
        return criteria;
    }

    private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
        return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
    }
}