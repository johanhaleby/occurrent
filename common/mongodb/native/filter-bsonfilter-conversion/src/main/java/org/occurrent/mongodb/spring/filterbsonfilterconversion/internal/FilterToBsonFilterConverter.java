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
        } else if (filter instanceof CompositionFilter) {
            CompositionFilter cf = (CompositionFilter) filter;
            Bson[] composedBson = cf.filters().stream().map(f -> innerConvert(fieldNamePrefix, timeRepresentation, f)).toArray(Bson[]::new);
            switch (cf.operator()) {
                case AND:
                    criteria = Filters.and(composedBson);
                    break;
                case OR:
                    criteria = Filters.or(composedBson);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cf.operator());
            }
        } else {
            throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
        }
        return criteria;
    }

    private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
        return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
    }
}