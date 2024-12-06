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

package org.occurrent.mongodb.spring.filterqueryconversion.internal;

import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.All;
import org.occurrent.filter.Filter.CompositionFilter;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import static java.util.Objects.requireNonNull;

/**
 * Converts a {@link Filter} into either a {@link Query} or {@link Criteria} that can be used for e.g. querying
 * an event store using Spring.
 */
public class FilterConverter {

    public static Query convertFilterToQuery(TimeRepresentation timeRepresentation, Filter filter) {
        return convertFilterToQuery(null, timeRepresentation, filter);
    }

    public static Query convertFilterToQuery(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(timeRepresentation, "TimeRepresentation cannot be null");

        final Query query;
        if (filter instanceof All) {
            query = new Query();
        } else {
            query = Query.query(convertFilterToCriteria(fieldNamePrefix, timeRepresentation, filter));
        }
        return query;
    }

    public static Criteria convertFilterToCriteria(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        final Criteria criteria;
        if (filter instanceof All) {
            criteria = new Criteria();
        } else if (filter instanceof SingleConditionFilter scf) {
            Condition<?> conditionToUse = SpecialFilterHandling.resolveSpecialCases(timeRepresentation, scf);
            String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName());
            criteria = ConditionToCriteriaConverter.convertConditionToCriteria(fieldName, conditionToUse);
        } else if (filter instanceof CompositionFilter cf) {
            Criteria[] composedCriteria = cf.filters().stream().map(f -> FilterConverter.convertFilterToCriteria(fieldNamePrefix, timeRepresentation, f)).toArray(Criteria[]::new);
            Criteria c = new Criteria();
            switch (cf.operator()) {
                case AND:
                    c.andOperator(composedCriteria);
                    break;
                case OR:
                    c.orOperator(composedCriteria);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cf.operator());
            }
            criteria = c;
        } else {
            throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
        }
        return criteria;
    }

    private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
        return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
    }
}