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
import org.occurrent.filter.Filter.CapabilityFilter;
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

    // Mirror of DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, duplicated as a literal so this module keeps no DCB dependency.
    private static final String DCB_TAGS_FIELD = "dcbTags";

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
        return switch (filter) {
            case All ignored -> new Criteria();
            case SingleConditionFilter scf -> {
                Condition<?> conditionToUse = SpecialFilterHandling.resolveSpecialCases(timeRepresentation, scf);
                String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName());
                yield ConditionToCriteriaConverter.convertConditionToCriteria(fieldName, conditionToUse);
            }
            case CapabilityFilter cpf -> capabilityCriteria(fieldNamePrefix, cpf);
            case CompositionFilter cf -> {
                Criteria[] composedCriteria = cf.filters().stream().map(f -> FilterConverter.convertFilterToCriteria(fieldNamePrefix, timeRepresentation, f)).toArray(Criteria[]::new);
                Criteria c = new Criteria();
                yield switch (cf.operator()) {
                    case AND -> c.andOperator(composedCriteria);
                    case OR -> c.orOperator(composedCriteria);
                };
            }
        };
    }

    private static Criteria capabilityCriteria(String fieldNamePrefix, CapabilityFilter cpf) {
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
        return Criteria.where(fieldName).exists(shouldHaveDcbTags);
    }

    private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
        return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
    }
}
