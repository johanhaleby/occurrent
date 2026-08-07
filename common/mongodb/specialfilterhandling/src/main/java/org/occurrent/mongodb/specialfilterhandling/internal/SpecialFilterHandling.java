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

package org.occurrent.mongodb.specialfilterhandling.internal;

import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.InOperandCondition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.sql.Date;
import java.time.OffsetDateTime;
import java.util.List;

import static org.occurrent.filter.Filter.TIME;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.time.internal.RFC3339.RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER;

/**
 * Some filters need to be treated specially, for example they may be dependent on the EventStore configuration.
 */
public class SpecialFilterHandling {

    @SuppressWarnings("unchecked")
    public static Condition<?> resolveSpecialCases(TimeRepresentation timeRepresentation, SingleConditionFilter scf) {
        if (TIME.equals(scf.fieldName())) {
            Condition<OffsetDateTime> zdfCondition = (Condition<OffsetDateTime>) scf.condition();
            if (timeRepresentation == RFC_3339_STRING) {
                return resolveRfc3339StringCondition(zdfCondition);
            } else {
                return zdfCondition.map(zdf -> Date.from(zdf.toInstant()));
            }
        }
        return scf.condition();
    }

    /**
     * A pre-upgrade event may still hold its {@code time} value in the legacy, variable-width shape written by
     * {@code OffsetDateTime.toString()} (see ADR 79), rather than the canonical fixed-width shape that
     * {@link org.occurrent.time.internal.RFC3339#RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER} produces. An equality or
     * inequality condition therefore has to match both shapes for a given instant to see events written either side
     * of the upgrade. Range conditions (LT, GT, LTE, GTE) are left as a plain canonical mapping, since a legacy and a
     * canonical value do not sort against each other (ADR 79), so mixing them into a range would trade a silent miss
     * for an incorrect match rather than fixing anything.
     */
    private static Condition<String> resolveRfc3339StringCondition(Condition<OffsetDateTime> condition) {
        return switch (condition) {
            case SingleOperandCondition<OffsetDateTime> single -> switch (single.operandConditionName()) {
                case EQ -> Condition.in(bothRepresentationsOf(single.operand()));
                case NE -> Condition.not(Condition.in(bothRepresentationsOf(single.operand())));
                case LT, GT, LTE, GTE -> single.map(RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER::format);
            };
            case InOperandCondition<OffsetDateTime> in -> Condition.in(in.operand().stream()
                    .flatMap(t -> bothRepresentationsOf(t).stream())
                    .toList());
            case MultiOperandCondition<OffsetDateTime> multi -> {
                List<Condition<String>> resolvedOperations = multi.operations().stream()
                        .map(SpecialFilterHandling::resolveRfc3339StringCondition)
                        .toList();
                yield switch (multi.operationName()) {
                    case AND -> Condition.and(resolvedOperations);
                    case OR -> Condition.or(resolvedOperations);
                    case NOT -> Condition.not(resolvedOperations.get(0));
                };
            }
        };
    }

    private static List<String> bothRepresentationsOf(OffsetDateTime offsetDateTime) {
        return List.of(RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER.format(offsetDateTime), offsetDateTime.toString());
    }
}