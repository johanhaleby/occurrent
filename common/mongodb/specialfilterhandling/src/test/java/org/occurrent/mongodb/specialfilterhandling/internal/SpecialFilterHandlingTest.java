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

package org.occurrent.mongodb.specialfilterhandling.internal;

import org.junit.jupiter.api.Test;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.SingleConditionFilter;

import java.sql.Date;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling.resolveSpecialCases;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.time.internal.RFC3339.RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER;

class SpecialFilterHandlingTest {

    // Zero seconds and zero nanos, so OffsetDateTime.toString() omits both and differs from the canonical
    // fixed-width shape (ADR 79).
    private static final OffsetDateTime INSTANT = OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC);
    private static final OffsetDateTime OTHER_INSTANT = INSTANT.plusDays(1);
    private static final String CANONICAL = RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER.format(INSTANT);
    private static final String LEGACY = INSTANT.toString();
    private static final String OTHER_CANONICAL = RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER.format(OTHER_INSTANT);
    private static final String OTHER_LEGACY = OTHER_INSTANT.toString();

    @Test
    void eq_becomes_in_of_canonical_and_legacy_shape() {
        Condition<?> resolved = resolveRfc3339(Condition.eq(INSTANT));
        assertThat(resolved).isEqualTo(Condition.in(CANONICAL, LEGACY));
    }

    @Test
    void ne_becomes_not_in_of_canonical_and_legacy_shape() {
        Condition<?> resolved = resolveRfc3339(Condition.ne(INSTANT));
        assertThat(resolved).isEqualTo(Condition.not(Condition.in(CANONICAL, LEGACY)));
    }

    @Test
    void in_expands_every_operand_to_canonical_and_legacy_shape() {
        Condition<?> resolved = resolveRfc3339(Condition.in(INSTANT, OTHER_INSTANT));
        assertThat(resolved).isEqualTo(Condition.in(CANONICAL, LEGACY, OTHER_CANONICAL, OTHER_LEGACY));
    }

    @Test
    void range_conditions_map_to_the_canonical_shape_only() {
        // Condition.map keeps the pre-map description, so a fresh Condition built from the mapped value would not
        // be record-equal to the resolved one even though the operand, the value BSON conversion actually reads,
        // is correct. Assert on the operand directly instead.
        assertThat(operandOf(resolveRfc3339(Condition.lt(INSTANT)))).isEqualTo(CANONICAL);
        assertThat(operandOf(resolveRfc3339(Condition.gt(INSTANT)))).isEqualTo(CANONICAL);
        assertThat(operandOf(resolveRfc3339(Condition.lte(INSTANT)))).isEqualTo(CANONICAL);
        assertThat(operandOf(resolveRfc3339(Condition.gte(INSTANT)))).isEqualTo(CANONICAL);
    }

    @Test
    void and_recurses_into_each_operand() {
        Condition<OffsetDateTime> composed = Condition.and(Condition.eq(INSTANT), Condition.eq(OTHER_INSTANT));
        Condition<?> resolved = resolveRfc3339(composed);
        assertThat(resolved).isEqualTo(Condition.and(Condition.in(CANONICAL, LEGACY), Condition.in(OTHER_CANONICAL, OTHER_LEGACY)));
    }

    @Test
    void or_recurses_into_each_operand() {
        Condition<OffsetDateTime> composed = Condition.or(Condition.eq(INSTANT), Condition.eq(OTHER_INSTANT));
        Condition<?> resolved = resolveRfc3339(composed);
        assertThat(resolved).isEqualTo(Condition.or(Condition.in(CANONICAL, LEGACY), Condition.in(OTHER_CANONICAL, OTHER_LEGACY)));
    }

    @Test
    void not_recurses_into_its_operand_the_same_way_ne_does() {
        Condition<OffsetDateTime> composed = Condition.not(Condition.eq(INSTANT));
        Condition<?> resolved = resolveRfc3339(composed);
        assertThat(resolved).isEqualTo(Condition.not(Condition.in(CANONICAL, LEGACY)));
    }

    @Test
    void a_multi_operand_condition_recurses_into_a_range_child_too() {
        Condition<OffsetDateTime> composed = Condition.and(Condition.eq(INSTANT), Condition.lt(OTHER_INSTANT));
        Condition<?> resolved = resolveRfc3339(composed);
        MultiOperandCondition<?> multi = (MultiOperandCondition<?>) resolved;
        assertThat(multi.operations().get(0)).isEqualTo(Condition.in(CANONICAL, LEGACY));
        assertThat(operandOf(multi.operations().get(1))).isEqualTo(OTHER_CANONICAL);
    }

    @Test
    void date_representation_is_unaffected_by_the_rfc3339_string_transform() {
        SingleConditionFilter filter = (SingleConditionFilter) Filter.time(Condition.eq(INSTANT));
        Condition<?> resolved = resolveSpecialCases(DATE, filter);
        assertThat(operandOf(resolved)).isEqualTo(Date.from(INSTANT.toInstant()));
    }

    @Test
    void a_filter_on_a_field_other_than_time_is_returned_unchanged() {
        SingleConditionFilter filter = (SingleConditionFilter) Filter.filter("someOtherField", Condition.eq("a value"));
        Condition<?> resolved = resolveSpecialCases(RFC_3339_STRING, filter);
        assertThat(resolved).isEqualTo(Condition.eq("a value"));
    }

    private static Condition<?> resolveRfc3339(Condition<OffsetDateTime> condition) {
        SingleConditionFilter filter = (SingleConditionFilter) Filter.time(condition);
        return resolveSpecialCases(RFC_3339_STRING, filter);
    }

    private static Object operandOf(Condition<?> condition) {
        return ((SingleOperandCondition<?>) condition).operand();
    }
}
