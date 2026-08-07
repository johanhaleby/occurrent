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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.condition.Condition.*;

/**
 * These pin the semantics of {@link ConditionMatcher#matchesCondition(CloudEvent, String, org.occurrent.condition.Condition, DataFieldReader)}
 * for a {@code Filter.data(..)} condition, measured against real MongoDB rather than read off documentation (see
 * {@code EventStoreQueriesConformance}), plus the collection and type-mismatch rules that follow from them.
 */
class ConditionMatcherTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:test"))
            .withType("test")
            .build();

    // A tiny DataFieldReader over a plain Map, so these tests stay free of a JSON parser. Path resolution mirrors
    // the dotted-path contract documented on DataFieldReader: an absent key, or a path that continues past a
    // value that is not a Map, answers empty.
    private static DataFieldReader readerOver(Object root) {
        return (event, path) -> {
            Object current = root;
            for (String segment : path.split("\\.")) {
                if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return Optional.empty();
                }
                current = map.get(segment);
            }
            return Optional.ofNullable(current);
        };
    }

    private static boolean matches(Object payload, Filter filter) {
        Filter.SingleConditionFilter scf = (Filter.SingleConditionFilter) filter;
        return ConditionMatcher.matchesCondition(EVENT, scf.fieldName(), scf.condition(), readerOver(payload));
    }

    @Test
    void a_dotted_path_reaches_a_field_inside_a_nested_object() {
        Object payload = Map.of("person", Map.of("city", "Malmo"));

        assertThat(matches(payload, Filter.data("person.city", eq("Malmo")))).isTrue();
    }

    @Test
    void a_dotted_path_that_reaches_the_field_but_not_the_value_matches_nothing() {
        Object payload = Map.of("person", Map.of("city", "Malmo"));

        assertThat(matches(payload, Filter.data("person.city", eq("Lund")))).isFalse();
    }

    @Test
    void a_numeric_operand_matches_a_numeric_field() {
        Object payload = Map.of("amount", 42);

        assertThat(matches(payload, Filter.data("amount", eq(42)))).isTrue();
    }

    @Test
    void a_text_operand_does_not_match_a_numeric_field_because_a_number_is_not_its_text() {
        Object payload = Map.of("amount", 42);

        assertThat(matches(payload, Filter.data("amount", eq("42")))).isFalse();
    }

    @Test
    void eq_compares_numbers_by_value_regardless_of_java_type() {
        // A Long stored in the payload against an Integer operand is the realistic break: Jackson parses a
        // serialized whole number back as Integer or Long depending on magnitude, while the filter operand comes
        // from whatever type the caller happened to write the literal as.
        assertThat(matches(Map.of("amount", 42L), Filter.data("amount", eq(42)))).isTrue();
        assertThat(matches(Map.of("amount", 42), Filter.data("amount", eq(42L)))).isTrue();
        assertThat(matches(Map.of("amount", 42.0), Filter.data("amount", eq(42)))).isTrue();
    }

    @Test
    void ne_compares_numbers_by_value_regardless_of_java_type() {
        assertThat(matches(Map.of("amount", 42L), Filter.data("amount", ne(42)))).isFalse();
        assertThat(matches(Map.of("amount", 43L), Filter.data("amount", ne(42)))).isTrue();
    }

    @Test
    void in_compares_numbers_by_value_regardless_of_java_type() {
        assertThat(matches(Map.of("amount", 42L), Filter.data("amount", in(1, 42)))).isTrue();
        assertThat(matches(Map.of("amount", 43L), Filter.data("amount", in(1, 42)))).isFalse();
    }

    @Test
    void eq_and_ne_do_not_throw_on_a_non_finite_double_and_fall_back_to_object_equality() {
        // BigDecimal.valueOf(double) throws for NaN and an infinity, so the numeric-by-value path steers around
        // those rather than crashing a query over one bad payload.
        assertThat(matches(Map.of("amount", Double.NaN), Filter.data("amount", eq(Double.NaN)))).isTrue();
        assertThat(matches(Map.of("amount", Double.POSITIVE_INFINITY), Filter.data("amount", eq(Double.POSITIVE_INFINITY)))).isTrue();
        assertThat(matches(Map.of("amount", Double.NaN), Filter.data("amount", eq(42.0)))).isFalse();
        assertThat(matches(Map.of("amount", Double.NaN), Filter.data("amount", ne(42.0)))).isTrue();
    }

    @Test
    void a_range_operator_compares_a_whole_number_and_a_fraction_by_value() {
        assertThat(matches(Map.of("amount", 42), Filter.data("amount", gt(10)))).isTrue();
        assertThat(matches(Map.of("amount", 42.5), Filter.data("amount", gt(10)))).isTrue();
    }

    @Test
    void a_range_operator_given_an_operand_of_a_different_type_matches_nothing_rather_than_throwing() {
        Object payload = Map.of("amount", 42);

        assertThat(matches(payload, Filter.data("amount", gt("10")))).isFalse();
    }

    @Test
    void an_array_field_matches_when_any_element_satisfies_eq() {
        Object payload = Map.of("tags", List.of("red", "blue"));

        assertThat(matches(payload, Filter.data("tags", eq("red")))).isTrue();
        assertThat(matches(payload, Filter.data("tags", eq("green")))).isFalse();
    }

    @Test
    void an_array_field_matches_a_range_operator_when_any_element_satisfies_it() {
        Object payload = Map.of("scores", List.of(1, 5, 10));

        assertThat(matches(payload, Filter.data("scores", gt(9)))).isTrue();
        assertThat(matches(payload, Filter.data("scores", gt(100)))).isFalse();
    }

    @Test
    void an_array_field_matches_in_when_any_element_is_among_the_operands() {
        Object payload = Map.of("tags", List.of("red", "blue"));

        assertThat(matches(payload, Filter.data("tags", in("green", "red")))).isTrue();
        assertThat(matches(payload, Filter.data("tags", in("green", "yellow")))).isFalse();
    }

    @Test
    void ne_on_an_array_matches_only_when_no_element_equals_the_operand() {
        // Not asserted against MongoDB by the conformance suite: this pins the implementation's chosen reading
        // ("no element equals") rather than a verified store behaviour.
        Object payload = Map.of("tags", List.of("red", "blue"));

        assertThat(matches(payload, Filter.data("tags", ne("green")))).isTrue();
        assertThat(matches(payload, Filter.data("tags", ne("red")))).isFalse();
    }

    @Test
    void an_absent_field_matches_nothing() {
        Object payload = Map.of("name", "alice");

        assertThat(matches(payload, Filter.data("nosuchfield", eq("x")))).isFalse();
    }

    @Test
    void a_path_that_continues_past_a_scalar_matches_nothing() {
        Object payload = Map.of("name", "alice");

        assertThat(matches(payload, Filter.data("name.deeper", eq("x")))).isFalse();
    }

    @Test
    void a_root_that_is_not_an_object_matches_nothing() {
        Object payload = List.of(1, 2, 3);

        assertThat(matches(payload, Filter.data("0", eq(1)))).isFalse();
    }

    @Test
    void an_absent_field_matches_nothing_for_a_range_operator_too() {
        Object payload = Map.of("name", "alice");

        assertThat(matches(payload, Filter.data("nosuchfield", gt(1)))).isFalse();
    }

    @Test
    void a_type_mismatch_on_an_attribute_field_matches_nothing_rather_than_throwing() {
        // "id" is a String attribute; comparing it with a range operator against a non-String operand used to
        // throw a ClassCastException from Comparable.compareTo. This is the same type-mismatch rule as for a
        // data field, applied to an attribute instead, and is the one observable change for non-data fields.
        boolean result = ConditionMatcher.matchesCondition(EVENT, "id", gt(42));

        assertThat(result).isFalse();
    }

    @Test
    void numbers_of_different_java_types_still_compare_by_value_on_an_extension() {
        // "streamversion" style extensions are stored as Long while a filter operand built from an int literal is
        // an Integer. Comparable.compareTo(Long, Integer) throws ClassCastException; comparing by value through
        // BigDecimal both avoids that and gives the numerically correct answer, for an extension as much as for
        // a data field.
        CloudEvent eventWithLongExtension = CloudEventBuilder.v1(EVENT).withExtension("counter", 42L).build();

        assertThat(ConditionMatcher.matchesCondition(eventWithLongExtension, "counter", gt(10))).isTrue();
        assertThat(ConditionMatcher.matchesCondition(eventWithLongExtension, "counter", gt(100))).isFalse();
    }

    @Test
    void a_data_field_condition_without_a_reader_refuses_rather_than_matching_or_throwing_silently() {
        assertThatThrownBy(() -> ConditionMatcher.matchesCondition(EVENT, "data.amount", eq(42)))
                .as("no condition the caller writes instead makes a payload readable, so this refuses the way anything "
                        + "refuses a capability it was not built with")
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
