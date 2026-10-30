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

package org.occurrent.eventstore.api.dcb;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Covers {@link DcbConsistencyToken}, which holds the criteria it was read with and compares them as a set of
 * alternatives.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbConsistencyTokenTest {

    private static final DcbCriterion A = DcbCriteria.tags(Tag.of("a", "1"));
    private static final DcbCriterion B = DcbCriteria.tags(Tag.of("b", "1"));

    @Test
    void of_exposes_the_criteria_and_the_version_it_was_given() {
        DcbCriteria criteria = DcbCriteria.anyOf(A, B);

        DcbConsistencyToken token = DcbConsistencyToken.of(criteria, 7);

        assertThat(token.criteria()).isSameAs(criteria);
        assertThat(token.version()).isEqualTo(7);
    }

    @Test
    void of_accepts_a_version_of_zero() {
        assertThat(DcbConsistencyToken.of(DcbCriteria.all(), 0).version()).isZero();
    }

    @Test
    void of_throws_NullPointerException_when_the_criteria_is_null() {
        Throwable thrown = catchThrowable(() -> DcbConsistencyToken.of(null, 1));

        assertThat(thrown).isExactlyInstanceOf(NullPointerException.class).hasMessage("Criteria cannot be null");
    }

    @Test
    void of_throws_IllegalArgumentException_when_the_version_is_negative() {
        Throwable thrown = catchThrowable(() -> DcbConsistencyToken.of(A, -1));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Consistency token version cannot be negative");
    }

    @Test
    void tokens_with_the_same_criteria_and_version_are_equal() {
        assertThat(DcbConsistencyToken.of(A, 3)).isEqualTo(DcbConsistencyToken.of(A, 3));
        assertThat(DcbConsistencyToken.of(A, 3)).hasSameHashCodeAs(DcbConsistencyToken.of(A, 3));
    }

    @Test
    void any_of_in_a_different_order_is_the_same_criteria() {
        DcbConsistencyToken first = DcbConsistencyToken.of(DcbCriteria.anyOf(A, B), 3);
        DcbConsistencyToken second = DcbConsistencyToken.of(DcbCriteria.anyOf(B, A), 3);

        assertThat(first).isEqualTo(second);
        assertThat(first).hasSameHashCodeAs(second);
    }

    @Test
    void a_criterion_is_the_same_criteria_as_an_items_holding_only_that_criterion() {
        DcbConsistencyToken single = DcbConsistencyToken.of(A, 3);
        DcbConsistencyToken wrapped = DcbConsistencyToken.of(new DcbCriteria.Items(List.of(A)), 3);

        assertThat(single).isEqualTo(wrapped);
        assertThat(wrapped).isEqualTo(single);
        assertThat(single).hasSameHashCodeAs(wrapped);
    }

    @Test
    void any_of_a_repeated_criterion_is_the_same_criteria_as_that_criterion() {
        DcbConsistencyToken repeated = DcbConsistencyToken.of(DcbCriteria.anyOf(A, A), 3);
        DcbConsistencyToken single = DcbConsistencyToken.of(A, 3);

        assertThat(repeated).isEqualTo(single);
        assertThat(repeated).hasSameHashCodeAs(single);
    }

    @Test
    void any_of_with_a_duplicated_alternative_is_the_same_criteria_as_any_of_without_it() {
        DcbConsistencyToken withDuplicate = DcbConsistencyToken.of(DcbCriteria.anyOf(A, B, B), 3);
        DcbConsistencyToken withoutDuplicate = DcbConsistencyToken.of(DcbCriteria.anyOf(A, B), 3);

        assertThat(withDuplicate).isEqualTo(withoutDuplicate);
        assertThat(withoutDuplicate).isEqualTo(withDuplicate);
        assertThat(withDuplicate).hasSameHashCodeAs(withoutDuplicate);
    }

    @Test
    void types_is_not_the_same_criteria_as_any_of_the_single_types() {
        DcbConsistencyToken types = DcbConsistencyToken.of(DcbCriteria.types("A", "B"), 3);
        DcbConsistencyToken anyOfTypes = DcbConsistencyToken.of(DcbCriteria.anyOf(DcbCriteria.type("A"), DcbCriteria.type("B")), 3);

        assertThat(types).isNotEqualTo(anyOfTypes);
        assertThat(anyOfTypes).isNotEqualTo(types);
    }

    @Test
    void all_is_the_same_criteria_only_as_all() {
        assertThat(DcbConsistencyToken.of(DcbCriteria.all(), 3)).isEqualTo(DcbConsistencyToken.of(DcbCriteria.all(), 3));
        assertThat(DcbConsistencyToken.of(DcbCriteria.all(), 3)).isNotEqualTo(DcbConsistencyToken.of(A, 3));
        assertThat(DcbConsistencyToken.of(A, 3)).isNotEqualTo(DcbConsistencyToken.of(DcbCriteria.all(), 3));
        assertThat(DcbConsistencyToken.of(DcbCriteria.all(), 3)).isNotEqualTo(DcbConsistencyToken.of(DcbCriteria.anyOf(A, B), 3));
    }

    @Test
    void the_same_version_with_different_criteria_is_not_equal() {
        assertThat(DcbConsistencyToken.of(A, 3)).isNotEqualTo(DcbConsistencyToken.of(B, 3));
    }

    @Test
    void the_same_criteria_with_a_different_version_is_not_equal() {
        assertThat(DcbConsistencyToken.of(A, 3)).isNotEqualTo(DcbConsistencyToken.of(A, 4));
    }

    @Test
    void of_with_a_version_only_gives_an_empty_marker_map() {
        assertThat(DcbConsistencyToken.of(A, 3).markers()).isEmpty();
    }

    @Test
    void of_with_a_version_only_is_the_same_token_as_of_with_an_empty_marker_map() {
        assertThat(DcbConsistencyToken.of(A, 3)).isEqualTo(DcbConsistencyToken.of(A, 3, Map.of()));
        assertThat(DcbConsistencyToken.of(A, 3)).hasSameHashCodeAs(DcbConsistencyToken.of(A, 3, Map.of()));
    }

    @Test
    void of_exposes_the_markers_it_was_given_and_keeps_its_own_copy_of_them() {
        Map<String, MarkerObservation> markers = new HashMap<>(Map.of("tag:a:1", new MarkerObservation("e1", 2, 0)));

        DcbConsistencyToken token = DcbConsistencyToken.of(A, 2, markers);
        markers.put("tag:b:1", MarkerObservation.absent());

        assertThat(token.markers()).containsExactly(Map.entry("tag:a:1", new MarkerObservation("e1", 2, 0)));
    }

    @Test
    void of_throws_NullPointerException_when_the_markers_are_null() {
        Throwable thrown = catchThrowable(() -> DcbConsistencyToken.of(A, 1, null));

        assertThat(thrown).isExactlyInstanceOf(NullPointerException.class).hasMessage("Markers cannot be null");
    }

    @Test
    void a_marker_that_was_not_found_is_observed_as_no_epoch_version_0_and_unstamped_0() {
        assertThat(MarkerObservation.absent()).isEqualTo(new MarkerObservation(null, 0, 0));
    }

    @Test
    void tokens_with_the_same_criteria_version_and_markers_are_equal() {
        DcbConsistencyToken first = DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e1", 3, 1)));
        DcbConsistencyToken second = DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e1", 3, 1)));

        assertThat(first).isEqualTo(second);
        assertThat(first).hasSameHashCodeAs(second);
    }

    @Test
    void tokens_that_differ_only_in_what_they_saw_of_a_marker_are_not_equal() {
        DcbConsistencyToken token = DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e1", 3, 1)));

        assertThat(token).as("epoch").isNotEqualTo(DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e2", 3, 1))));
        assertThat(token).as("version").isNotEqualTo(DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e1", 4, 1))));
        assertThat(token).as("unstamped").isNotEqualTo(DcbConsistencyToken.of(A, 3, Map.of("tag:a:1", new MarkerObservation("e1", 3, 0))));
        assertThat(token).as("marker key").isNotEqualTo(DcbConsistencyToken.of(A, 3, Map.of("tag:b:1", new MarkerObservation("e1", 3, 1))));
        assertThat(token).as("no markers").isNotEqualTo(DcbConsistencyToken.of(A, 3));
        assertThat(DcbConsistencyToken.of(A, 3)).as("no markers, the other way").isNotEqualTo(token);
    }

    @Test
    void a_token_is_not_equal_to_null_or_to_another_type() {
        DcbConsistencyToken token = DcbConsistencyToken.of(A, 3);

        assertThat(token).isNotEqualTo(null);
        assertThat(token).isNotEqualTo("token");
    }

    @Test
    void to_string_shows_the_criteria_and_the_version() {
        assertThat(DcbConsistencyToken.of(A, 3).toString()).contains(A.toString()).contains("version=3");
    }
}
