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

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers the rule that a {@link DcbAppendCondition} accepts a token only when the token was read with the same criteria
 * as the condition.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@SuppressWarnings("deprecation")
class DcbAppendConditionTest {

    private static final DcbCriterion A = DcbCriteria.tags(Tag.of("a", "1"));
    private static final DcbCriterion B = DcbCriteria.tags(Tag.of("b", "1"));

    @Test
    void the_constructor_throws_IllegalArgumentException_when_the_token_was_read_with_other_criteria() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        Throwable thrown = catchThrowable(() -> new DcbAppendCondition(B, Optional.of(tokenReadWithA)));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_deprecated_two_argument_fail_if_events_match_throws_IllegalArgumentException_when_the_token_was_read_with_other_criteria() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.failIfEventsMatch(B, tokenReadWithA));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void whole_store_lock_throws_IllegalArgumentException_when_the_token_was_not_read_with_all() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.wholeStoreLock(tokenReadWithA));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_message_names_both_criteria() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.failIfEventsMatch(B, tokenReadWithA));

        assertThat(thrown).hasMessageContaining(A.toString()).hasMessageContaining(B.toString());
    }

    @Test
    void the_whole_store_lock_message_uses_the_whole_store_wording_and_names_the_token_criteria() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.wholeStoreLock(tokenReadWithA));

        assertAll(
                () -> assertThat(thrown).hasMessageContaining("A whole-store lock needs a consistency token read with DcbCriteria.all()"),
                () -> assertThat(thrown).hasMessageContaining(A.toString())
        );
    }

    @Test
    void a_token_read_with_all_is_refused_for_scoped_criteria() {
        DcbConsistencyToken tokenReadWithAll = DcbConsistencyToken.of(DcbCriteria.all(), 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.failIfEventsMatch(A, tokenReadWithAll));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_token_read_with_a_wider_any_of_is_refused() {
        DcbConsistencyToken tokenReadWithAorB = DcbConsistencyToken.of(DcbCriteria.anyOf(A, B), 3);

        Throwable thrown = catchThrowable(() -> DcbAppendCondition.failIfEventsMatch(A, tokenReadWithAorB));

        assertThat(thrown).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_one_argument_fail_if_events_match_takes_the_criteria_from_the_token() {
        DcbCriteria readCriteria = DcbCriteria.anyOf(A, B);
        DcbConsistencyToken token = DcbConsistencyToken.of(readCriteria, 3);

        DcbAppendCondition condition = DcbAppendCondition.failIfEventsMatch(token);

        assertAll(
                () -> assertThat(condition.criteria()).isSameAs(readCriteria),
                () -> assertThat(condition.consistencyToken()).containsSame(token)
        );
    }

    @Test
    void the_one_argument_fail_if_events_match_accepts_a_token_read_with_all() {
        DcbConsistencyToken token = DcbConsistencyToken.of(DcbCriteria.all(), 3);

        DcbAppendCondition condition = DcbAppendCondition.failIfEventsMatch(token);

        assertThat(condition).isEqualTo(DcbAppendCondition.wholeStoreLock(token));
    }

    @Test
    void the_deprecated_two_argument_fail_if_events_match_accepts_the_same_criteria_built_in_another_order() {
        DcbConsistencyToken tokenReadWithAorB = DcbConsistencyToken.of(DcbCriteria.anyOf(A, B), 3);
        DcbCriteria conditionCriteria = DcbCriteria.anyOf(B, A);

        DcbAppendCondition condition = DcbAppendCondition.failIfEventsMatch(conditionCriteria, tokenReadWithAorB);

        assertAll(
                () -> assertThat(condition.criteria()).isSameAs(conditionCriteria),
                () -> assertThat(condition.consistencyToken()).containsSame(tokenReadWithAorB)
        );
    }

    @Test
    void the_constructor_accepts_a_criterion_and_an_items_holding_only_that_criterion() {
        DcbConsistencyToken tokenReadWithA = DcbConsistencyToken.of(A, 3);

        DcbAppendCondition condition = new DcbAppendCondition(new DcbCriteria.Items(List.of(A)), Optional.of(tokenReadWithA));

        assertThat(condition.consistencyToken()).containsSame(tokenReadWithA);
    }

    @Test
    void a_condition_without_a_token_accepts_any_criteria() {
        assertAll(
                () -> assertThat(DcbAppendCondition.failIfEventsMatch(A).consistencyToken()).isEmpty(),
                () -> assertThat(DcbAppendCondition.wholeStoreLock().consistencyToken()).isEmpty()
        );
    }
}
