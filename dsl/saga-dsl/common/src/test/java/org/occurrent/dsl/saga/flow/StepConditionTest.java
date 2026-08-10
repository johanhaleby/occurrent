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

package org.occurrent.dsl.saga.flow;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.dsl.saga.flow.StepCondition.*;

/**
 * Covers {@link StepCondition}'s construction surface in isolation, the {@code event} leaf factories and the
 * {@code allOf}/{@code anyOf} normalization laws (flatten, singleton collapse, empty rejected, declaration order
 * preserved), the same way {@code DcbCriteriaTest} checks {@code DcbCriteria.anyOf}. Evaluating a tree against a
 * received window is covered where the window lives, in {@code FlowSagaTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StepConditionTest {

    sealed interface TestEvent permits A, B, C {
    }

    record A(int value) implements TestEvent {
    }

    record B(int value) implements TestEvent {
    }

    record C(int value) implements TestEvent {
    }

    // --- event(...) leaves ------------------------------------------------------------------------------------------

    @Test
    void event_of_a_type_alone_defaults_to_count_one_and_no_predicate() {
        StepCondition<TestEvent> condition = event(A.class);

        assertThat(condition).isEqualTo(new AtLeast<>(new EventMatcher<>(A.class, null), 1));
    }

    @Test
    void event_with_a_count_carries_it_and_still_has_no_predicate() {
        StepCondition<TestEvent> condition = event(A.class, 3);

        assertThat(condition).isEqualTo(new AtLeast<>(new EventMatcher<>(A.class, null), 3));
    }

    @Test
    void event_with_a_predicate_defaults_to_count_one() {
        Predicate<A> positive = a -> a.value() > 0;

        StepCondition<TestEvent> condition = event(A.class, positive);

        assertThat(condition).isEqualTo(new AtLeast<>(new EventMatcher<>(A.class, widen(positive)), 1));
    }

    @Test
    void event_with_a_count_and_a_predicate_carries_both() {
        Predicate<A> positive = a -> a.value() > 0;

        StepCondition<TestEvent> condition = event(A.class, 2, positive);

        assertThat(condition).isEqualTo(new AtLeast<>(new EventMatcher<>(A.class, widen(positive)), 2));
    }

    // Widens a leaf's own predicate type to the tree's event type for the expected-value side of an equality assertion,
    // the same relationship StepCondition.event's unchecked cast relies on. The predicate only ever runs on an event its
    // own eventType.isInstance already accepted.
    @SuppressWarnings("unchecked")
    private static <E, T extends E> Predicate<E> widen(Predicate<T> predicate) {
        return (Predicate<E>) predicate;
    }

    @Test
    void event_rejects_a_count_below_one() {
        assertThatThrownBy(() -> event(A.class, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count");
        assertThatThrownBy(() -> event(A.class, -1, (Predicate<A>) a -> true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count");
    }

    @Test
    void event_rejects_a_null_type() {
        assertThatThrownBy(() -> event(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventType");
    }

    @Test
    void event_rejects_a_null_predicate_passed_to_the_predicate_only_overload() {
        assertThatThrownBy(() -> event(A.class, (Predicate<A>) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("predicate");
    }

    // --- AtLeast, AllOf, AnyOf constructed directly ------------------------------------------------------------------

    @Test
    void atLeast_rejects_a_count_below_one() {
        assertThatThrownBy(() -> new AtLeast<>(new EventMatcher<TestEvent>(A.class, null), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count");
    }

    @Test
    void allOf_and_anyOf_reject_an_empty_list_constructed_directly() {
        assertThatThrownBy(() -> new AllOf<>(List.<StepCondition<TestEvent>>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allOf")
                .hasMessageContaining("at least one");
        assertThatThrownBy(() -> new AnyOf<>(List.<StepCondition<TestEvent>>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anyOf")
                .hasMessageContaining("at least one");
    }

    @Test
    void allOf_and_anyOf_reject_a_null_element_constructed_directly() {
        List<StepCondition<TestEvent>> withNull = new ArrayList<>();
        withNull.add(event(A.class));
        withNull.add(null);

        assertThatThrownBy(() -> new AllOf<>(withNull)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AnyOf<>(withNull)).isInstanceOf(NullPointerException.class);
    }

    // --- allOf/anyOf normalization: singleton collapse ---------------------------------------------------------------

    @Test
    void allOf_of_one_condition_collapses_to_that_condition_rather_than_wrapping_it() {
        StepCondition<TestEvent> leaf = event(A.class);

        assertThat(allOf(leaf)).isSameAs(leaf);
        assertThat(allOf(List.of(leaf))).isSameAs(leaf);
    }

    @Test
    void anyOf_of_one_condition_collapses_to_that_condition_rather_than_wrapping_it() {
        StepCondition<TestEvent> leaf = event(A.class);

        assertThat(anyOf(leaf)).isSameAs(leaf);
        assertThat(anyOf(List.of(leaf))).isSameAs(leaf);
    }

    // --- allOf/anyOf normalization: several conditions, declaration order preserved, no dedupe -------------------------

    @Test
    void allOf_of_several_conditions_keeps_them_all_in_declaration_order() {
        StepCondition<TestEvent> a = event(A.class);
        StepCondition<TestEvent> b = event(B.class);
        StepCondition<TestEvent> c = event(C.class);

        StepCondition<TestEvent> condition = allOf(c, a, b);

        assertThat(condition).isInstanceOfSatisfying(AllOf.class, allOf ->
                assertThat(allOf.conditions()).containsExactly(c, a, b));
    }

    @Test
    void anyOf_of_several_conditions_keeps_them_all_in_declaration_order() {
        StepCondition<TestEvent> a = event(A.class);
        StepCondition<TestEvent> b = event(B.class);
        StepCondition<TestEvent> c = event(C.class);

        StepCondition<TestEvent> condition = anyOf(c, a, b);

        assertThat(condition).isInstanceOfSatisfying(AnyOf.class, anyOf ->
                assertThat(anyOf.conditions()).containsExactly(c, a, b));
    }

    @Test
    void allOf_does_not_deduplicate_a_repeated_condition() {
        StepCondition<TestEvent> a = event(A.class);

        StepCondition<TestEvent> condition = allOf(a, a);

        assertThat(condition).isInstanceOfSatisfying(AllOf.class, allOf ->
                assertThat(allOf.conditions()).containsExactly(a, a));
    }

    // --- allOf/anyOf normalization: same-kind flatten ---------------------------------------------------------------

    @Test
    void allOf_flattens_a_nested_allOf_but_not_a_nested_anyOf() {
        StepCondition<TestEvent> a = event(A.class);
        StepCondition<TestEvent> b = event(B.class);
        StepCondition<TestEvent> c = event(C.class);
        StepCondition<TestEvent> nestedAllOf = allOf(a, b);
        StepCondition<TestEvent> nestedAnyOf = anyOf(a, b);

        StepCondition<TestEvent> flattened = allOf(nestedAllOf, c);
        StepCondition<TestEvent> notFlattened = allOf(nestedAnyOf, c);

        assertThat(flattened).isInstanceOfSatisfying(AllOf.class, allOf ->
                assertThat(allOf.conditions()).containsExactly(a, b, c));
        assertThat(notFlattened).isInstanceOfSatisfying(AllOf.class, allOf ->
                assertThat(allOf.conditions()).containsExactly(nestedAnyOf, c));
    }

    @Test
    void anyOf_flattens_a_nested_anyOf_but_not_a_nested_allOf() {
        StepCondition<TestEvent> a = event(A.class);
        StepCondition<TestEvent> b = event(B.class);
        StepCondition<TestEvent> c = event(C.class);
        StepCondition<TestEvent> nestedAnyOf = anyOf(a, b);
        StepCondition<TestEvent> nestedAllOf = allOf(a, b);

        StepCondition<TestEvent> flattened = anyOf(nestedAnyOf, c);
        StepCondition<TestEvent> notFlattened = anyOf(nestedAllOf, c);

        assertThat(flattened).isInstanceOfSatisfying(AnyOf.class, anyOf ->
                assertThat(anyOf.conditions()).containsExactly(a, b, c));
        assertThat(notFlattened).isInstanceOfSatisfying(AnyOf.class, anyOf ->
                assertThat(anyOf.conditions()).containsExactly(nestedAllOf, c));
    }

    @Test
    void a_deeply_nested_chain_of_the_same_kind_flattens_to_one_level() {
        StepCondition<TestEvent> a = event(A.class);
        StepCondition<TestEvent> b = event(B.class);
        StepCondition<TestEvent> c = event(C.class);

        StepCondition<TestEvent> condition = allOf(allOf(allOf(a, b), c));

        assertThat(condition).isInstanceOfSatisfying(AllOf.class, allOf ->
                assertThat(allOf.conditions()).containsExactly(a, b, c));
    }

    // --- allOf/anyOf: empty collection rejected -------------------------------------------------------------------

    @Test
    void allOf_and_anyOf_reject_an_empty_collection() {
        assertThatThrownBy(() -> allOf(List.<StepCondition<TestEvent>>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allOf")
                .hasMessageContaining("at least one");
        assertThatThrownBy(() -> anyOf(List.<StepCondition<TestEvent>>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anyOf")
                .hasMessageContaining("at least one");
    }

    // --- class-literal shortcuts ---------------------------------------------------------------------------------

    @Test
    void allOf_of_class_literals_is_the_same_shape_as_allOf_of_the_equivalent_event_leaves() {
        StepCondition<TestEvent> viaClasses = allOf(A.class, B.class);
        StepCondition<TestEvent> viaLeaves = allOf(event(A.class), event(B.class));

        assertThat(viaClasses).isEqualTo(viaLeaves);
    }

    @Test
    void anyOf_of_class_literals_is_the_same_shape_as_anyOf_of_the_equivalent_event_leaves() {
        StepCondition<TestEvent> viaClasses = anyOf(A.class, B.class);
        StepCondition<TestEvent> viaLeaves = anyOf(event(A.class), event(B.class));

        assertThat(viaClasses).isEqualTo(viaLeaves);
    }

    @Test
    void allOf_of_a_single_class_literal_collapses_to_that_leaf() {
        StepCondition<TestEvent> condition = allOf(A.class);

        assertThat(condition).isEqualTo(event(A.class));
    }
}
