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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the fluent {@link DcbCriteria} construction surface. The internal criterion representation is unchanged, so
 * these tests also prove the matcher, the Mongo converter, and the marker derivation keep seeing the same shapes.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCriteriaTest {

    @Test
    void type_builds_a_single_type_item() {
        DcbCriterion item = DcbCriteria.type("OrderPlaced");
        assertThat(item.types()).containsExactly("OrderPlaced");
        assertThat(item.tags()).isEmpty();
        assertThat(item.excludedTypes()).isEmpty();
    }

    @Test
    void types_are_any_of() {
        assertThat(DcbCriteria.types("A", "B").types()).containsExactlyInAnyOrder("A", "B");
        assertThat(DcbCriteria.types(List.of("A", "B")).types()).containsExactlyInAnyOrder("A", "B");
    }

    @Test
    void tags_are_all_of() {
        assertThat(DcbCriteria.tags(Tag.of("a", "1"), Tag.of("b", "2")).tags()).containsExactlyInAnyOrder(Tag.of("a", "1"), Tag.of("b", "2"));
        assertThat(DcbCriteria.tags(List.of(Tag.of("a", "1"), Tag.of("b", "2"))).tags()).containsExactlyInAnyOrder(Tag.of("a", "1"), Tag.of("b", "2"));
    }

    @Test
    void fluent_refinement_combines_type_tag_and_excluded_type() {
        DcbCriterion item = DcbCriteria.type("OrderPlaced").tags(Tag.of("order", "1")).excludingTypes("OrderDeleted");
        assertThat(item.types()).containsExactly("OrderPlaced");
        assertThat(item.tags()).containsExactly(Tag.of("order", "1"));
        assertThat(item.excludedTypes()).containsExactly("OrderDeleted");
    }

    @Test
    void a_single_alternative_is_itself_a_criteria() {
        assertThat((DcbCriteria) DcbCriteria.tags(Tag.of("a", "1"))).isInstanceOf(DcbCriterion.class);
    }

    @Test
    void anyOf_of_one_alternative_collapses_to_that_alternative() {
        DcbCriterion item = DcbCriteria.tags(Tag.of("a", "1"));
        assertThat(DcbCriteria.anyOf(item)).isEqualTo(item);
        assertThat(DcbCriteria.anyOf(List.of(item))).isEqualTo(item);
    }

    @Test
    void anyOf_of_several_alternatives_is_an_items_criteria() {
        DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("a", "1")), DcbCriteria.type("X"));
        assertThat(criteria).isInstanceOfSatisfying(DcbCriteria.Items.class, items ->
                assertThat(items.items()).containsExactly(
                        new DcbCriterion(Set.of(), Set.of(Tag.of("a", "1"))),
                        new DcbCriterion(Set.of("X"), Set.of())));
    }

    @Test
    void anyOf_flattens_nested_items() {
        DcbCriteria nested = DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("a", "1")), DcbCriteria.tags(Tag.of("b", "2")));
        DcbCriteria criteria = DcbCriteria.anyOf(nested, DcbCriteria.tags(Tag.of("c", "3")));
        assertThat(criteria).isInstanceOfSatisfying(DcbCriteria.Items.class, items ->
                assertThat(items.items()).hasSize(3));
    }

    @Test
    void anyOf_collapses_to_match_all_when_any_alternative_matches_all() {
        assertThat(DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("a", "1")), DcbCriteria.all())).isInstanceOf(DcbCriteria.MatchAll.class);
    }

    @Test
    void tagsAnyOf_is_an_or_of_single_tag_alternatives() {
        DcbCriteria criteria = DcbCriteria.tagsAnyOf(Tag.of("a", "1"), Tag.of("b", "2"));
        assertThat(criteria).isEqualTo(DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("a", "1")), DcbCriteria.tags(Tag.of("b", "2"))));
        assertThat(criteria).isInstanceOfSatisfying(DcbCriteria.Items.class, items ->
                assertThat(items.items()).containsExactly(
                        new DcbCriterion(Set.of(), Set.of(Tag.of("a", "1"))),
                        new DcbCriterion(Set.of(), Set.of(Tag.of("b", "2")))));
    }

    @Test
    void tagsAnyOf_of_one_tag_collapses_to_a_single_alternative() {
        assertThat(DcbCriteria.tagsAnyOf(Tag.of("a", "1"))).isEqualTo(DcbCriteria.tags(Tag.of("a", "1")));
    }
}
