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
 * Covers the fluent {@link DcbQuery} construction surface. The internal item representation is unchanged, so these tests
 * also prove the matcher, the Mongo converter, and the marker derivation keep seeing the same shapes.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbQueryTest {

    @Test
    void type_builds_a_single_type_item() {
        DcbQueryItem item = DcbQuery.type("OrderPlaced");
        assertThat(item.types()).containsExactly("OrderPlaced");
        assertThat(item.tags()).isEmpty();
        assertThat(item.excludedTypes()).isEmpty();
    }

    @Test
    void types_are_any_of() {
        assertThat(DcbQuery.types("A", "B").types()).containsExactlyInAnyOrder("A", "B");
        assertThat(DcbQuery.types(List.of("A", "B")).types()).containsExactlyInAnyOrder("A", "B");
    }

    @Test
    void tags_are_all_of() {
        assertThat(DcbQuery.tags("a", "b").tags()).containsExactlyInAnyOrder("a", "b");
        assertThat(DcbQuery.tags(List.of("a", "b")).tags()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void fluent_refinement_combines_type_tag_and_excluded_type() {
        DcbQueryItem item = DcbQuery.type("OrderPlaced").tags("order:1").excludingTypes("OrderDeleted");
        assertThat(item.types()).containsExactly("OrderPlaced");
        assertThat(item.tags()).containsExactly("order:1");
        assertThat(item.excludedTypes()).containsExactly("OrderDeleted");
    }

    @Test
    void a_single_alternative_is_itself_a_query() {
        assertThat((DcbQuery) DcbQuery.tags("a")).isInstanceOf(DcbQueryItem.class);
    }

    @Test
    void anyOf_of_one_alternative_collapses_to_that_alternative() {
        DcbQueryItem item = DcbQuery.tags("a");
        assertThat(DcbQuery.anyOf(item)).isEqualTo(item);
        assertThat(DcbQuery.anyOf(List.of(item))).isEqualTo(item);
    }

    @Test
    void anyOf_of_several_alternatives_is_an_items_query() {
        DcbQuery query = DcbQuery.anyOf(DcbQuery.tags("a"), DcbQuery.type("X"));
        assertThat(query).isInstanceOfSatisfying(DcbQuery.Items.class, items ->
                assertThat(items.items()).containsExactly(
                        new DcbQueryItem(Set.of(), Set.of("a")),
                        new DcbQueryItem(Set.of("X"), Set.of())));
    }

    @Test
    void anyOf_flattens_nested_items() {
        DcbQuery nested = DcbQuery.anyOf(DcbQuery.tags("a"), DcbQuery.tags("b"));
        DcbQuery query = DcbQuery.anyOf(nested, DcbQuery.tags("c"));
        assertThat(query).isInstanceOfSatisfying(DcbQuery.Items.class, items ->
                assertThat(items.items()).hasSize(3));
    }

    @Test
    void anyOf_collapses_to_match_all_when_any_alternative_matches_all() {
        assertThat(DcbQuery.anyOf(DcbQuery.tags("a"), DcbQuery.all())).isInstanceOf(DcbQuery.MatchAll.class);
    }

    @Test
    void tagsAnyOf_is_an_or_of_single_tag_alternatives() {
        DcbQuery query = DcbQuery.tagsAnyOf("a", "b");
        assertThat(query).isEqualTo(DcbQuery.anyOf(DcbQuery.tags("a"), DcbQuery.tags("b")));
        assertThat(query).isInstanceOfSatisfying(DcbQuery.Items.class, items ->
                assertThat(items.items()).containsExactly(
                        new DcbQueryItem(Set.of(), Set.of("a")),
                        new DcbQueryItem(Set.of(), Set.of("b"))));
    }

    @Test
    void tagsAnyOf_of_one_tag_collapses_to_a_single_alternative() {
        assertThat(DcbQuery.tagsAnyOf("a")).isEqualTo(DcbQuery.tags("a"));
    }
}
