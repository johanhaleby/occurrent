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

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class TagTest {

    @Test
    void canonical_joins_key_and_value_with_a_colon() {
        assertThat(Tag.of("k", "v").canonical()).isEqualTo("k:v");
    }

    @Test
    void parse_round_trips_a_canonical_tag() {
        Tag tag = Tag.of("k", "v");
        assertThat(Tag.parse("k:v")).isEqualTo(tag);
        assertThat(Tag.parse(tag.canonical())).isEqualTo(tag);
    }

    @Test
    void parse_splits_on_the_first_colon_so_a_value_may_contain_colons() {
        Tag tag = Tag.parse("email:a:b@x");
        assertThat(tag.key()).isEqualTo("email");
        assertThat(tag.value()).isEqualTo("a:b@x");
        // The value keeps its colons through a canonical round-trip.
        assertThat(Tag.parse(tag.canonical())).isEqualTo(tag);
    }

    @Test
    void parse_rejects_a_string_without_a_colon() {
        assertThatThrownBy(() -> Tag.parse("nocolon"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag must be in 'key:value' form: nocolon");
    }

    @Test
    void parse_rejects_a_blank_value() {
        assertThatThrownBy(() -> Tag.parse("k:"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag value cannot be blank");
    }

    @Test
    void parse_rejects_a_blank_key() {
        assertThatThrownBy(() -> Tag.parse(":v"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key cannot be blank");
    }

    @Test
    void of_rejects_a_blank_key_or_value() {
        assertThatThrownBy(() -> Tag.of(" ", "v"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key cannot be blank");
        assertThatThrownBy(() -> Tag.of("k", " "))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag value cannot be blank");
    }

    @Test
    void of_rejects_a_newline_in_key_or_value() {
        assertThatThrownBy(() -> Tag.of("k\nk", "v"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key/value cannot contain a newline");
        assertThatThrownBy(() -> Tag.of("k", "v\nv"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key/value cannot contain a newline");
    }

    @Test
    void of_strips_surrounding_whitespace_from_key_and_value() {
        assertThat(Tag.of(" k ", " v ")).isEqualTo(Tag.of("k", "v"));
    }

    @Test
    void of_rejects_a_colon_in_the_key() {
        assertThatThrownBy(() -> Tag.of("k:k", "v"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key cannot contain ':'");
    }

    @Test
    void tags_order_by_their_canonical_form() {
        TreeSet<Tag> tags = new TreeSet<>();
        tags.add(Tag.of("b", "1"));
        tags.add(Tag.of("a", "2"));
        assertThat(tags).containsExactly(Tag.of("a", "2"), Tag.of("b", "1"));
    }
}
