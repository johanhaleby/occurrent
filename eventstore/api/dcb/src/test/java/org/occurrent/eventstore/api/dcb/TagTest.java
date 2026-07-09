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
    void of_joins_key_and_value_with_a_colon() {
        assertThat(Tag.of("k", "v").canonical()).isEqualTo("k:v");
        assertThat(Tag.of("k", "v").value()).isEqualTo("k:v");
    }

    @Test
    void of_supports_a_value_less_tag() {
        Tag tag = Tag.of("premium");
        assertThat(tag.canonical()).isEqualTo("premium");
        assertThat(tag.value()).isEqualTo("premium");
    }

    @Test
    void the_key_value_form_and_the_string_form_are_the_same_tag() {
        assertThat(Tag.of("course", "c1")).isEqualTo(Tag.of("course:c1"));
        assertThat(Tag.parse("course:c1")).isEqualTo(Tag.of("course", "c1"));
    }

    @Test
    void parse_round_trips_the_canonical_form() {
        assertThat(Tag.parse(Tag.of("k", "v").canonical())).isEqualTo(Tag.of("k", "v"));
        assertThat(Tag.parse(Tag.of("premium").canonical())).isEqualTo(Tag.of("premium"));
    }

    @Test
    void a_value_may_contain_colons() {
        Tag tag = Tag.of("email", "a:b@x");
        assertThat(tag.canonical()).isEqualTo("email:a:b@x");
        assertThat(Tag.parse(tag.canonical())).isEqualTo(tag);
    }

    @Test
    void of_rejects_a_blank_tag() {
        assertThatThrownBy(() -> Tag.of(" "))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag cannot be blank");
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
    void of_rejects_a_newline() {
        assertThatThrownBy(() -> Tag.of("a\nb"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag cannot contain a newline");
        assertThatThrownBy(() -> Tag.of("k", "v\nv"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag cannot contain a newline");
    }

    @Test
    void of_strips_surrounding_whitespace() {
        assertThat(Tag.of(" k ", " v ")).isEqualTo(Tag.of("k", "v"));
        assertThat(Tag.of("  premium  ")).isEqualTo(Tag.of("premium"));
    }

    @Test
    void of_rejects_a_colon_in_the_key() {
        assertThatThrownBy(() -> Tag.of("k:k", "v"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tag key cannot contain ':'");
    }

    @Test
    void tags_order_by_their_string_value() {
        TreeSet<Tag> tags = new TreeSet<>();
        tags.add(Tag.of("b", "1"));
        tags.add(Tag.of("a", "2"));
        tags.add(Tag.of("premium"));
        assertThat(tags).containsExactly(Tag.of("a", "2"), Tag.of("b", "1"), Tag.of("premium"));
    }
}
