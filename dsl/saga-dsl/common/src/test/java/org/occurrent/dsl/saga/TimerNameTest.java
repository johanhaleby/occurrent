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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.dsl.saga.TimerName.Qualified;
import org.occurrent.dsl.saga.TimerName.Simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("TimerName")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TimerNameTest {

    // Legal wherever a colon is refused, which is a Simple name and a Qualified namespace.
    private static Stream<String> colon_free_strings() {
        return Stream.of(
                "",
                "payment",
                "a",
                "påminnelse",
                "タイマー",
                "⏰",
                " ",
                "  leading and trailing spaces  ",
                "a".repeat(10_000)
        );
    }

    // Legal wherever a colon is allowed, which is a Qualified name.
    private static Stream<String> arbitrary_names() {
        return Stream.of(
                "",
                "x",
                "a:b",
                "a:b:c",
                "a::b",
                "2026-08-11T10:15:30Z",
                "awaiting-players",
                "påminnelse",
                "タイマー",
                "⏰",
                " ",
                "a".repeat(10_000)
        );
    }

    // Every constructible TimerName, a Simple over each colon-free name and a Qualified over every colon-free
    // namespace crossed with every arbitrary name.
    private static Stream<TimerName> constructible_timer_names() {
        Stream<TimerName> simples = colon_free_strings().map(Simple::new);
        Stream<TimerName> qualifieds = colon_free_strings()
                .flatMap(namespace -> arbitrary_names().map(name -> new Qualified(namespace, name)));
        return Stream.concat(simples, qualifieds);
    }

    // Every string parse(String) must handle, including the awkward colon placements called out in its javadoc.
    private static Stream<String> arbitrary_strings() {
        return Stream.of(
                "",
                ":",
                ":x",
                "x:",
                "a:b:c",
                "a::b",
                "2026-08-11T10:15:30Z",
                "payment",
                "step:awaiting-players",
                "påminnelse",
                "タイマー",
                "⏰",
                " ",
                "  leading and trailing spaces  ",
                "a".repeat(10_000)
        );
    }

    @Nested
    class RoundTrips {

        @ParameterizedTest
        @MethodSource("org.occurrent.dsl.saga.TimerNameTest#constructible_timer_names")
        void parsing_a_constructible_values_own_encoding_returns_an_equal_value(TimerName timerName) {
            assertThat(TimerName.parse(timerName.encode())).isEqualTo(timerName);
        }

        @ParameterizedTest
        @MethodSource("org.occurrent.dsl.saga.TimerNameTest#arbitrary_strings")
        void parsing_a_string_and_encoding_it_back_returns_the_original_string(String name) {
            assertThat(TimerName.parse(name).encode()).isEqualTo(name);
        }
    }

    @Nested
    class Parse {

        @Test
        void empty_string_parses_to_an_empty_Simple_name() {
            assertThat(TimerName.parse("")).isEqualTo(new Simple(""));
        }

        @Test
        void a_lone_colon_parses_to_a_Qualified_name_with_an_empty_namespace_and_an_empty_name() {
            assertThat(TimerName.parse(":")).isEqualTo(new Qualified("", ""));
        }

        @Test
        void a_leading_colon_parses_to_a_Qualified_name_with_an_empty_namespace() {
            assertThat(TimerName.parse(":x")).isEqualTo(new Qualified("", "x"));
        }

        @Test
        void a_trailing_colon_parses_to_a_Qualified_name_with_an_empty_name() {
            assertThat(TimerName.parse("x:")).isEqualTo(new Qualified("x", ""));
        }

        @Test
        void several_colons_split_only_at_the_first_one() {
            assertThat(TimerName.parse("a:b:c")).isEqualTo(new Qualified("a", "b:c"));
        }

        @Test
        void a_colon_free_string_parses_to_a_Simple_name() {
            assertThat(TimerName.parse("payment")).isEqualTo(new Simple("payment"));
        }

        @Test
        void throws_NullPointerException_when_given_null() {
            assertThatThrownBy(() -> TimerName.parse(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Construction {

        @Test
        void Simple_throws_NullPointerException_when_name_is_null() {
            assertThatThrownBy(() -> new Simple(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void Simple_throws_IllegalArgumentException_when_name_contains_a_colon() {
            assertThatThrownBy(() -> new Simple("a:b"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void Qualified_throws_NullPointerException_when_namespace_is_null() {
            assertThatThrownBy(() -> new Qualified(null, "name"))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void Qualified_throws_NullPointerException_when_name_is_null() {
            assertThatThrownBy(() -> new Qualified("namespace", null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void Qualified_throws_IllegalArgumentException_when_namespace_contains_a_colon() {
            assertThatThrownBy(() -> new Qualified("a:b", "c"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void Qualified_accepts_a_colon_in_name() {
            assertThat(new Qualified("a", "b:c").encode()).isEqualTo("a:b:c");
        }

        @Test
        void of_throws_IllegalArgumentException_when_namespace_contains_a_colon() {
            assertThatThrownBy(() -> TimerName.of("a:b", "c"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void of_builds_a_Qualified_name() {
            assertThat(TimerName.of("a", "b")).isEqualTo(new Qualified("a", "b"));
        }
    }

    @Nested
    class ToStringReturnsEncode {

        @Test
        void on_a_Simple_name() {
            Simple simple = new Simple("payment");

            assertThat(simple.toString()).isEqualTo(simple.encode());
        }

        @Test
        void on_a_Qualified_name() {
            Qualified qualified = new Qualified("step", "awaiting-players");

            assertThat(qualified.toString()).isEqualTo(qualified.encode());
        }

        @Test
        void on_a_Qualified_name_that_looks_like_an_ISO_8601_timestamp() {
            TimerName timerName = TimerName.parse("2026-08-11T10:15:30Z");

            assertThat(timerName.toString()).isEqualTo(timerName.encode());
        }

        @Test
        void on_a_Qualified_name_with_an_empty_namespace() {
            TimerName timerName = TimerName.parse(":x");

            assertThat(timerName.toString()).isEqualTo(timerName.encode());
        }
    }

    @Nested
    class Distinctness {

        @Test
        void distinct_strings_parse_to_distinct_timer_names() {
            // The saga builder looks its timer handlers up by TimerName, so two timers whose stored strings differ
            // becoming one equal value would silently merge their handlers.
            List<String> strings = arbitrary_strings().collect(Collectors.toList());
            long distinctStrings = strings.stream().distinct().count();

            Set<TimerName> parsed = strings.stream().map(TimerName::parse).collect(Collectors.toSet());

            assertThat(parsed).hasSize((int) distinctStrings);
        }
    }

    @Nested
    class EqualsAndHashCode {

        @Test
        void parse_of_a_qualified_string_equals_the_equivalent_of_call() {
            assertThat(TimerName.parse("a:b")).isEqualTo(TimerName.of("a", "b"));
        }

        @Test
        void parse_of_a_qualified_string_and_the_equivalent_of_call_have_the_same_hashCode() {
            assertThat(TimerName.parse("a:b").hashCode()).isEqualTo(TimerName.of("a", "b").hashCode());
        }
    }
}
