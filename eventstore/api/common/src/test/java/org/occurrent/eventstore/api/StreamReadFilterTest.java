/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.condition.Condition;

import java.util.Locale;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

@DisplayName("StreamReadFilter")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StreamReadFilterTest {

    @Nested
    @DisplayName("when creating attribute filters")
    class When_creating_attribute_filters {

        @ParameterizedTest
        @MethodSource("org.occurrent.eventstore.api.StreamReadFilterTest#forbidden_name_variants")
        @DisplayName("rejects reserved stream fields")
        void rejects_reserved_stream_fields(String reservedName) {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.attribute(reservedName, eq("x")));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain attribute");
        }

        @Test
        void rejects_null_attribute_name() {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.attribute(null, eq("x")));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessageContaining("attribute name cannot be null");
        }

        @Test
        void rejects_null_condition() {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.attribute("type", null));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Condition cannot be null");
        }

        @ParameterizedTest
        @MethodSource("org.occurrent.eventstore.api.StreamReadFilterTest#allowed_attribute_names")
        @DisplayName("accepts non reserved names")
        void accepts_non_reserved_names(String name) {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.attribute(name, eq("x")));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void rejects_reserved_name_independent_of_default_locale() {
            // Given
            var previousDefaultLocale = Locale.getDefault();
            Locale.setDefault(new Locale("tr", "TR"));

            try {
                // When
                var thrown = catchThrowable(() -> StreamReadFilter.attribute("  " + OccurrentCloudEventExtension.STREAM_ID.toUpperCase(Locale.ROOT) + "  ", eq("x")));

                // Then
                assertThat(thrown)
                        .isExactlyInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("StreamReadFilter must not constrain attribute");
            } finally {
                Locale.setDefault(previousDefaultLocale);
            }
        }
    }

    @Nested
    @DisplayName("when creating extension filters")
    class When_creating_extension_filters {

        @ParameterizedTest
        @MethodSource("org.occurrent.eventstore.api.StreamReadFilterTest#forbidden_name_variants")
        @DisplayName("rejects reserved stream fields")
        void rejects_reserved_stream_fields(String reservedName) {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.extension(reservedName, eq("x")));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain extension");
        }

        @Test
        void rejects_null_extension_name() {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.extension(null, eq("x")));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessageContaining("extension name cannot be null");
        }

        @Test
        void rejects_null_condition() {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.extension("x-correlation-id", (Condition<String>) null));

            // Then
            assertThat(thrown)
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Condition cannot be null");
        }

        @ParameterizedTest
        @MethodSource("org.occurrent.eventstore.api.StreamReadFilterTest#allowed_extension_names")
        @DisplayName("accepts non reserved names")
        void accepts_non_reserved_names(String name) {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilter.extension(name, eq("x")));

            // Then
            assertThat(thrown).isNull();
        }
    }

    private static Stream<String> forbidden_name_variants() {
        var streamId = OccurrentCloudEventExtension.STREAM_ID;
        var streamVersion = OccurrentCloudEventExtension.STREAM_VERSION;
        return Stream.of(
                streamId,
                streamId.toUpperCase(Locale.ROOT),
                "  " + streamId + "  ",
                "  " + streamId.toUpperCase(Locale.ROOT) + "  ",
                streamVersion,
                streamVersion.toUpperCase(Locale.ROOT),
                "  " + streamVersion + "  ",
                "  " + streamVersion.toUpperCase(Locale.ROOT) + "  "
        );
    }

    private static Stream<String> allowed_attribute_names() {
        return Stream.of("type", "source", "subject", "x-trace-id", " ");
    }

    private static Stream<String> allowed_extension_names() {
        return Stream.of("x-correlation-id", "stream-id", "stream_version", " ");
    }
}
