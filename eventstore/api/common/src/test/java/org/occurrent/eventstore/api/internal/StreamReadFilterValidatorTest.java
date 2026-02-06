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

package org.occurrent.eventstore.api.internal;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.StreamReadFilter;

import java.util.Locale;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

@DisplayName("StreamReadFilterValidator")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StreamReadFilterValidatorTest {

    @Nested
    @DisplayName("when creating StreamReadFilter using factory methods")
    class FactoryValidation {

        @Test
        void rejects_forbidden_attribute_name_when_creating_filter() {
            Throwable thrown = catchThrowable(() -> StreamReadFilter.attribute(OccurrentCloudEventExtension.STREAM_ID, eq("x")));
            assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain attribute");
        }

        @Test
        void rejects_forbidden_extension_name_when_creating_filter() {
            Throwable thrown = catchThrowable(() -> StreamReadFilter.extension(OccurrentCloudEventExtension.STREAM_VERSION, eq("1")));
            assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain extension");
        }
    }

    @Nested
    @DisplayName("when filter is null")
    class When_filter_is_null {

        @Test
        void rejects_null_filter() {
            // Given

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(null));

            // Then
            assertThat(thrown)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("StreamReadFilter cannot be null");
        }
    }

    @Nested
    @DisplayName("when filter is allowed")
    class When_filter_is_allowed {

        @Test
        void accepts_data_filter() {
            // Given
            var filter = StreamReadFilter.data("orderId", eq("123"));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void accepts_non_reserved_extension_filter() {
            // Given
            var filter = StreamReadFilter.extension("myExtension", eq("value"));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void accepts_attribute_filter_for_regular_cloudevent_attribute() {
            // Given
            var filter = StreamReadFilter.attribute(StreamReadFilter.TYPE, eq("MyEventType"));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown).isNull();
        }

        @Test
        void is_deterministic_for_same_input() throws Throwable {
            // Given
            var filter = StreamReadFilter.type("MyEventType").and(StreamReadFilter.subject("subject-1"));

            // When
            ThrowingSupplier<Throwable> validate = () -> catchThrowable(() -> StreamReadFilterValidator.validate(filter));
            var first = validate.get();
            var second = validate.get();

            // Then
            assertThat(first).isNull();
            assertThat(second).isNull();
        }
    }

    @Nested
    @DisplayName("when filter attempts to constrain stream identity or stream concurrency")
    class When_filter_attempts_to_constrain_stream_identity_or_stream_concurrency {

        @ParameterizedTest
        @MethodSource("forbidden_names_as_attribute_variants")
        @DisplayName("rejects forbidden names supplied via attribute")
        void rejects_forbidden_names_supplied_via_attribute(String forbiddenName) {
            // Given
            var filter = new StreamReadFilter.AttributeFilter<>(forbiddenName, eq("x"));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain attribute");
        }

        @ParameterizedTest
        @MethodSource("forbidden_names_as_extension_variants")
        @DisplayName("rejects forbidden names supplied via extension")
        void rejects_forbidden_names_supplied_via_extension(String forbiddenName) {
            // Given
            var filter = new StreamReadFilter.ExtensionFilter<>(forbiddenName, eq("x"));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain extension");
        }

        @Test
        void rejects_forbidden_names_independent_of_default_locale() {
            // Given
            var previous = Locale.getDefault();
            Locale.setDefault(new Locale("tr", "TR"));
            try {
                var filter = new StreamReadFilter.ExtensionFilter<>("  " + OccurrentCloudEventExtension.STREAM_ID.toUpperCase(Locale.ROOT) + "  ", eq("x"));

                // When
                var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

                // Then
                assertThat(thrown)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("StreamReadFilter must not constrain extension");
            } finally {
                Locale.setDefault(previous);
            }
        }

        static Stream<String> forbidden_names_as_attribute_variants() {
            return forbiddenNameVariants();
        }

        static Stream<String> forbidden_names_as_extension_variants() {
            return forbiddenNameVariants();
        }

        private static Stream<String> forbiddenNameVariants() {
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
    }

    @Nested
    @DisplayName("when filter is composite")
    class When_filter_is_composite {

        @Test
        void rejects_forbidden_name_nested_in_composition() {
            // Given
            var filter = StreamReadFilter.type("MyEventType")
                    .and(new StreamReadFilter.ExtensionFilter<>(OccurrentCloudEventExtension.STREAM_VERSION, eq("1")));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("StreamReadFilter must not constrain extension");
        }

        @Test
        void accepts_deeply_nested_valid_composition() {
            // Given
            var filter = StreamReadFilter.type("A")
                    .or(StreamReadFilter.subject("s1").and(StreamReadFilter.data("orderId", eq("1"))))
                    .and(StreamReadFilter.extension("x-trace-id", eq("t1")));

            // When
            var thrown = catchThrowable(() -> StreamReadFilterValidator.validate(filter));

            // Then
            assertThat(thrown).isNull();
        }
    }
}
