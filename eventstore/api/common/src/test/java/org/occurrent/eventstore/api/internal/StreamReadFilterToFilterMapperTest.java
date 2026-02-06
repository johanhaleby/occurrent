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
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.condition.Condition.eq;

@DisplayName("StreamReadFilterToFilterMapper")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StreamReadFilterToFilterMapperTest {

    @Nested
    @DisplayName("map")
    class MapPublic {

        @Test
        void adds_stream_id_to_stream_read_filter_when_stream_read_filter_is_not_null() {
            // Given
            var input = StreamReadFilter.type("MyType");

            // When
            var mapped = StreamReadFilterToFilterMapper.mapWithStreamId("myStreamId", input);

            // Then
            assertThat(mapped).isEqualTo(Filter.streamId("myStreamId").and(Filter.type("MyType")));
        }

        @Test
        void returns_only__stream_id_filter_when_stream_read_filter_is_null() {
            // Given
            StreamReadFilter input = null;

            // When
            var mapped = StreamReadFilterToFilterMapper.mapWithStreamId("myStreamId", input);

            // Then
            assertThat(mapped).isEqualTo(Filter.streamId("myStreamId"));
        }
    }

    @Nested
    @DisplayName("mapInternal")
    class MapInternal {

        @Nested
        @DisplayName("when filter is a single predicate")
        class When_filter_is_a_single_predicate {

            @Test
            void maps_attribute_filter_to_filter_single_condition() {
                // Given
                var input = StreamReadFilter.attribute(StreamReadFilter.TYPE, eq("MyType"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.type("MyType"));
            }

            @Test
            void maps_extension_filter_to_filter_single_condition() {
                // Given
                var input = StreamReadFilter.extension("x-trace-id", eq("t1"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.filter("x-trace-id", eq("t1")));
            }

            @Test
            void maps_data_filter_to_filter_data_predicate() {
                // Given
                var input = StreamReadFilter.data("order.id", eq("123"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.data("order.id", eq("123")));
            }

            @Test
            void preserves_time_condition_values_without_using_system_defaults() {
                // Given
                var boundaryInstant = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
                var input = StreamReadFilter.time(eq(boundaryInstant));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.time(eq(boundaryInstant)));
            }

            @Test
            void maps_source_convenience_to_equivalent_filter_source() {
                // Given
                var uri = URI.create("urn:test");
                var input = StreamReadFilter.source(uri);

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.source(uri));
            }
        }

        @Nested
        @DisplayName("when filter is composite")
        class When_filter_is_composite {

            @Test
            void maps_and_composition_to_filter_and_composition() {
                // Given
                var input = StreamReadFilter.type("A").and(StreamReadFilter.subject("S"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.type("A").and(Filter.subject("S")));
            }

            @Test
            void maps_or_composition_to_filter_or_composition() {
                // Given
                var input = StreamReadFilter.type("A").or(StreamReadFilter.type("B"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped).isEqualTo(Filter.type("A").or(Filter.type("B")));
            }

            @Test
            void preserves_filter_order_in_composition() {
                // Given
                var first = StreamReadFilter.type("A");
                var second = StreamReadFilter.subject("S");
                var input = first.and(second);

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped)
                        .isEqualTo(new Filter.CompositionFilter(
                                Filter.CompositionOperator.AND,
                                List.of(Filter.type("A"), Filter.subject("S"))
                        ));
            }

            @Test
            void preserves_duplicates_in_composition() {
                // Given
                var input = StreamReadFilter.type("A").and(StreamReadFilter.type("A"));

                // When
                var mapped = StreamReadFilterToFilterMapper.map(input);

                // Then
                assertThat(mapped)
                        .isEqualTo(new Filter.CompositionFilter(
                                Filter.CompositionOperator.AND,
                                List.of(Filter.type("A"), Filter.type("A"))
                        ));
            }

            @Test
            void mapping_is_deterministic_for_same_input() throws Throwable {
                // Given
                var input = StreamReadFilter.type("A")
                        .and(StreamReadFilter.subject("S"))
                        .or(StreamReadFilter.data("x", eq("1")));

                // When
                ThrowingSupplier<Filter> map = () -> StreamReadFilterToFilterMapper.map(input);
                var first = map.get();
                var second = map.get();

                // Then
                assertThat(first).isEqualTo(second);
            }
        }
    }
}
