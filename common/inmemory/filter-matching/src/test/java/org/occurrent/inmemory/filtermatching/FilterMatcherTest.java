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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.condition.Condition.eq;

/**
 * Covers the two-arg {@code matchesFilter} overload staying exactly as before (three shared subscription call
 * sites rely on it not needing a {@link DataFieldReader}) and the new three-arg overload threading a reader
 * through composition filters.
 */
class FilterMatcherTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:test"))
            .withType("test")
            .build();

    private static DataFieldReader readerOver(Map<String, Object> payload) {
        return (event, path) -> Optional.ofNullable(payload.get(path));
    }

    @Test
    void the_two_arg_overload_refuses_a_data_filter_exactly_like_before() {
        Filter dataFilter = Filter.data("amount", eq(42));

        assertThatThrownBy(() -> FilterMatcher.matchesFilter(EVENT, dataFilter))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_three_arg_overload_answers_a_data_filter_using_the_supplied_reader() {
        Filter dataFilter = Filter.data("amount", eq(42));
        DataFieldReader reader = readerOver(Map.of("amount", 42));

        assertThat(FilterMatcher.matchesFilter(EVENT, dataFilter, reader)).isTrue();
    }

    @Test
    void a_reader_is_threaded_through_a_composition_filter() {
        Filter both = Filter.type("test").and(Filter.data("amount", eq(42)));
        DataFieldReader reader = readerOver(Map.of("amount", 42));

        assertThat(FilterMatcher.matchesFilter(EVENT, both, reader)).isTrue();
        assertThat(FilterMatcher.matchesFilter(EVENT, both, readerOver(Map.of("amount", 1)))).isFalse();
    }

    @Test
    void a_non_data_filter_behaves_identically_regardless_of_which_overload_is_used() {
        Filter byType = Filter.type("test");

        assertThat(FilterMatcher.matchesFilter(EVENT, byType)).isTrue();
        assertThat(FilterMatcher.matchesFilter(EVENT, byType, DataFieldReader.refusing())).isTrue();
    }
}
