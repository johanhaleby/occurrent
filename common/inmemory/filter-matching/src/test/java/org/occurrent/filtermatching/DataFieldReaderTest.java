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

package org.occurrent.filtermatching;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the default {@link DataFieldReader#readAll(CloudEvent, java.util.Collection)}, which every implementation
 * gets by calling {@link DataFieldReader#read(CloudEvent, String)} once per path until it overrides the method.
 */
class DataFieldReaderTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:test"))
            .withType("test")
            .build();

    @Test
    void the_default_resolves_every_path_by_calling_read_once_per_path() {
        Map<String, Object> payload = Map.of("name", "Anna", "age", 42);
        AtomicInteger readCalls = new AtomicInteger();
        DataFieldReader reader = (event, path) -> {
            readCalls.incrementAndGet();
            return Optional.ofNullable(payload.get(path));
        };

        Map<String, Object> result = reader.readAll(EVENT, List.of("name", "age"));

        assertThat(result).containsExactlyInAnyOrderEntriesOf(Map.of("name", "Anna", "age", 42));
        assertThat(readCalls).hasValue(2);
    }

    @Test
    void the_default_omits_a_path_that_read_answers_empty_for() {
        DataFieldReader reader = (event, path) -> path.equals("present")
                ? Optional.of("value")
                : Optional.empty();

        Map<String, Object> result = reader.readAll(EVENT, List.of("present", "missing"));

        assertThat(result).containsExactly(Map.entry("present", "value"));
    }

    @Test
    void the_default_answers_an_empty_map_for_no_paths() {
        DataFieldReader reader = (event, path) -> Optional.of("unused");

        assertThat(reader.readAll(EVENT, List.of())).isEmpty();
    }
}
