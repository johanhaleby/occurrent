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

package org.occurrent.filtermatching.jackson;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class JacksonDataFieldReaderTest {

    private static final URI SOURCE = URI.create("urn:test");

    private final JacksonDataFieldReader reader = new JacksonDataFieldReader();

    @Test
    void reads_a_top_level_string_field() {
        CloudEvent event = eventWithJson("{\"name\":\"alice\"}");
        assertThat(reader.read(event, "name")).contains("alice");
    }

    @Test
    void reads_a_nested_field_through_a_dotted_path() {
        CloudEvent event = eventWithJson("{\"person\":{\"city\":\"Malmo\"}}");
        assertThat(reader.read(event, "person.city")).contains("Malmo");
    }

    @Test
    void reads_a_whole_number_as_a_number_not_as_text() {
        CloudEvent event = eventWithJson("{\"amount\":42}");
        Optional<Object> value = reader.read(event, "amount");
        assertThat(value).isPresent();
        assertThat(value.get()).isInstanceOf(Number.class);
        assertThat(((Number) value.get()).intValue()).isEqualTo(42);
    }

    @Test
    void reads_a_fractional_number_as_a_number() {
        CloudEvent event = eventWithJson("{\"amount\":42.5}");
        Optional<Object> value = reader.read(event, "amount");
        assertThat(value).isPresent();
        assertThat(value.get()).isInstanceOf(Number.class);
        assertThat(((Number) value.get()).doubleValue()).isEqualTo(42.5);
    }

    @Test
    void reads_a_boolean_field() {
        CloudEvent event = eventWithJson("{\"active\":true}");
        assertThat(reader.read(event, "active")).contains(true);
    }

    @Test
    void reads_an_array_field_as_a_plain_list_rather_than_a_match_decision() {
        CloudEvent event = eventWithJson("{\"tags\":[\"red\",\"blue\"]}");
        Optional<Object> value = reader.read(event, "tags");
        assertThat(value).isPresent();
        assertThat(value.get()).isInstanceOf(List.class);
        assertThat(value.get()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("red", "blue");
    }

    @Test
    void a_dotted_path_traverses_an_array_of_objects_the_way_mongodb_does() {
        CloudEvent event = eventWithJson("{\"items\":[{\"sku\":\"a\"},{\"sku\":\"b\"}]}");
        Optional<Object> value = reader.read(event, "items.sku");
        assertThat(value).isPresent();
        assertThat(value.get()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("a", "b");
    }

    @Test
    void a_dotted_path_through_an_array_of_objects_skips_an_element_missing_the_field() {
        CloudEvent event = eventWithJson("{\"items\":[{\"sku\":\"a\"},{\"other\":\"x\"}]}");
        Optional<Object> value = reader.read(event, "items.sku");
        assertThat(value).isPresent();
        assertThat(value.get()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("a");
    }

    @Test
    void is_empty_when_a_dotted_path_through_an_array_of_objects_matches_no_element() {
        CloudEvent event = eventWithJson("{\"items\":[{\"other\":\"x\"}]}");
        assertThat(reader.read(event, "items.sku")).isEmpty();
    }

    @Test
    void reads_an_object_field_as_a_plain_map() {
        CloudEvent event = eventWithJson("{\"person\":{\"city\":\"Malmo\"}}");
        Optional<Object> value = reader.read(event, "person");
        assertThat(value).isPresent();
        assertThat(value.get()).isInstanceOf(Map.class);
        assertThat(value.get()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP).containsEntry("city", "Malmo");
    }

    @Test
    void is_empty_when_the_field_is_absent() {
        CloudEvent event = eventWithJson("{\"name\":\"alice\"}");
        assertThat(reader.read(event, "nosuchfield")).isEmpty();
    }

    @Test
    void is_empty_when_the_path_continues_past_a_scalar() {
        CloudEvent event = eventWithJson("{\"name\":\"alice\"}");
        assertThat(reader.read(event, "name.deeper")).isEmpty();
    }

    @Test
    void is_empty_when_the_json_root_is_an_array_rather_than_an_object() {
        CloudEvent event = eventWithJson("[1,2,3]");
        assertThat(reader.read(event, "0")).isEmpty();
    }

    @Test
    void is_empty_when_the_json_root_is_a_bare_scalar() {
        CloudEvent event = eventWithJson("\"just a string\"");
        assertThat(reader.read(event, "anything")).isEmpty();
    }

    @Test
    void is_empty_for_a_payload_that_is_not_json_at_all() {
        CloudEvent event = eventWithBytes("not json {{{".getBytes(StandardCharsets.UTF_8), "application/json");
        assertThat(reader.read(event, "name")).isEmpty();
    }

    @Test
    void is_empty_for_truncated_json() {
        CloudEvent event = eventWithBytes("{\"name\":\"ali".getBytes(StandardCharsets.UTF_8), "application/json");
        assertThat(reader.read(event, "name")).isEmpty();
    }

    @Test
    void is_empty_when_data_is_absent() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .build();
        assertThat(reader.read(event, "name")).isEmpty();
    }

    @Test
    void does_not_parse_a_payload_whose_content_type_does_not_say_json() {
        // Bytes that would parse fine as JSON, but the event says they are plain text, the same way MongoDB leaves a
        // non-JSON-declared payload as an opaque value it cannot query by field.
        CloudEvent event = eventWithBytes("{\"name\":\"alice\"}".getBytes(StandardCharsets.UTF_8), "text/plain");
        assertThat(reader.read(event, "name")).isEmpty();
    }

    @Test
    void treats_a_missing_content_type_as_json_per_the_cloudevents_default() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withData("{\"name\":\"alice\"}".getBytes(StandardCharsets.UTF_8))
                .build();
        assertThat(event.getDataContentType()).isNull();
        assertThat(reader.read(event, "name")).contains("alice");
    }

    @Test
    void treats_a_json_suffixed_content_type_as_json() {
        CloudEvent event = eventWithBytes("{\"name\":\"alice\"}".getBytes(StandardCharsets.UTF_8), "application/vnd.acme+json");
        assertThat(reader.read(event, "name")).contains("alice");
    }

    private static CloudEvent eventWithJson(String json) {
        return eventWithBytes(json.getBytes(StandardCharsets.UTF_8), "application/json");
    }

    private static CloudEvent eventWithBytes(byte[] data, @Nullable String contentType) {
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withData(data);
        if (contentType != null) {
            builder.withDataContentType(contentType);
        }
        return builder.build();
    }
}
