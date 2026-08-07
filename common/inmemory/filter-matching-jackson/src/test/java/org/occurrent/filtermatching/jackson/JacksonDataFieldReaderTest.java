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
import io.cloudevents.core.data.PojoCloudEventData;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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

    @Test
    void the_first_occurrence_wins_for_a_duplicate_field_name() {
        // Valid JSON should not contain duplicate keys, so this only pins down a defined answer rather than a
        // requirement; a full tree parse into a Map would instead keep the last occurrence.
        CloudEvent event = eventWithJson("{\"name\":\"alice\",\"name\":\"bob\"}");
        assertThat(reader.read(event, "name")).contains("alice");
    }

    @Test
    void reads_a_top_level_field_directly_from_a_map_backed_event() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "alice");
        CloudEvent event = eventWithMap(data);
        assertThat(reader.read(event, "name")).contains("alice");
    }

    @Test
    void reads_a_nested_field_directly_from_a_map_backed_event() {
        Map<String, Object> city = new LinkedHashMap<>();
        city.put("city", "Malmo");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("person", city);
        CloudEvent event = eventWithMap(data);
        assertThat(reader.read(event, "person.city")).contains("Malmo");
    }

    @Test
    void a_dotted_path_traverses_an_array_of_maps_in_a_map_backed_event() {
        Map<String, Object> itemA = new LinkedHashMap<>();
        itemA.put("sku", "a");
        Map<String, Object> itemB = new LinkedHashMap<>();
        itemB.put("sku", "b");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("items", List.of(itemA, itemB));
        CloudEvent event = eventWithMap(data);
        Optional<Object> value = reader.read(event, "items.sku");
        assertThat(value).isPresent();
        assertThat(value.get()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("a", "b");
    }

    @Test
    void is_empty_when_the_path_continues_past_a_scalar_in_a_map_backed_event() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "alice");
        CloudEvent event = eventWithMap(data);
        assertThat(reader.read(event, "name.deeper")).isEmpty();
    }

    @Test
    void falls_back_to_the_bytes_when_a_pojo_cloud_event_data_does_not_wrap_a_map() {
        // A PojoCloudEventData wrapping something other than a Map, a List for instance, has nothing this reader
        // can walk without parsing, so it goes through the same byte-sourced path a non-Mongo event would.
        List<String> data = List.of("red", "blue");
        PojoCloudEventData<List<String>> wrapped = PojoCloudEventData.wrap(data,
                list -> ("[\"" + String.join("\",\"", list) + "\"]").getBytes(StandardCharsets.UTF_8));
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withDataContentType("application/json")
                .withData(wrapped)
                .build();
        assertThat(reader.read(event, "0")).isEmpty();
    }

    private static CloudEvent eventWithMap(Map<String, Object> data) {
        PojoCloudEventData<Map<String, Object>> wrapped = PojoCloudEventData.wrap(data, JacksonDataFieldReaderTest::toJsonBytes);
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withDataContentType("application/json")
                .withData(wrapped)
                .build();
    }

    private static byte[] toJsonBytes(Map<String, Object> map) {
        // Stands in for DocumentCloudEventReader's document.toJson().getBytes(UTF_8): if the map short-circuit did
        // not fire, this is what the reader would have to fall back to parsing.
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
