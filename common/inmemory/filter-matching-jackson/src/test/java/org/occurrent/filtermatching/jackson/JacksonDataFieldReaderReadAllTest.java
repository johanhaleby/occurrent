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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link JacksonDataFieldReader#readAll(CloudEvent, java.util.Collection)}, the override this reader adds so
 * a byte-backed payload with several requested paths is parsed once instead of once per path (the default on
 * {@link org.occurrent.filtermatching.DataFieldReader} loops {@link JacksonDataFieldReader#read}, which is exactly
 * the repeated reparse #623 is about).
 * <p>
 * The property-style tests compare {@code readAll} against the already-trusted {@code read}, called once per path,
 * across many randomly shaped documents. The two are required to agree on every path for every document, which is
 * what proves the single-pass rewrite still answers exactly what the old per-path reads answered.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class JacksonDataFieldReaderReadAllTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JacksonDataFieldReader reader = new JacksonDataFieldReader();

    @Test
    void resolves_several_top_level_paths_in_one_call() {
        CloudEvent event = eventWithJson("{\"name\":\"alice\",\"age\":42,\"active\":true}");

        Map<String, Object> result = reader.readAll(event, List.of("name", "age", "active"));

        assertThat(result).containsExactly(Map.entry("name", "alice"), Map.entry("age", 42), Map.entry("active", true));
    }

    @Test
    void omits_a_path_that_is_absent_while_keeping_the_ones_that_are_present() {
        CloudEvent event = eventWithJson("{\"name\":\"alice\"}");

        Map<String, Object> result = reader.readAll(event, List.of("name", "nosuchfield"));

        assertThat(result).containsExactly(Map.entry("name", "alice"));
    }

    @Test
    void answers_an_empty_map_for_no_paths_without_parsing() {
        CloudEvent event = eventWithBytes("not json {{{".getBytes(StandardCharsets.UTF_8), "application/json");

        assertThat(reader.readAll(event, List.of())).isEmpty();
    }

    @Test
    void resolves_two_paths_that_share_a_prefix_by_walking_the_shared_object_once() {
        CloudEvent event = eventWithJson("{\"person\":{\"city\":\"Malmo\",\"age\":30}}");

        Map<String, Object> result = reader.readAll(event, List.of("person.city", "person.age"));

        assertThat(result).containsExactly(Map.entry("person.city", "Malmo"), Map.entry("person.age", 30));
    }

    @Test
    void resolves_a_path_that_ends_at_a_node_alongside_a_path_that_continues_past_it() {
        // "person" is terminal right where "person.city" still needs to look inside the same object, the mixed
        // case that forces a single materialisation instead of pure streaming.
        CloudEvent event = eventWithJson("{\"person\":{\"city\":\"Malmo\",\"age\":30}}");

        Map<String, Object> result = reader.readAll(event, List.of("person", "person.city"));

        assertThat(result).containsKey("person");
        assertThat(result.get("person")).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("city", "Malmo")
                .containsEntry("age", 30);
        assertThat(result).containsEntry("person.city", "Malmo");
    }

    @Test
    void resolves_a_dotted_path_through_an_array_of_objects_alongside_a_sibling_path() {
        CloudEvent event = eventWithJson("{\"items\":[{\"sku\":\"a\",\"price\":5},{\"sku\":\"b\",\"price\":7}],\"name\":\"cart\"}");

        Map<String, Object> result = reader.readAll(event, List.of("items.sku", "name"));

        assertThat(result.get("items.sku")).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("a", "b");
        assertThat(result).containsEntry("name", "cart");
    }

    @Test
    void resolves_two_paths_that_both_traverse_the_same_array_of_objects() {
        CloudEvent event = eventWithJson("{\"items\":[{\"sku\":\"a\",\"price\":5},{\"sku\":\"b\",\"price\":7}]}");

        Map<String, Object> result = reader.readAll(event, List.of("items.sku", "items.price"));

        assertThat(result.get("items.sku")).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly("a", "b");
        assertThat(result.get("items.price")).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).containsExactly(5, 7);
    }

    @Test
    void keeps_the_caller_supplied_path_order_regardless_of_field_order_in_the_document() {
        CloudEvent event = eventWithJson("{\"c\":3,\"a\":1,\"b\":2}");

        Map<String, Object> result = reader.readAll(event, List.of("b", "c", "a"));

        assertThat(result.keySet()).containsExactly("b", "c", "a");
    }

    @Test
    void read_and_readAll_agree_when_a_later_sibling_field_holds_a_nested_field_of_the_same_name() {
        // A target field that is not an element's last field, followed by a sibling whose own value holds a
        // nested field of the same name, used to leak that nested field into the match: found by the property
        // tests above, and fixed by having the object scan always finish the rest of its own fields with
        // skipChildren() before an enclosing array loop resumes, rather than leaving that to accidental luck.
        CloudEvent event = eventWithJson("{\"items\":[{\"id\":1,\"meta\":{\"id\":\"nested\"}}]}");

        Optional<Object> viaRead = reader.read(event, "items.id");
        Map<String, Object> viaReadAll = reader.readAll(event, List.of("items.id"));

        assertThat(viaRead).contains(List.of(1));
        assertThat(viaReadAll).containsExactly(Map.entry("items.id", List.of(1)));
    }

    @Test
    void a_field_reached_before_malformed_bytes_still_resolves_while_a_later_one_does_not() {
        CloudEvent event = eventWithBytes("{\"early\":\"ok\",\"broken\":[1,,2],\"late\":\"unreached\"}".getBytes(StandardCharsets.UTF_8), "application/json");

        Map<String, Object> result = reader.readAll(event, List.of("early", "late"));

        assertThat(result).containsExactly(Map.entry("early", "ok"));
    }

    @Test
    void answers_an_empty_map_when_the_content_type_is_not_json() {
        CloudEvent event = eventWithBytes("{\"name\":\"alice\"}".getBytes(StandardCharsets.UTF_8), "text/plain");

        assertThat(reader.readAll(event, List.of("name"))).isEmpty();
    }

    @Test
    void answers_an_empty_map_when_data_is_absent() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .build();

        assertThat(reader.readAll(event, List.of("name"))).isEmpty();
    }

    @Test
    void resolves_several_top_level_paths_directly_from_a_map_backed_event() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "alice");
        data.put("age", 42);
        CloudEvent event = eventWithMap(data);

        Map<String, Object> result = reader.readAll(event, List.of("name", "age", "nosuchfield"));

        assertThat(result).containsExactly(Map.entry("name", "alice"), Map.entry("age", 42));
    }

    @RepeatedTest(200)
    void agrees_with_calling_read_once_per_path_on_a_randomly_shaped_byte_backed_document(org.junit.jupiter.api.RepetitionInfo repetitionInfo) {
        Random random = new Random(repetitionInfo.getCurrentRepetition());
        Map<String, Object> document = RandomJson.randomObject(random, 3);
        CloudEvent event = eventWithJson(toJson(document));
        List<String> paths = RandomJson.candidatePaths(random, document);

        Map<String, Object> expected = new LinkedHashMap<>();
        for (String path : paths) {
            reader.read(event, path).ifPresent(value -> expected.put(path, value));
        }

        Map<String, Object> actual = reader.readAll(event, paths);

        assertThat(actual)
                .as("document %s, paths %s", document, paths)
                .isEqualTo(expected);
    }

    @RepeatedTest(200)
    void agrees_with_calling_read_once_per_path_on_a_randomly_shaped_map_backed_document(org.junit.jupiter.api.RepetitionInfo repetitionInfo) {
        Random random = new Random(1_000_000L + repetitionInfo.getCurrentRepetition());
        Map<String, Object> document = RandomJson.randomObject(random, 3);
        CloudEvent event = eventWithMap(document);
        List<String> paths = RandomJson.candidatePaths(random, document);

        Map<String, Object> expected = new LinkedHashMap<>();
        for (String path : paths) {
            reader.read(event, path).ifPresent(value -> expected.put(path, value));
        }

        Map<String, Object> actual = reader.readAll(event, paths);

        assertThat(actual)
                .as("document %s, paths %s", document, paths)
                .isEqualTo(expected);
    }

    /**
     * Builds random JSON-object shapes (nested objects, arrays of objects with several fields, shared field names
     * at every nesting depth so generated paths share prefixes and a target field can collide with a nested field
     * of the same name inside a later sibling) and candidate paths over them, real and fabricated, for the
     * property-style comparison above.
     */
    private static final class RandomJson {

        private static final String[] FIELD_NAME_POOL = {"a", "b", "c", "d"};

        private RandomJson() {
        }

        static Map<String, Object> randomObject(Random random, int depth) {
            Map<String, Object> object = new LinkedHashMap<>();
            int fieldCount = 1 + random.nextInt(FIELD_NAME_POOL.length);
            for (int i = 0; i < fieldCount; i++) {
                object.put(FIELD_NAME_POOL[i], randomValue(random, depth));
            }
            return object;
        }

        private static Object randomValue(Random random, int depth) {
            int choice = depth <= 0 ? random.nextInt(3) : random.nextInt(5);
            return switch (choice) {
                case 0 -> "value" + random.nextInt(5);
                case 1 -> random.nextInt(100);
                case 2 -> random.nextBoolean();
                case 3 -> randomObject(random, depth - 1);
                default -> randomArrayOfObjects(random, depth - 1);
            };
        }

        private static List<Object> randomArrayOfObjects(Random random, int depth) {
            int elementCount = random.nextInt(4);
            List<Object> elements = new ArrayList<>();
            for (int i = 0; i < elementCount; i++) {
                // Every element gets at least two fields, so the target field is not always last in its object,
                // exercising the array-of-multi-field-objects traversal rather than the simpler single-field case.
                Map<String, Object> element = new LinkedHashMap<>();
                int fieldCount = 2 + random.nextInt(FIELD_NAME_POOL.length - 1);
                for (int f = 0; f < fieldCount; f++) {
                    element.put(FIELD_NAME_POOL[f], randomValue(random, Math.max(0, depth - 1)));
                }
                elements.add(element);
            }
            return elements;
        }

        /**
         * A mix of paths that genuinely exist in {@code document} (walked out of the structure itself, so they
         * exercise every shape the document actually has) and paths built from the same field-name alphabet that
         * likely do not exist, so the absent-path behaviour is exercised too.
         */
        static List<String> candidatePaths(Random random, Map<String, Object> document) {
            Set<String> paths = new LinkedHashSet<>();
            collectPaths(document, "", paths);
            List<String> existing = new ArrayList<>(paths);
            List<String> result = new ArrayList<>();
            int existingCount = Math.min(existing.size(), 3 + random.nextInt(4));
            for (int i = 0; i < existingCount; i++) {
                result.add(existing.get(random.nextInt(existing.size())));
            }
            int fabricatedCount = 1 + random.nextInt(3);
            for (int i = 0; i < fabricatedCount; i++) {
                int segments = 1 + random.nextInt(3);
                StringBuilder path = new StringBuilder();
                for (int s = 0; s < segments; s++) {
                    if (s > 0) {
                        path.append('.');
                    }
                    path.append(FIELD_NAME_POOL[random.nextInt(FIELD_NAME_POOL.length)]);
                }
                result.add(path.toString());
            }
            if (result.isEmpty()) {
                result.add("a");
            }
            return result;
        }

        private static void collectPaths(Object value, String prefix, Set<String> paths) {
            if (value instanceof Map<?, ?> map) {
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String path = prefix.isEmpty() ? entry.getKey().toString() : prefix + "." + entry.getKey();
                    paths.add(path);
                    collectPaths(entry.getValue(), path, paths);
                }
            } else if (value instanceof List<?> list) {
                for (Object element : list) {
                    // A path through an array does not name an index, it names the field an element holds, the
                    // same "any element" rule the reader applies, so collect paths from an element directly under
                    // the array's own prefix rather than adding an index segment.
                    collectPaths(element, prefix, paths);
                }
            }
        }
    }

    private static String toJson(Map<String, Object> document) {
        try {
            return MAPPER.writeValueAsString(document);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CloudEvent eventWithMap(Map<String, Object> data) {
        PojoCloudEventData<Map<String, Object>> wrapped = PojoCloudEventData.wrap(data, JacksonDataFieldReaderReadAllTest::toJsonBytes);
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withDataContentType("application/json")
                .withData(wrapped)
                .build();
    }

    private static byte[] toJsonBytes(Map<String, Object> map) {
        try {
            return MAPPER.writeValueAsBytes(map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CloudEvent eventWithJson(String json) {
        return eventWithBytes(json.getBytes(StandardCharsets.UTF_8), "application/json");
    }

    private static CloudEvent eventWithBytes(byte[] data, String contentType) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("Test")
                .withDataContentType(contentType)
                .withData(data)
                .build();
    }
}
