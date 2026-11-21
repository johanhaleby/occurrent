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

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The record of a skipped input holds the event as it was delivered. Changing the delivered event, or an event a read
 * returned, must leave the record alone, and an event the store cannot copy is refused with nothing recorded.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreSkippedEventCopyTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String KEY = "a@1";

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaCodec<String> codec = SagaCodec.of(store.stateCodec(), null);

    @Test
    void bytes_data_is_read_as_delivered_when_the_delivered_array_is_changed_after_the_skip() {
        quarantinedInstance();
        byte[] delivered = {1, 2, 3};
        skip(event(BytesCloudEventData.wrap(delivered)));

        delivered[0] = 99;

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).containsExactly(1, 2, 3)
        );
    }

    @Test
    void map_data_is_read_as_delivered_when_the_delivered_map_and_its_nested_values_are_changed_after_the_skip() {
        quarantinedInstance();
        List<Object> tags = new ArrayList<>(List.of("x", "y"));
        Map<String, Object> address = new LinkedHashMap<>(Map.of("city", "Malmo"));
        byte[] raw = {7, 8};
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("name", "book");
        delivered.put("address", address);
        delivered.put("tags", tags);
        delivered.put("raw", raw);
        delivered.put("none", null);
        skip(event(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render)));
        String renderedAsDelivered = new String(render(delivered), StandardCharsets.UTF_8);

        delivered.put("name", "changed");
        delivered.put("added", 1);
        delivered.remove("none");
        address.put("city", "Lund");
        address.put("street", "x");
        tags.add("z");
        tags.set(0, "changed");
        raw[0] = 99;

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isInstanceOf(PojoCloudEventData.class),
                () -> assertThat(valueOf(read)).hasSize(5)
                        .containsEntry("name", "book")
                        .containsEntry("address", Map.of("city", "Malmo"))
                        .containsEntry("tags", List.of("x", "y"))
                        .containsEntry("raw", new byte[]{7, 8})
                        .containsEntry("none", null),
                () -> assertThat(new String(read.toBytes(), StandardCharsets.UTF_8)).isEqualTo(renderedAsDelivered)
        );
    }

    @Test
    void list_data_is_read_as_delivered_when_the_delivered_list_and_its_nested_list_are_changed_after_the_skip() {
        quarantinedInstance();
        List<Object> nested = new ArrayList<>(List.of(1, 2));
        List<Object> delivered = new ArrayList<>(List.of("a", nested));
        skip(event(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render)));

        delivered.add("b");
        nested.add(3);

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(((PojoCloudEventData<?>) read).getValue()).isEqualTo(List.of("a", List.of(1, 2))),
                () -> assertThat(new String(read.toBytes(), StandardCharsets.UTF_8)).isEqualTo("[a, [1, 2]]")
        );
    }

    @Test
    void changing_the_bytes_of_a_returned_event_leaves_the_next_read_as_delivered() {
        quarantinedInstance();
        skip(event(BytesCloudEventData.wrap(new byte[]{1, 2, 3})));

        recordedEvent().getData().toBytes()[0] = 99;

        assertThat(recordedEvent().getData().toBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void changing_the_map_and_nested_values_of_a_returned_event_leaves_the_next_read_as_delivered() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("tags", new ArrayList<>(List.of("x")));
        delivered.put("raw", new byte[]{7, 8});
        skip(event(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render)));
        Map<String, Object> returned = valueOf(recordedEvent().getData());

        ((List<Object>) returned.get("tags")).add("changed");
        ((byte[]) returned.get("raw"))[0] = 99;
        returned.put("added", 1);

        Map<String, Object> next = valueOf(recordedEvent().getData());
        assertAll(
                () -> assertThat(next).hasSize(2).containsEntry("tags", List.of("x")).containsEntry("raw", new byte[]{7, 8}),
                () -> assertThat(new String(recordedEvent().getData().toBytes(), StandardCharsets.UTF_8)).isEqualTo("tags=[x];raw=[7, 8];")
        );
    }

    @Test
    void a_mutable_object_in_the_data_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("counter", new AtomicInteger(1));
        CloudEventData data = PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render);
        byte[] asDelivered = data.toBytes();

        skip(event(data));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).isEqualTo(asDelivered)
        );
    }

    @Test
    void a_mutable_object_nested_in_a_list_in_the_data_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("parts", List.of(new StringBuilder("x")));
        CloudEventData data = PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render);

        skip(event(data));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).isEqualTo(data.toBytes())
        );
    }

    @Test
    void a_map_key_that_is_not_a_scalar_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<Object, Object> delivered = new LinkedHashMap<>();
        delivered.put(new StringBuilder("k"), "v");
        CloudEventData data = PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render);

        skip(event(data));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).isEqualTo(data.toBytes())
        );
    }

    @Test
    void a_pojo_whose_value_is_not_a_map_or_a_list_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        CloudEventData data = PojoCloudEventData.wrap(new StringBuilder("x"), value -> value.toString().getBytes(StandardCharsets.UTF_8));

        skip(event(data));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).isEqualTo("x".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void data_of_another_implementation_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        CloudEventData other = () -> new byte[]{1};

        skip(event(other));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).containsExactly(1)
        );
    }

    @Test
    void a_date_in_the_data_is_read_as_a_date_of_the_same_value_when_the_delivered_date_is_changed_after_the_skip() {
        quarantinedInstance();
        Date delivered = new Date(1_000L);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("at", delivered);
        skip(event(PojoCloudEventData.wrap(data, InMemorySagaStateStoreSkippedEventCopyTest::render)));

        delivered.setTime(2_000L);

        Object read = valueOf(recordedEvent().getData()).get("at");
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(Date.class),
                () -> assertThat(read).isEqualTo(new Date(1_000L))
        );
    }

    @Test
    void a_date_in_the_data_is_read_as_recorded_when_a_date_a_read_returned_is_changed() {
        quarantinedInstance();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("at", new Date(1_000L));
        data.put("all", List.of(new Date(3_000L)));
        skip(event(PojoCloudEventData.wrap(data, InMemorySagaStateStoreSkippedEventCopyTest::render)));
        Map<String, Object> firstRead = valueOf(recordedEvent().getData());
        ((Date) firstRead.get("at")).setTime(2_000L);
        ((Date) ((List<?>) firstRead.get("all")).getFirst()).setTime(4_000L);

        Map<String, Object> secondRead = valueOf(recordedEvent().getData());

        assertAll(
                () -> assertThat(secondRead.get("at")).isEqualTo(new Date(1_000L)),
                () -> assertThat(secondRead.get("all")).isEqualTo(List.of(new Date(3_000L)))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("immutableLeaves")
    void an_immutable_value_in_the_data_is_read_as_the_same_class_with_an_equal_value(String name, Object leaf) {
        quarantinedInstance();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("leaf", leaf);
        data.put("nested", List.of(leaf));
        skip(event(PojoCloudEventData.wrap(data, InMemorySagaStateStoreSkippedEventCopyTest::render)));

        Map<String, Object> read = valueOf(recordedEvent().getData());

        assertAll(
                () -> assertThat(read.get("leaf")).isExactlyInstanceOf(leaf.getClass()).isEqualTo(leaf),
                () -> assertThat(read.get("nested")).isEqualTo(List.of(leaf))
        );
    }

    static Stream<Arguments> immutableLeaves() {
        return Stream.of(
                Arguments.of("UUID", UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                Arguments.of("ObjectId", new ObjectId("507f1f77bcf86cd799439011")),
                Arguments.of("Decimal128", Decimal128.parse("1.25")),
                Arguments.of("Binary", new Binary(new byte[]{1, 2})),
                Arguments.of("Code", new Code("x = 1")),
                Arguments.of("Symbol", new Symbol("sym")),
                Arguments.of("MinKey", new MinKey()),
                Arguments.of("MaxKey", new MaxKey()),
                Arguments.of("BsonTimestamp", new BsonTimestamp(5, 6)),
                Arguments.of("BsonRegularExpression", new BsonRegularExpression("a.*", "i")),
                Arguments.of("BsonDbPointer", new BsonDbPointer("ns", new ObjectId("507f1f77bcf86cd799439011"))),
                Arguments.of("BsonUndefined", new BsonUndefined())
        );
    }

    @Test
    void a_code_with_scope_in_the_data_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("code", new CodeWithScope("x", new Document("a", 1)));

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void a_subclass_of_date_in_the_data_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("at", new Timestamp(1_000L));

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void bytes_data_of_a_subclass_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        assertHeldAsBytesAndRead(new SubclassedBytesData(new byte[]{1, 2, 3}));
    }

    @Test
    void a_subclass_of_a_number_in_the_data_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("amount", new SubclassedBigDecimal("1.5"));

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void a_map_key_that_is_a_subclass_of_a_number_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<Object, Object> delivered = new LinkedHashMap<>();
        delivered.put(new SubclassedBigDecimal("1.5"), "v");

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void an_extension_that_is_a_subclass_of_a_number_is_refused_and_nothing_is_recorded() {
        quarantinedInstance();
        CloudEvent delivered = CloudEventBuilder.v1().withId("e1").withSource(URI.create("urn:test")).withType("t")
                .withExtension("amount", new SubclassedBigDecimal("1.5")).build();

        assertThatThrownBy(() -> skip(delivered))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(KEY)
                .hasMessageContaining("amount")
                .hasMessageContaining(SubclassedBigDecimal.class.getName());
        assertNothingWasRecorded();
    }

    @Test
    void a_map_holding_two_equal_keys_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(identityMapWithTwoEqualKeys(), InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void a_map_holding_two_equal_keys_nested_in_a_map_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("inner", identityMapWithTwoEqualKeys());

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void a_map_holding_two_equal_keys_nested_in_a_list_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<String, Object> delivered = new LinkedHashMap<>();
        delivered.put("parts", List.of(identityMapWithTwoEqualKeys()));

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    @Test
    void a_comparator_ordered_map_holding_two_equal_keys_is_held_as_the_bytes_of_the_data_and_read_as_bytes() {
        quarantinedInstance();
        Map<Object, Object> delivered = new TreeMap<>((a, b) -> -1);
        delivered.put("item", "first");
        delivered.put("item", "second");
        assertThat(delivered).hasSize(2);

        assertHeldAsBytesAndRead(PojoCloudEventData.wrap(delivered, InMemorySagaStateStoreSkippedEventCopyTest::render));
    }

    private void assertHeldAsBytesAndRead(CloudEventData delivered) {
        byte[] asDelivered = delivered.toBytes();

        skip(event(delivered));

        CloudEventData read = recordedEvent().getData();
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).isEqualTo(asDelivered)
        );
    }

    private static Map<Object, Object> identityMapWithTwoEqualKeys() {
        Map<Object, Object> map = new IdentityHashMap<>();
        map.put(new String("item"), "first");
        map.put(new String("item"), "second");
        assertThat(map).hasSize(2);
        return map;
    }

    private static final class SubclassedBytesData extends BytesCloudEventData {
        SubclassedBytesData(byte[] value) {
            super(value);
        }
    }

    private static final class SubclassedBigDecimal extends java.math.BigDecimal {
        SubclassedBigDecimal(String value) {
            super(value);
        }
    }

    private void assertNothingWasRecorded() {
        SagaEnvelope<String> stored = codec.decode(store.find("s1").orElseThrow());
        assertAll(
                () -> assertThat(stored.version()).as("the instance's version").isEqualTo(1L),
                () -> assertThat(stored.status()).as("the instance's status").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(stored.quarantineSkipCount()).as("the instance's skip count").isEqualTo(0L),
                () -> assertThat(store.findSkippedInputs("s1", "q1", 10)).as("the records").isEmpty()
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> valueOf(CloudEventData data) {
        return (Map<String, Object>) ((PojoCloudEventData<?>) data).getValue();
    }

    // The bytes of what the value holds when they are asked for, so a changed value shows in the bytes too
    private static byte[] render(Object value) {
        return (value instanceof Map<?, ?> map ? renderMap(map) : String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
    }

    private static String renderMap(Map<?, ?> map) {
        StringBuilder rendered = new StringBuilder();
        map.forEach((k, v) -> rendered.append(k).append('=').append(v instanceof byte[] bytes ? Arrays.toString(bytes) : v).append(';'));
        return rendered.toString();
    }

    private CloudEvent recordedEvent() {
        List<SkippedInput> records = store.findSkippedInputs("s1", "q1", 10);
        assertThat(records).hasSize(1);
        return records.getFirst().event();
    }

    private static CloudEvent event(CloudEventData data) {
        return CloudEventBuilder.v1().withId(KEY).withSource(URI.create("urn:test")).withType("test").withData(data).build();
    }

    private void skip(CloudEvent event) {
        StoredSaga current = store.find("s1").orElseThrow();
        SagaEnvelope<String> next = atVersion(codec.decode(current), current.version() + 1);
        store.compareAndSaveSkipped(codec.encodeWithoutState(next), current.version(), "q1", new SkippedInput(0, KEY, event));
    }

    private void quarantinedInstance() {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", NOW.plusSeconds(1), "q1");
        SagaEnvelope<String> quarantined = new SagaEnvelope<>("s1", "a", SagaStatus.QUARANTINED, 1, List.of(), Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), 0L);
        store.compareAndSave(codec.encode(quarantined), 0);
    }

    private static SagaEnvelope<String> atVersion(SagaEnvelope<String> e, long version) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), version, e.timers(), e.streamWatermarks(), e.positionWatermark(), e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }
}
