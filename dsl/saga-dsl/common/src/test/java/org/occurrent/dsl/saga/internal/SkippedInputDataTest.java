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

package org.occurrent.dsl.saga.internal;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.util.RawValue;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.internal.SkippedInputData.JSON_CLOUD_EVENT_DATA;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SkippedInputDataTest {

    private static final byte[] BYTES = "{\"a\":1}".getBytes(UTF_8);

    private static final class CustomData implements CloudEventData {
        @Override
        public byte[] toBytes() {
            return BYTES.clone();
        }
    }

    @Test
    void JsonCloudEventData_held_as_BytesCloudEventData_differs_and_the_reason_names_both_classes() {
        CloudEventData delivered = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.difference(delivered, BytesCloudEventData.wrap(BYTES)))
                .contains(JsonCloudEventData.class.getName())
                .contains(BytesCloudEventData.class.getName());
    }

    @Test
    void JsonCloudEventData_held_as_JsonCloudEventData_does_not_differ() {
        CloudEventData delivered = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.difference(delivered, JsonCloudEventData.wrap(new ObjectMapper().createObjectNode()))).isNull();
    }

    @Test
    void BytesCloudEventData_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.difference(BytesCloudEventData.wrap(BYTES), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.difference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void data_of_a_custom_class_held_as_BytesCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.difference(new CustomData(), BytesCloudEventData.wrap(BYTES))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_PojoCloudEventData_does_not_differ() {
        assertThat(SkippedInputData.difference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), PojoCloudEventData.wrap("x", s -> BYTES.clone()))).isNull();
    }

    @Test
    void PojoCloudEventData_held_as_JsonCloudEventData_differs() {
        CloudEventData held = JsonCloudEventData.wrap(new ObjectMapper().createObjectNode());

        assertThat(SkippedInputData.difference(PojoCloudEventData.wrap("x", s -> BYTES.clone()), held)).isNotNull();
    }

    record Kind(String name, JsonNode value, Class<? extends JsonNode> releasedClass, JsonNode released, boolean accepted) {
        @Override
        public String toString() {
            return name;
        }
    }

    record Address(String city) {
    }

    static Stream<Kind> kinds() {
        return Stream.of(
                new Kind("an integer past long", BigIntegerNode.valueOf(BigInteger.TWO.pow(70)), BigIntegerNode.class, BigIntegerNode.valueOf(BigInteger.TWO.pow(70)), true),
                new Kind("a decimal past double", DecimalNode.valueOf(new BigDecimal("1234567890.123456789")), DecimalNode.class, DecimalNode.valueOf(new BigDecimal("1234567890.123456789")), true),
                new Kind("1E+400 as a decimal", DecimalNode.valueOf(new BigDecimal("1E+400")), DecimalNode.class, DecimalNode.valueOf(new BigDecimal("1E+400")), true),
                new Kind("a negative zero double", DoubleNode.valueOf(-0.0), DoubleNode.class, DoubleNode.valueOf(-0.0), true),
                new Kind("a float", FloatNode.valueOf(0.1f), DoubleNode.class, DoubleNode.valueOf(0.1), true),
                new Kind("a long that fits an int", LongNode.valueOf(5L), IntNode.class, IntNode.valueOf(5), true),
                new Kind("a double", DoubleNode.valueOf(1.5), DoubleNode.class, DoubleNode.valueOf(1.5), true),
                new Kind("a double that is not a number", DoubleNode.valueOf(Double.NaN), TextNode.class, TextNode.valueOf("NaN"), false),
                new Kind("the text NaN", TextNode.valueOf("NaN"), TextNode.class, TextNode.valueOf("NaN"), true),
                new Kind("a 100 000 character string", TextNode.valueOf("x".repeat(100_000)), TextNode.class, TextNode.valueOf("x".repeat(100_000)), true),
                new Kind("nesting 100 levels deep", nested(100), ObjectNode.class, nested(100), true),
                new Kind("binary", BinaryNode.valueOf(new byte[]{1, 2, 3}), TextNode.class, TextNode.valueOf("AQID"), false),
                new Kind("a POJO of a record", new POJONode(new Address("Malmo")), ObjectNode.class, new ObjectMapper().createObjectNode().put("city", "Malmo"), false)
        );
    }

    @TestFactory
    Stream<DynamicTest> a_JsonCloudEventData_holding_each_kind_of_value_is_rebuilt_as_the_delivered_value_or_refused() {
        return kinds().map(kind -> DynamicTest.dynamicTest(kind.name(), () -> assertRebuilt(kind)));
    }

    private static void assertRebuilt(Kind kind) {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.set("v", kind.value());
        CloudEventData delivered = JsonCloudEventData.wrap(node);

        CloudEventData held = SkippedInputData.fromBytes(SkippedInputData.classOf(delivered), delivered.toBytes());

        JsonNode released = ((JsonCloudEventData) held).getNode().get("v");
        assertAll(
                () -> assertThat(held).isExactlyInstanceOf(JsonCloudEventData.class),
                () -> assertThat(released).as("released value class").isExactlyInstanceOf(kind.releasedClass()),
                () -> assertThat(released).as("released value").isEqualTo(kind.released()),
                () -> assertThat(SkippedInputData.difference(delivered, held) == null).as("accepted").isEqualTo(kind.accepted())
        );
    }

    @Test
    void a_JsonCloudEventData_whose_root_is_null_is_rebuilt_as_the_delivered_NullNode() {
        CloudEventData delivered = JsonCloudEventData.wrap(NullNode.getInstance());

        CloudEventData held = SkippedInputData.fromBytes(SkippedInputData.classOf(delivered), delivered.toBytes());

        assertAll(
                () -> assertThat(((JsonCloudEventData) held).getNode()).isSameAs(NullNode.getInstance()),
                () -> assertThat(SkippedInputData.difference(delivered, held)).isNull()
        );
    }

    @Test
    void a_RawValue_is_refused() {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.putRawValue("v", new RawValue("1.10"));
        assertThat(node.get("v")).as("a raw value is held by a POJONode").isInstanceOf(POJONode.class);
        CloudEventData delivered = JsonCloudEventData.wrap(node);

        CloudEventData held = SkippedInputData.fromBytes(SkippedInputData.classOf(delivered), delivered.toBytes());

        assertThat(SkippedInputData.difference(delivered, held)).isNotNull();
    }

    @Test
    void nesting_past_the_depth_Jackson_reads_cannot_be_rebuilt() {
        int depth = StreamReadConstraints.DEFAULT_MAX_DEPTH + 10;
        byte[] bytes = ("[".repeat(depth) + "]".repeat(depth)).getBytes(UTF_8);

        assertThatThrownBy(() -> SkippedInputData.fromBytes(JSON_CLOUD_EVENT_DATA, bytes)).isInstanceOf(IllegalStateException.class);
    }

    // Duplicate keys can't be held by an ObjectNode, so there is no delivered JsonCloudEventData to rebuild from

    private static ObjectNode nested(int levels) {
        ObjectNode root = new ObjectMapper().createObjectNode();
        ObjectNode current = root;
        for (int i = 1; i < levels; i++) {
            current = current.putObject("n");
        }
        current.put("leaf", 1);
        return root;
    }
}
