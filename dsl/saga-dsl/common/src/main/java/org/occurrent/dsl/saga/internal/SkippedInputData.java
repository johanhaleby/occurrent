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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The data of a skipped input that a saga state store holds as its bytes, because it can't hold the data as it was
 * delivered, and the class of that data. Every store rebuilds such data here, so a release hands the converter the same
 * data from any of them. A {@code JsonCloudEventData} comes back as one with the same JSON value, and data of any other
 * class as a {@link BytesCloudEventData} with the same bytes.
 */
public final class SkippedInputData {

    // Named, so a store whose skipped inputs hold no JsonCloudEventData never loads cloudevents-json-jackson, an optional
    // dependency of this module
    static final String JSON_CLOUD_EVENT_DATA = "io.cloudevents.jackson.JsonCloudEventData";

    private SkippedInputData() {
    }

    /**
     * The name a store holds next to the bytes of {@code data}, which {@link #fromBytes(String, byte[])} rebuilds the
     * data from.
     */
    public static String classOf(CloudEventData data) {
        return requireNonNull(data, "data cannot be null").getClass().getName();
    }

    /**
     * The data held as {@code bytes} for data of the class {@code dataClass}. A {@code JsonCloudEventData} is read from
     * its bytes the way the CloudEvents JSON format with its default options reads the data of a JSON event, so a
     * decimal number is a {@code DoubleNode}. When a {@code double} can't hold one of its decimal numbers, such as
     * {@code 1234567890.123456789}, every decimal number in it is read as a {@code DecimalNode} instead, so no number
     * comes back as another value. The format the application registered isn't used, so an option such as
     * {@code withForceJsonDataToBase64()} doesn't turn the data into bytes. A node built another way comes back as the
     * node its JSON reads as, such as a {@code DecimalNode} of {@code 1.10} as a {@code DoubleNode} of {@code 1.1}, and
     * {@link #difference(CloudEventData, CloudEventData)} tells when that isn't the delivered value. Data of any other
     * class comes back as a {@link BytesCloudEventData}.
     *
     * @throws IllegalStateException when {@code dataClass} names a {@code JsonCloudEventData} and
     *                               {@code io.cloudevents:cloudevents-json-jackson} isn't on the classpath, or the bytes
     *                               can't be read as JSON
     */
    public static CloudEventData fromBytes(String dataClass, byte[] bytes) {
        requireNonNull(dataClass, "dataClass cannot be null");
        requireNonNull(bytes, "bytes cannot be null");
        if (!JSON_CLOUD_EVENT_DATA.equals(dataClass)) {
            return BytesCloudEventData.wrap(bytes);
        }
        try {
            return Json.read(bytes);
        } catch (LinkageError e) {
            // A NoClassDefFoundError when cloudevents-json-jackson or Jackson isn't there
            throw new IllegalStateException("A skipped input holds data that was delivered as a " + JSON_CLOUD_EVENT_DATA + ", and no CloudEvents JSON format is on the classpath to read it back. "
                                            + "Add io.cloudevents:cloudevents-json-jackson.", e);
        }
    }

    /**
     * How {@code held}, the data a store holds or rebuilt for {@code delivered}, differs from the data a release
     * promises to hand the converter, or {@code null} when it doesn't. A release hands back data of the class that was
     * delivered, or a {@link BytesCloudEventData} for data that isn't a {@code JsonCloudEventData}. A
     * {@code JsonCloudEventData} comes back with the same JSON value at every depth. A number equals a number of the
     * same value of any node class, so {@code 1.10} equals {@code 1.1}, and any other value equals only an equal value
     * of the same node class, so a {@code BinaryNode} or a {@code POJONode} differs from the text or the object it reads
     * back as.
     */
    public static @Nullable String difference(CloudEventData delivered, CloudEventData held) {
        requireNonNull(delivered, "delivered cannot be null");
        requireNonNull(held, "held cannot be null");
        boolean releasedAsPromised = held.getClass() == delivered.getClass()
                                     || held.getClass() == BytesCloudEventData.class && !JSON_CLOUD_EVENT_DATA.equals(classOf(delivered));
        if (!releasedAsPromised) {
            return "Its data, a " + classOf(delivered) + ", comes back as a " + classOf(held);
        }
        // Held is a JsonCloudEventData too, and Jackson is loaded since delivered is one
        return JSON_CLOUD_EVENT_DATA.equals(classOf(delivered)) ? Json.difference(delivered, held) : null;
    }

    /**
     * The path to the value at {@code position} of the {@code kind} it is in, such as an entry of a map or an element of
     * a list, inside the value at {@code path}, or at the top of the data when {@code path} is empty. A position counts
     * from 0 in the order the map, the list or the JSON object gives its values. A path names positions and never a key,
     * so a message built from it quotes no part of the event. The event can hold personal data, and erasing it from the
     * event store doesn't erase a log line.
     */
    public static String pathTo(String path, String kind, int position) {
        String step = kind + " " + position;
        return path.isEmpty() ? step : path + " > " + step;
    }

    // Jackson is used here alone, so it's loaded on the first JsonCloudEventData
    private static final class Json {
        // The mapper the CloudEvents JSON format reads an event with, and the same mapper reading decimals as BigDecimal
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final ObjectReader AS_THE_FORMAT_READS = MAPPER.reader();
        private static final ObjectReader EXACT = MAPPER.reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

        static CloudEventData read(byte[] bytes) {
            try {
                JsonNode read = AS_THE_FORMAT_READS.readTree(bytes);
                JsonNode exact = EXACT.readTree(bytes);
                return JsonCloudEventData.wrap(difference("", exact, read) == null ? read : exact);
            } catch (IOException e) {
                // Named by its class and where it failed alone, since Jackson's message quotes the token it couldn't read
                String where = e instanceof JsonProcessingException processing && processing.getLocation() != null
                        ? " at byte " + processing.getLocation().getByteOffset() : "";
                throw new IllegalStateException("A skipped input's JSON data can't be read back from its bytes, since reading them throws " + e.getClass().getName() + where + ".");
            }
        }

        static @Nullable String difference(CloudEventData delivered, CloudEventData held) {
            return difference("", ((JsonCloudEventData) delivered).getNode(), ((JsonCloudEventData) held).getNode());
        }

        // Where held differs from delivered at any depth, and how, or null when every value is the same
        private static @Nullable String difference(String path, JsonNode delivered, JsonNode held) {
            if (delivered.isObject() && held.isObject()) {
                if (!names(delivered).equals(names(held))) {
                    return at(path) + " comes back with " + held.size() + " fields where it was delivered with " + delivered.size();
                }
                int position = 0;
                for (Map.Entry<String, JsonNode> field : delivered.properties()) {
                    String difference = difference(pathTo(path, "field", position++), field.getValue(), held.get(field.getKey()));
                    if (difference != null) {
                        return difference;
                    }
                }
                return null;
            }
            if (delivered.isArray() && held.isArray()) {
                if (delivered.size() != held.size()) {
                    return at(path) + " comes back with " + held.size() + " elements where it was delivered with " + delivered.size();
                }
                for (int i = 0; i < delivered.size(); i++) {
                    String difference = difference(pathTo(path, "element", i), delivered.get(i), held.get(i));
                    if (difference != null) {
                        return difference;
                    }
                }
                return null;
            }
            if (delivered.isNumber() && held.isNumber()) {
                return sameNumber(delivered, held) ? null : at(path) + ", a " + delivered.getClass().getName() + ", comes back as a " + held.getClass().getName() + " of another value";
            }
            if (delivered.getClass() != held.getClass()) {
                return at(path) + ", a " + delivered.getClass().getName() + ", comes back as a " + held.getClass().getName();
            }
            return delivered.equals(held) ? null : at(path) + ", a " + delivered.getClass().getName() + ", comes back as another value";
        }

        private static boolean sameNumber(JsonNode delivered, JsonNode held) {
            BigDecimal value = valueOf(delivered);
            BigDecimal heldValue = valueOf(held);
            if (value == null || heldValue == null) {
                return value == heldValue && Double.compare(delivered.doubleValue(), held.doubleValue()) == 0;
            }
            return value.compareTo(heldValue) == 0;
        }

        // The value of a number as the JSON it writes, so a float of 0.1 is 0.1, or null for a double or a float that
        // isn't finite
        private static @Nullable BigDecimal valueOf(JsonNode number) {
            if (number.isBigDecimal() || number.isIntegralNumber()) {
                return number.decimalValue();
            }
            return Double.isFinite(number.doubleValue()) ? new BigDecimal(number.asText()) : null;
        }

        private static Set<String> names(JsonNode object) {
            Set<String> names = new LinkedHashSet<>();
            object.properties().forEach(field -> names.add(field.getKey()));
            return names;
        }

        private static String at(String path) {
            return path.isEmpty() ? "Its JSON data" : "The value at '" + path + "' of its JSON data";
        }
    }
}
