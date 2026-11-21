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

import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.format.EventFormat;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * The data of a skipped input that a saga state store holds as its bytes, because it can't hold the data as it was
 * delivered, and the class of that data. Every store rebuilds such data here, so a release hands the converter the same
 * data from any of them. A {@code JsonCloudEventData} comes back as one, and data of any other class as a
 * {@link BytesCloudEventData} with the same bytes.
 */
public final class SkippedInputData {

    // Named, since this module doesn't depend on cloudevents-json-jackson
    static final String JSON_CLOUD_EVENT_DATA = "io.cloudevents.jackson.JsonCloudEventData";
    private static final String JSON_FORMAT = "io.cloudevents.jackson.JsonFormat";
    // An event of JSON content type with the bytes as its data, written here rather than by a format, so no option of a
    // registered format decides whether the data is written as JSON or as base64
    private static final byte[] CARRIER_START = ("{\"specversion\":\"1.0\",\"id\":\"data\",\"source\":\"urn:occurrent:skipped-input-data\","
                                                 + "\"type\":\"data\",\"datacontenttype\":\"application/json\",\"data\":").getBytes(StandardCharsets.UTF_8);

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
     * its bytes by a CloudEvents JSON format with its default options, the way that format reads the data of a JSON
     * event, so its JSON node is the one the format read on delivery. The format the application registered isn't used,
     * so an option such as {@code withForceJsonDataToBase64()} doesn't turn the data into bytes. A node built another
     * way comes back as the node its JSON reads as, such as a {@code DecimalNode} of {@code 1.10} as a
     * {@code DoubleNode} of {@code 1.1}. Data of any other class comes back as a {@link BytesCloudEventData}.
     *
     * @throws IllegalStateException when {@code dataClass} names a {@code JsonCloudEventData} and
     *                               {@code io.cloudevents:cloudevents-json-jackson} isn't on the classpath
     */
    public static CloudEventData fromBytes(String dataClass, byte[] bytes) {
        requireNonNull(dataClass, "dataClass cannot be null");
        requireNonNull(bytes, "bytes cannot be null");
        if (!JSON_CLOUD_EVENT_DATA.equals(dataClass)) {
            return BytesCloudEventData.wrap(bytes);
        }
        EventFormat json = DefaultJsonFormat.FORMAT;
        if (json == null) {
            throw new IllegalStateException("A skipped input holds data that was delivered as a " + JSON_CLOUD_EVENT_DATA + ", and no CloudEvents JSON format is on the classpath to read it back. "
                                            + "Add io.cloudevents:cloudevents-json-jackson.");
        }
        ByteArrayOutputStream carrier = new ByteArrayOutputStream(CARRIER_START.length + bytes.length + 1);
        carrier.writeBytes(CARRIER_START);
        carrier.writeBytes(bytes);
        carrier.write('}');
        // The format reads the data of a JSON content type as a JsonCloudEventData
        return requireNonNull(json.deserialize(carrier.toByteArray()).getData(), "data");
    }

    /**
     * How the class of {@code held}, the data a store holds or rebuilt for {@code delivered}, differs from the class a
     * release promises to hand the converter, or {@code null} when it doesn't. A release hands back data of the class
     * that was delivered, or a {@link BytesCloudEventData} for data that isn't a {@code JsonCloudEventData}.
     */
    public static @Nullable String classDifference(CloudEventData delivered, CloudEventData held) {
        requireNonNull(delivered, "delivered cannot be null");
        requireNonNull(held, "held cannot be null");
        boolean releasedAsPromised = held.getClass() == delivered.getClass()
                                     || held.getClass() == BytesCloudEventData.class && !JSON_CLOUD_EVENT_DATA.equals(classOf(delivered));
        return releasedAsPromised ? null : "Its data, a " + classOf(delivered) + ", comes back as a " + classOf(held);
    }

    // Built on first use, so a store whose skipped inputs hold no JsonCloudEventData never loads it
    private static final class DefaultJsonFormat {
        private static final @Nullable EventFormat FORMAT = load();

        private static @Nullable EventFormat load() {
            try {
                return (EventFormat) Class.forName(JSON_FORMAT, true, SkippedInputData.class.getClassLoader()).getConstructor().newInstance();
            } catch (ClassNotFoundException | LinkageError e) {
                // LinkageError when the format is there and Jackson isn't
                return null;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Cannot create " + JSON_FORMAT + " to read back a skipped input's JSON data", e);
            }
        }
    }
}
