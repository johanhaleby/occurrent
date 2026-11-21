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

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;

import java.net.URI;

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
    private static final String CLOUD_EVENT_JSON_FORMAT = "application/cloudevents+json";
    private static final URI CARRIER_SOURCE = URI.create("urn:occurrent:skipped-input-data");

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
     * its bytes by the CloudEvents JSON format on the classpath, which is how that format reads the data of a JSON event,
     * so its JSON node is the one the format read on delivery. A node built another way comes back as the node its JSON
     * reads as, such as a {@code DecimalNode} of {@code 1.10} as a {@code DoubleNode} of {@code 1.1}. Data of any other
     * class comes back as a {@link BytesCloudEventData}.
     *
     * @throws IllegalStateException when {@code dataClass} names a {@code JsonCloudEventData} and no CloudEvents JSON
     *                               format is on the classpath
     */
    public static CloudEventData fromBytes(String dataClass, byte[] bytes) {
        requireNonNull(dataClass, "dataClass cannot be null");
        requireNonNull(bytes, "bytes cannot be null");
        if (!JSON_CLOUD_EVENT_DATA.equals(dataClass)) {
            return BytesCloudEventData.wrap(bytes);
        }
        EventFormat json = EventFormatProvider.getInstance().resolveFormat(CLOUD_EVENT_JSON_FORMAT);
        if (json == null) {
            throw new IllegalStateException("A skipped input holds data that was delivered as a " + JSON_CLOUD_EVENT_DATA + ", and no CloudEvents JSON format is on the classpath to read it back. "
                                            + "Add io.cloudevents:cloudevents-json-jackson.");
        }
        // The format writes bytes of a JSON content type as they are, and reads the data of a JSON content type as a
        // JsonCloudEventData
        CloudEvent carrier = CloudEventBuilder.v1().withId("data").withSource(CARRIER_SOURCE).withType("data")
                .withDataContentType("application/json").withData(bytes).build();
        return requireNonNull(json.deserialize(json.serialize(carrier)).getData(), "data");
    }
}
