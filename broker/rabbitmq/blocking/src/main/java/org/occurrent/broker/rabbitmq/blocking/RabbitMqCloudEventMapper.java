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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The one place that maps a {@link CloudEvent} onto AMQP 0-9-1, in both directions {@link RabbitMqCloudEventSink} and
 * a consume-side bridge need. The CloudEvents SDK has no writer for 0-9-1, only for AMQP 1.0, so this mapping is
 * Occurrent's own, modelled on the AMQP 1.0 binding so it is recognisable rather than invented from nothing. A bridge
 * reads a message back through the same mapping the sink writes it with, so the two directions belong on this one
 * class rather than on two that could drift apart from each other.
 * <p>
 * Every CloudEvent attribute, the context attributes and the extensions alike, becomes an entry in
 * {@link BasicProperties#getHeaders()} under its own name prefixed with {@value #HEADER_PREFIX}, written as a
 * string. A binary extension is Base64-encoded rather than written through its raw {@code toString()}.
 * {@code datacontenttype} is the one exception, since it becomes {@link BasicProperties#getContentType()}
 * instead, a dedicated AMQP field rather than a header. The event data becomes the message body unchanged.
 * Every message is published persistent, since a transient one a broker restart discards would otherwise still
 * count as delivered once {@link RabbitMqCloudEventSink} sees its publisher confirm.
 * <p>
 * {@link #toCloudEvent(BasicProperties, byte[])} rebuilds every attribute and extension as a {@link String}, whatever
 * type it started as, since AMQP headers carry no richer typing to recover it from. This is not a new limitation, it
 * is the same one a CloudEvent already has after a JSON round trip, which is why {@code EventMetadata.getStreamVersion()}
 * and {@code EventMetadata.getPosition()} already accept a {@code String} as well as a {@code Number}.
 */
public final class RabbitMqCloudEventMapper {

    /**
     * The header namespace every CloudEvent attribute is written under. Reserved on {@link RabbitMqDestination}, so
     * an application header cannot collide with it and silently overwrite one of these.
     */
    public static final String HEADER_PREFIX = "cloudEvents_";

    private static final String DATA_CONTENT_TYPE_ATTRIBUTE = "datacontenttype";

    /**
     * Fixed by choosing {@link SpecVersion#V1} in {@link #toCloudEvent(BasicProperties, byte[])} rather than read
     * back from the header, since a {@link CloudEventBuilder} takes the spec version at construction and has no
     * method to set it afterwards.
     */
    private static final String SPEC_VERSION_ATTRIBUTE = "specversion";

    /**
     * AMQP's persistent delivery mode. Unset defaults to transient, which a broker restart can discard even after a
     * publisher confirm, so every message this mapper writes asks to survive one instead.
     */
    private static final int PERSISTENT_DELIVERY_MODE = 2;

    private RabbitMqCloudEventMapper() {
    }

    /**
     * The {@link BasicProperties} for {@code cloudEvent}, carrying every one of its attributes as a
     * {@value #HEADER_PREFIX}-prefixed header (string-valued), its content type as {@link BasicProperties#getContentType()},
     * and {@code applicationHeaders} alongside them.
     */
    public static BasicProperties toBasicProperties(CloudEvent cloudEvent, Map<String, String> applicationHeaders) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        requireNonNull(applicationHeaders, "applicationHeaders cannot be null");

        Map<String, Object> headers = new LinkedHashMap<>(applicationHeaders);
        for (String attributeName : cloudEvent.getAttributeNames()) {
            if (DATA_CONTENT_TYPE_ATTRIBUTE.equals(attributeName)) {
                continue;
            }
            Object value = cloudEvent.getAttribute(attributeName);
            if (value != null) {
                headers.put(HEADER_PREFIX + attributeName, value.toString());
            }
        }
        for (String extensionName : cloudEvent.getExtensionNames()) {
            Object value = cloudEvent.getExtension(extensionName);
            if (value != null) {
                headers.put(HEADER_PREFIX + extensionName, encodeExtensionValue(value));
            }
        }

        return new BasicProperties.Builder()
                .contentType(cloudEvent.getDataContentType())
                .headers(headers)
                .deliveryMode(PERSISTENT_DELIVERY_MODE)
                .build();
    }

    /**
     * A binary extension is Base64-encoded rather than written through {@link Object#toString()}, which would
     * otherwise produce {@code byte[]}'s Java identity string instead of the extension's actual bytes.
     */
    private static String encodeExtensionValue(Object value) {
        return value instanceof byte[] bytes ? Base64.getEncoder().encodeToString(bytes) : value.toString();
    }

    /**
     * The message body for {@code cloudEvent}: its data, unchanged, or an empty array for an event with none.
     */
    public static byte[] toBody(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        CloudEventData data = cloudEvent.getData();
        return data == null ? new byte[0] : data.toBytes();
    }

    /**
     * Rebuilds the {@link CloudEvent} {@code properties} and {@code body} were written from by
     * {@link #toBasicProperties(CloudEvent, Map)}/{@link #toBody(CloudEvent)}, the reverse of that mapping. Every
     * {@value #HEADER_PREFIX}-prefixed header becomes a context attribute or an extension, as it originally was, and
     * {@link BasicProperties#getContentType()} becomes {@code datacontenttype}. A header outside that namespace (an
     * application header on the destination the sink or the bridge was built with) is not part of the mapping and is
     * ignored here, exactly as {@link RabbitMqDestination}'s reserved-prefix check assumes.
     * <p>
     * Every rebuilt attribute and extension is a {@link String}, since that is what a header actually carries. A
     * binary extension is not un-Base64-decoded back to {@code byte[]}, for the same reason: nothing on the message
     * says which headers were encoded that way, so a caller reading such an extension gets the Base64 text rather
     * than the original bytes.
     */
    public static CloudEvent toCloudEvent(BasicProperties properties, byte[] body) {
        requireNonNull(properties, "properties cannot be null");
        requireNonNull(body, "body cannot be null");

        CloudEventBuilder builder = CloudEventBuilder.fromSpecVersion(SpecVersion.V1);
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null) {
            for (Map.Entry<String, Object> header : headers.entrySet()) {
                String key = header.getKey();
                Object value = header.getValue();
                if (!key.startsWith(HEADER_PREFIX) || value == null) {
                    continue;
                }
                String attributeName = key.substring(HEADER_PREFIX.length());
                if (SPEC_VERSION_ATTRIBUTE.equals(attributeName)) {
                    continue;
                }
                // toString() rather than a cast: a header value read back off the wire is a
                // com.rabbitmq.client.LongString for a string-valued header, not a java.lang.String.
                builder.withContextAttribute(attributeName, value.toString());
            }
        }
        String contentType = properties.getContentType();
        if (contentType != null) {
            builder.withDataContentType(contentType);
        }
        if (body.length > 0) {
            builder.withData(BytesCloudEventData.wrap(body));
        }
        return builder.build();
    }
}
