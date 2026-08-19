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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
 * {@link #toCloudEvent(BasicProperties, byte[])} rebuilds every attribute and extension as a {@link String},
 * since AMQP headers carry no richer typing to recover it from, except the two extensions
 * {@link OccurrentCloudEventExtension} itself defines a type for, {@code streamversion} and {@code position}, which
 * come back the {@code Long} that type already is. See its own javadoc, and ADR 133's amendment, for the reasoning.
 */
public final class RabbitMqCloudEventMapper {

    /**
     * The header namespace every CloudEvent attribute is written under. Reserved on {@link RabbitMqDestination}, so
     * an application header cannot collide with it and silently overwrite one of these.
     */
    public static final String HEADER_PREFIX = "cloudEvents_";

    private static final String DATA_CONTENT_TYPE_ATTRIBUTE = "datacontenttype";

    /**
     * Read back from the header in {@link #toCloudEvent(BasicProperties, byte[])} to pick the
     * {@link CloudEventBuilder}, since a builder takes its spec version at construction and has no method to set it
     * afterwards. Never passed to {@link CloudEventBuilder#withContextAttribute(String, String)} itself, since the
     * builder already fixes it.
     */
    private static final String SPEC_VERSION_ATTRIBUTE = "specversion";

    /**
     * Marks a message whose {@link CloudEvent#getData()} is present but explicitly empty ({@code byte[0]}), rather
     * than absent ({@code null}). Both encode as the same zero-length AMQP body, since a body has no way to be
     * absent the way {@code data} itself can, so {@link #toCloudEvent(BasicProperties, byte[])} cannot tell the two
     * apart from the body alone and needs this header to recover which one a zero-length body actually was.
     * Contains an underscore, which a real CloudEvent attribute or extension name can never contain, so this can
     * never collide with one and never needs its own reserved-prefix check beyond {@value #HEADER_PREFIX} itself.
     */
    private static final String EMPTY_DATA_ATTRIBUTE = "data_present_empty";

    /**
     * AMQP's persistent delivery mode. Unset defaults to transient, which a broker restart can discard even after a
     * publisher confirm, so every message this mapper writes asks to survive one instead.
     */
    private static final int PERSISTENT_DELIVERY_MODE = 2;

    /**
     * The extensions {@link #toCloudEvent(BasicProperties, byte[])} rebuilds as a {@code Long} rather than leaving
     * a {@link String}, since {@link OccurrentCloudEventExtension} defines both as one. See ADR 133's amendment.
     */
    private static final Set<String> NUMERIC_OCCURRENT_EXTENSIONS = Set.of(
            OccurrentCloudEventExtension.STREAM_VERSION, OccurrentCloudEventExtension.POSITION);

    private RabbitMqCloudEventMapper() {
    }

    /**
     * As {@link #toBasicProperties(CloudEvent, Map)}, for a caller that already computed {@code cloudEvent}'s body
     * through {@link #toBody(CloudEvent)} and wants to reuse those bytes rather than have this method call
     * {@link CloudEventData#toBytes()} a second time. Occurrent's own converters hand back a lazily-serializing
     * {@code PojoCloudEventData}, so calling {@code toBytes()} twice on the same publish serializes the domain
     * event twice. {@link RabbitMqCloudEventSink} and {@link RabbitMqDeliveryFailureAction} both publish through
     * this overload for that reason. {@code body} must be exactly what {@link #toBody(CloudEvent)} would have
     * returned for the same {@code cloudEvent}, since this method trusts it rather than recomputing it.
     */
    public static BasicProperties toBasicProperties(CloudEvent cloudEvent, Map<String, String> applicationHeaders, byte[] body) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        requireNonNull(applicationHeaders, "applicationHeaders cannot be null");
        requireNonNull(body, "body cannot be null");

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
        if (cloudEvent.getData() != null && body.length == 0) {
            headers.put(HEADER_PREFIX + EMPTY_DATA_ATTRIBUTE, "true");
        }

        return new BasicProperties.Builder()
                .contentType(cloudEvent.getDataContentType())
                .headers(headers)
                .deliveryMode(PERSISTENT_DELIVERY_MODE)
                .build();
    }

    /**
     * The {@link BasicProperties} for {@code cloudEvent}, carrying every one of its attributes as a
     * {@value #HEADER_PREFIX}-prefixed header (string-valued), its content type as {@link BasicProperties#getContentType()},
     * and {@code applicationHeaders} alongside them. Also carries the {@value #EMPTY_DATA_ATTRIBUTE} marker header
     * when {@code cloudEvent}'s data is present but empty, so {@link #toCloudEvent(BasicProperties, byte[])} can
     * rebuild that distinctly from data that was never there at all. See that method's own javadoc.
     * <p>
     * Calls {@link #toBody(CloudEvent)} internally to decide the marker header above, so a caller that also needs
     * the body should call {@link #toBasicProperties(CloudEvent, Map, byte[])} instead and pass it in, rather than
     * have {@code cloudEvent}'s data serialized a second time computing it here.
     */
    public static BasicProperties toBasicProperties(CloudEvent cloudEvent, Map<String, String> applicationHeaders) {
        return toBasicProperties(cloudEvent, applicationHeaders, toBody(cloudEvent));
    }

    /**
     * A binary extension is Base64-encoded rather than written through {@link Object#toString()}, which would
     * otherwise produce {@code byte[]}'s Java identity string instead of the extension's actual bytes.
     */
    private static String encodeExtensionValue(Object value) {
        return value instanceof byte[] bytes ? Base64.getEncoder().encodeToString(bytes) : value.toString();
    }

    /**
     * The message body for {@code cloudEvent}: its data, unchanged, or an empty array for an event with none. Data
     * that is itself present but empty also becomes an empty array here, the one thing an AMQP body cannot tell
     * apart from absent data on its own; {@link #toBasicProperties(CloudEvent, Map)} carries the
     * {@value #EMPTY_DATA_ATTRIBUTE} header alongside it so the two can still be told apart on the read side.
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
     * Every rebuilt attribute and extension is a {@link String}, since that is what a header actually carries,
     * except {@code streamversion} and {@code position}, which come back the {@code Long} that
     * {@link OccurrentCloudEventExtension} itself defines them as. Occurrent owns those two names and knows their
     * type, so restoring it here does not guess at anything, and a filter such as {@code Filter.streamVersion(...)}
     * still matches after the round trip instead of comparing a restored {@code String} against the {@code Long} a
     * filter operand actually is. Every other extension, application-defined ones included, stays a {@code String}
     * exactly as before, since this mapper has no way to know what type it should be. See ADR 133's amendment. A
     * binary extension is not un-Base64-decoded back to {@code byte[]} either, for the same not-knowable-here
     * reason, so a caller reading one gets the Base64 text rather than the original bytes.
     * <p>
     * Rebuilds under whatever {@link SpecVersion} the {@value #HEADER_PREFIX}{@value #SPEC_VERSION_ATTRIBUTE} header
     * names, {@link SpecVersion#V1} only when that header is absent, rather than always rebuilding as {@code V1}.
     * {@link #toBasicProperties(CloudEvent, Map)} writes whatever spec version the event it was given actually has,
     * so a {@code V03} event's own attributes, {@code schemaurl} rather than {@code dataschema} among them, only
     * round trip correctly when this reads that same version back instead of coercing every message to {@code V1}.
     * <p>
     * {@code data} comes back present but empty, rather than absent, when the {@value #EMPTY_DATA_ATTRIBUTE} header
     * is set. Without that header a zero-length body always means absent data, since an AMQP body cannot itself be
     * absent the way {@code data} can, and {@link #toBasicProperties(CloudEvent, Map)} is what sets the header in
     * the one case that distinction matters.
     */
    public static CloudEvent toCloudEvent(BasicProperties properties, byte[] body) {
        requireNonNull(properties, "properties cannot be null");
        requireNonNull(body, "body cannot be null");

        Map<String, Object> headers = properties.getHeaders();
        CloudEventBuilder builder = CloudEventBuilder.fromSpecVersion(specVersionOf(headers));
        boolean dataPresentEmpty = false;
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
                if (EMPTY_DATA_ATTRIBUTE.equals(attributeName)) {
                    dataPresentEmpty = true;
                    continue;
                }
                // toString() rather than a cast: a header value read back off the wire is a
                // com.rabbitmq.client.LongString for a string-valued header, not a java.lang.String.
                String stringValue = value.toString();
                if (NUMERIC_OCCURRENT_EXTENSIONS.contains(attributeName)) {
                    // Occurrent owns this extension name and defines it as a Long, so restore that type here
                    // instead of leaving a Filter on it comparing a String to a Number and never matching.
                    builder.withContextAttribute(attributeName, Long.parseLong(stringValue));
                } else {
                    builder.withContextAttribute(attributeName, stringValue);
                }
            }
        }
        String contentType = properties.getContentType();
        if (contentType != null) {
            builder.withDataContentType(contentType);
        }
        if (body.length > 0 || dataPresentEmpty) {
            builder.withData(BytesCloudEventData.wrap(body));
        }
        return builder.build();
    }

    /**
     * The {@link SpecVersion} the {@value #HEADER_PREFIX}{@value #SPEC_VERSION_ATTRIBUTE} header names, or
     * {@link SpecVersion#V1} when {@code headers} is {@code null} or carries no such header, for a message this
     * mapper never wrote.
     */
    private static SpecVersion specVersionOf(Map<String, Object> headers) {
        if (headers == null) {
            return SpecVersion.V1;
        }
        Object value = headers.get(HEADER_PREFIX + SPEC_VERSION_ATTRIBUTE);
        return value == null ? SpecVersion.V1 : SpecVersion.parse(value.toString());
    }
}
