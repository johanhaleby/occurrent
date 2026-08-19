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

package org.occurrent.broker.kafka.blocking.domain;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DomainEventForwarder;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.cloudevents.EventMetadata;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A {@link DomainEventSink} for a caller that already has a domain event of type {@code E}, converting it and
 * delegating to a {@link CloudEventSink} rather than talking to Kafka itself. {@link KafkaCloudEventSink} is the
 * obvious choice for that delegate, but nothing here requires it. Any {@link CloudEventSink} works, including an
 * application's own wrapper.
 * <p>
 * Pairing this with {@link DomainEventForwarder} is the one combination this design tells you not to build.
 * {@link DomainEventForwarder} always starts from a stored {@link CloudEvent}, so decoding it to a domain event and
 * handing it to a sink that converts straight back means one decode and one re-encode per event, and it is lossy
 * besides, since {@link #publish(Object)} builds a fresh event and only the id, source, subject and time a
 * {@link CloudEventConverter} happens to reproduce survive the round trip. Forwarding stored events uses
 * {@link org.occurrent.broker.api.blocking.CloudEventForwarder} with a plain {@link CloudEventSink} instead, which
 * converts nothing. {@link DomainEventForwarder} is for a {@link DomainEventSink} an application implements itself,
 * one that genuinely publishes through its own converter, never for this sink. Call this sink directly instead,
 * {@link #publish(Object)} for a domain event that never went through the event store, or
 * {@link #publish(EventMetadata, Object)} for one whose metadata came from elsewhere.
 */
public final class KafkaDomainEventSink<E> implements DomainEventSink<E> {

    private final CloudEventSink cloudEventSink;
    private final CloudEventConverter<E> converter;

    private KafkaDomainEventSink(CloudEventSink cloudEventSink, CloudEventConverter<E> converter) {
        this.cloudEventSink = cloudEventSink;
        this.converter = converter;
    }

    /**
     * @param cloudEventSink Publishes what {@code converter} produces. {@link KafkaCloudEventSink} for Kafka, or an
     *                       application's own {@link CloudEventSink}.
     * @param converter      Converts a domain event of type {@code E} to and from a {@link CloudEvent}.
     */
    public static <E> KafkaDomainEventSink<E> using(CloudEventSink cloudEventSink, CloudEventConverter<E> converter) {
        requireNonNull(cloudEventSink, CloudEventSink.class.getSimpleName() + " cannot be null");
        requireNonNull(converter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        return new KafkaDomainEventSink<>(cloudEventSink, converter);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Any extension {@code converter} sets on the converted event is stripped, not just the stream-identity ones,
     * so a consumer genuinely sees {@link EventMetadata#empty() EventMetadata} as documented above, regardless of
     * what {@code converter} happens to carry.
     */
    @Override
    public void publish(E domainEvent) {
        requireNonNull(domainEvent, "domainEvent cannot be null");
        cloudEventSink.publish(copyCoreAttributes(converter.toCloudEvent(domainEvent)).build());
    }

    /**
     * Converts {@code domainEvent} and stamps every extension {@code metadata} carries onto the resulting
     * {@link CloudEvent} before publishing it, so a consumer can rebuild an {@link EventMetadata} that matches what
     * {@code metadata} held here. Where {@code converter} already set an extension of its own, {@code metadata}
     * wins, since the caller reading it off a stored event is the one with the store's own answer, including a
     * {@code null} value in {@code metadata}, which drops that extension entirely rather than leaving the
     * converter's own value in place.
     */
    @Override
    public void publish(EventMetadata metadata, E domainEvent) {
        requireNonNull(metadata, EventMetadata.class.getSimpleName() + " cannot be null");
        requireNonNull(domainEvent, "domainEvent cannot be null");
        CloudEvent convertedEvent = converter.toCloudEvent(domainEvent);
        CloudEventBuilder builder = copyCoreAttributes(convertedEvent);
        for (String extensionName : convertedEvent.getExtensionNames()) {
            // metadata decides this extension's fate below, whether that is overriding it or dropping it, so it is
            // left out of this copy rather than set here and possibly overwritten a few lines down.
            if (!metadata.getData().containsKey(extensionName)) {
                stampExtension(builder, extensionName, convertedEvent.getExtension(extensionName));
            }
        }
        for (Map.Entry<String, Object> extension : metadata.getData().entrySet()) {
            Object value = extension.getValue();
            if (value != null) {
                stampExtension(builder, extension.getKey(), value);
            }
        }
        cloudEventSink.publish(builder.build());
    }

    private static CloudEventBuilder copyCoreAttributes(CloudEvent source) {
        return CloudEventBuilder.v1()
                .withId(source.getId())
                .withSource(source.getSource())
                .withType(source.getType())
                .withSubject(source.getSubject())
                .withTime(source.getTime())
                .withDataSchema(source.getDataSchema())
                .withDataContentType(source.getDataContentType())
                .withData(source.getData());
    }

    private static void stampExtension(CloudEventBuilder builder, String key, Object value) {
        switch (value) {
            case String s -> builder.withExtension(key, s);
            case Boolean b -> builder.withExtension(key, b);
            case Number n -> builder.withExtension(key, n);
            case URI u -> builder.withExtension(key, u);
            case OffsetDateTime o -> builder.withExtension(key, o);
            case byte[] bytes -> builder.withExtension(key, bytes);
            default -> throw new IllegalArgumentException("Extension \"" + key + "\" has an unsupported value type for a CloudEvent extension: " + value.getClass());
        }
    }
}
