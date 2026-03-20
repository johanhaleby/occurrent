/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.application.converter.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * Internal shared support for Jackson-backed CloudEvent converters.
 */
public final class CloudEventConverterSupport {
    public static final String DEFAULT_CONTENT_TYPE = "application/json";

    private CloudEventConverterSupport() {
    }

    public static <T> CloudEvent toCloudEvent(T domainEvent, URI cloudEventSource, Function<T, String> idMapper, CloudEventTypeMapper<T> cloudEventTypeMapper, Function<T, OffsetDateTime> timeMapper, Function<T, String> subjectMapper, String contentType, Function<T, Map<String, Object>> dataMapper, ThrowingFunction<Map<String, Object>, byte[]> dataSerializer) {
        requireNonNull(domainEvent, "Domain event cannot be null");
        Map<String, Object> data = dataMapper.apply(domainEvent);
        PojoCloudEventData<Map<String, Object>> cloudEventData = PojoCloudEventData.wrap(data, value -> {
            try {
                return dataSerializer.apply(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return CloudEventBuilder.v1()
                .withId(idMapper.apply(domainEvent))
                .withSource(cloudEventSource)
                .withType(cloudEventTypeMapper.getCloudEventType(domainEvent))
                .withTime(timeMapper.apply(domainEvent))
                .withSubject(subjectMapper.apply(domainEvent))
                .withDataContentType(contentType)
                .withData(cloudEventData)
                .build();
    }

    @SuppressWarnings("unchecked")
    public static <T> T toDomainEvent(CloudEvent cloudEvent, CloudEventTypeMapper<T> cloudEventTypeMapper, ThrowingBiFunction<Map<String, Object>, Class<T>, T> fromMap, ThrowingBiFunction<byte[], Class<T>, T> fromBytes) {
        CloudEventData data = cloudEvent.getData();
        Class<T> domainEventType = cloudEventTypeMapper.getDomainEventType(cloudEvent.getType());

        if (data instanceof PojoCloudEventData && ((PojoCloudEventData<Object>) data).getValue() instanceof Map) {
            Map<String, Object> value = (Map<String, Object>) ((PojoCloudEventData<?>) data).getValue();
            try {
                return fromMap.apply(value, domainEventType);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        try {
            return fromBytes.apply(requireNonNull(data, "cloud event data cannot be null").toBytes(), domainEventType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> Function<T, String> defaultIdMapperFunction() {
        return __ -> UUID.randomUUID().toString();
    }

    public static <T> CloudEventTypeMapper<T> defaultTypeMapper() {
        return ReflectionCloudEventTypeMapper.qualified();
    }

    public static <T> Function<T, OffsetDateTime> defaultTimeMapperFunction() {
        return __ -> OffsetDateTime.now(UTC);
    }

    public static <T> Function<T, String> defaultSubjectMapperFunction() {
        return __ -> null;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T, U, R> {
        R apply(T t, U u) throws IOException;
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T t) throws IOException;
    }
}
