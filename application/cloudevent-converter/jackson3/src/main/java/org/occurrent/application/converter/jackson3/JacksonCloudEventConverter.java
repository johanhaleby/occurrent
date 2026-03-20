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

package org.occurrent.application.converter.jackson3;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.internal.CloudEventConverterSupport;
import org.occurrent.application.converter.internal.CloudEventConverterSupport.ThrowingBiFunction;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.function.Function;

/**
 * A Jackson 3-native {@link CloudEventConverter} that uses a {@link tools.jackson.databind.ObjectMapper}
 * to serialize a domain event to JSON (content type {@value #DEFAULT_CONTENT_TYPE}) that is used as data in a {@link CloudEvent}.
 *
 * @param <T> The type of your domain event(s) to convert
 */
public class JacksonCloudEventConverter<T> implements CloudEventConverter<T> {
    static final String DEFAULT_CONTENT_TYPE = CloudEventConverterSupport.DEFAULT_CONTENT_TYPE;

    private final ObjectMapper objectMapper;
    private final URI cloudEventSource;
    private final Function<T, String> idMapper;
    private final CloudEventTypeMapper<T> cloudEventTypeMapper;
    private final Function<T, OffsetDateTime> timeMapper;
    private final Function<T, String> subjectMapper;
    private final String contentType;

    /**
     * Create a new instance of the {@link JacksonCloudEventConverter} that does the following:
     * <ol>
     *     <li>Uses a random UUID as cloud event id</li>
     *     <li>Uses the fully-qualified name of the domain event class as cloud event type. <b>You should definitely change this in production!</b></li>
     *     <li>Uses {@code OffsetDateTime.now(UTC)} as cloud event time</li>
     *     <li>Uses charset UTF-8 when converting the domain event to/from JSON</li>
     *     <li>No subject</li>
     * </ol>
     *
     * @param objectMapper The ObjectMapper instance to use
     * @param cloudEventSource The cloud event source.
     * @see Builder The Builder for more advanced configuration
     */
    public JacksonCloudEventConverter(ObjectMapper objectMapper, URI cloudEventSource) {
        this(objectMapper, cloudEventSource, defaultIdMapperFunction(), defaultTypeMapper(), defaultTimeMapperFunction(), defaultSubjectMapperFunction(), DEFAULT_CONTENT_TYPE);
    }

    private JacksonCloudEventConverter(ObjectMapper objectMapper, URI cloudEventSource, Function<T, String> idMapper, CloudEventTypeMapper<T> cloudEventTypeMapper, Function<T, OffsetDateTime> timeMapper, Function<T, String> subjectMapper, String contentType) {
        this.objectMapper = java.util.Objects.requireNonNull(objectMapper, ObjectMapper.class.getSimpleName() + " cannot be null");
        this.cloudEventSource = java.util.Objects.requireNonNull(cloudEventSource, "cloudEventSource cannot be null");
        this.idMapper = java.util.Objects.requireNonNull(idMapper, "idMapper cannot be null");
        this.cloudEventTypeMapper = java.util.Objects.requireNonNull(cloudEventTypeMapper, CloudEventTypeMapper.class.getSimpleName() + " cannot be null");
        this.timeMapper = java.util.Objects.requireNonNull(timeMapper, "timeMapper cannot be null");
        this.subjectMapper = java.util.Objects.requireNonNull(subjectMapper, "subjectMapper cannot be null");
        this.contentType = contentType;
    }

    @Override
    public CloudEvent toCloudEvent(T domainEvent) {
        return CloudEventConverterSupport.toCloudEvent(
                domainEvent,
                cloudEventSource,
                idMapper,
                cloudEventTypeMapper,
                timeMapper,
                subjectMapper,
                contentType,
                value -> objectMapper.convertValue(value, new TypeReference<>() {
                }),
                objectMapper::writeValueAsBytes
        );
    }

    @Override
    public T toDomainEvent(CloudEvent cloudEvent) {
        ThrowingBiFunction<Map<String, Object>, Class<T>, T> fromMap = (value, type) -> objectMapper.convertValue(value, type);
        ThrowingBiFunction<byte[], Class<T>, T> fromBytes = objectMapper::readValue;
        return CloudEventConverterSupport.toDomainEvent(cloudEvent, cloudEventTypeMapper, fromMap, fromBytes);
    }

    @Override
    public String getCloudEventType(Class<? extends T> type) {
        return cloudEventTypeMapper.getCloudEventType(type);
    }

    public static final class Builder<T> {
        private final ObjectMapper objectMapper;
        private final URI cloudEventSource;
        private String contentType = DEFAULT_CONTENT_TYPE;
        private Function<T, String> idMapper = defaultIdMapperFunction();
        private CloudEventTypeMapper<T> cloudEventTypeMapper = defaultTypeMapper();
        private Function<T, OffsetDateTime> timeMapper = defaultTimeMapperFunction();
        private Function<T, String> subjectMapper = defaultSubjectMapperFunction();

        public Builder(ObjectMapper objectMapper, URI cloudEventSource) {
            this.objectMapper = objectMapper;
            this.cloudEventSource = cloudEventSource;
        }

        public Builder<T> contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder<T> idMapper(Function<T, String> idMapper) {
            this.idMapper = idMapper;
            return this;
        }

        public Builder<T> typeMapper(CloudEventTypeMapper<T> cloudEventTypeMapper) {
            this.cloudEventTypeMapper = cloudEventTypeMapper;
            return this;
        }

        public Builder<T> timeMapper(Function<T, OffsetDateTime> timeMapper) {
            this.timeMapper = timeMapper;
            return this;
        }

        public Builder<T> subjectMapper(Function<T, String> subjectMapper) {
            this.subjectMapper = subjectMapper;
            return this;
        }

        public JacksonCloudEventConverter<T> build() {
            return new JacksonCloudEventConverter<>(objectMapper, cloudEventSource, idMapper, cloudEventTypeMapper, timeMapper, subjectMapper, contentType);
        }
    }

    static <T> Function<T, String> defaultIdMapperFunction() {
        return CloudEventConverterSupport.defaultIdMapperFunction();
    }

    static <T> CloudEventTypeMapper<T> defaultTypeMapper() {
        return CloudEventConverterSupport.defaultTypeMapper();
    }

    static <T> Function<T, OffsetDateTime> defaultTimeMapperFunction() {
        return CloudEventConverterSupport.defaultTimeMapperFunction();
    }

    static <T> Function<T, String> defaultSubjectMapperFunction() {
        return CloudEventConverterSupport.defaultSubjectMapperFunction();
    }
}
