/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.application.converter.xstream;

import com.thoughtworks.xstream.XStream;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.typemapper.CloudEventTypeGetter;

import java.net.URI;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * An {@link CloudEventConverter} that uses {@link XStream} to serialize a domain event into XML (content type {@value #DEFAULT_CONTENT_TYPE}) that is used as data in a {@link CloudEvent}.
 *
 * @param <T> The type of your domain event(s) to convert
 */
public class XStreamCloudEventConverter<T> implements CloudEventConverter<T>, CloudEventTypeGetter<T> {
    private static final String DEFAULT_CONTENT_TYPE = "application/xml";
    private static final Charset DEFAULT_CHARSET = UTF_8;

    private final XStream xStream;
    private final URI cloudEventSource;
    private final Function<T, String> idMapper;
    private final CloudEventTypeGetter<T> cloudEventTypeGetter;
    private final Function<T, OffsetDateTime> timeMapper;
    private final Function<T, String> subjectMapper;
    private final String contentType;
    private final Charset charset;

    /**
     * Create a new instance of the {@link XStreamCloudEventConverter} that does the following:
     * <ol>
     *     <li>Uses a random UUID as cloud event id</li>
     *     <li>Uses the simple name of the domain event class as cloud event type.</li>
     *     <li>Uses {@code OffsetDateTime.now(UTC)} as cloud event time</li>
     *     <li>Uses charset UTF-8 when converting the domain event to/from XML</li>
     *     <li>No subject</li>
     * </ol>
     * <p>
     * See <a href="https://occurrent.org/documentation#cloudevents">cloud event documentation</a> for info on what the cloud event attributes mean.<br><br>
     * Use {@link Builder} for more advanced configuration.
     *
     * @param xStream          The XStream instance to use
     * @param cloudEventSource The cloud event source.
     * @see Builder The Builder for more advanced configuration
     */
    public XStreamCloudEventConverter(XStream xStream, URI cloudEventSource) {
        this(xStream, cloudEventSource, defaultIdMapperFunction(), defaultCloudEventTypeGetter(), defaultTimeMapperFunction(), defaultSubjectMapperFunction(), DEFAULT_CONTENT_TYPE, DEFAULT_CHARSET);
    }

    private XStreamCloudEventConverter(XStream xStream, URI cloudEventSource, Function<T, String> idMapper, CloudEventTypeGetter<T> cloudEventTypeGetter, Function<T, OffsetDateTime> timeMapper, Function<T, String> subjectMapper, String contentType, Charset charset) {
        requireNonNull(xStream, XStream.class.getSimpleName() + " cannot be null");
        requireNonNull(cloudEventSource, "cloudEventSource cannot be null");
        requireNonNull(idMapper, "idMapper cannot be null");
        requireNonNull(cloudEventTypeGetter, "cloudEventTypeGetter cannot be null");
        requireNonNull(timeMapper, "timeMapper cannot be null");
        requireNonNull(subjectMapper, "subjectMapper cannot be null");
        requireNonNull(charset, Charset.class.getSimpleName() + " cannot be null");
        this.xStream = xStream;
        this.cloudEventSource = cloudEventSource;
        this.idMapper = idMapper;
        this.timeMapper = timeMapper;
        this.subjectMapper = subjectMapper;
        this.contentType = contentType;
        this.charset = charset;
        this.cloudEventTypeGetter = cloudEventTypeGetter;
    }

    /**
     * Converts the {@code domainEvent} into a {@link CloudEvent} using {@link XStream}.
     *
     * @param domainEvent The domain event to convert
     * @return A {@link CloudEvent} converted from the <code>domainEvent</code>.
     */
    @Override
    public CloudEvent toCloudEvent(T domainEvent) {
        requireNonNull(domainEvent, "Domain event cannot be null");
        return CloudEventBuilder.v1()
                .withId(idMapper.apply(domainEvent))
                .withSource(cloudEventSource)
                .withType(cloudEventTypeGetter.getCloudEventType(domainEvent))
                .withTime(timeMapper.apply(domainEvent))
                .withSubject(subjectMapper.apply(domainEvent))
                .withDataContentType(contentType)
                .withData(xStream.toXML(domainEvent).getBytes(charset))
                .build();
    }

    /**
     * Converts the {@link CloudEvent} back into a {@code domainEvent} using {@link XStream}.
     *
     * @param cloudEvent The cloud event to convert
     * @return A <code>domainEvent</code> converted from a {@link CloudEvent}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public T toDomainEvent(CloudEvent cloudEvent) {
        return (T) xStream.fromXML(new String(requireNonNull(cloudEvent.getData()).toBytes(), charset));
    }

    @Override
    public String getCloudEventType(Class<? extends T> type) {
        return cloudEventTypeGetter.getCloudEventType(type);
    }

    public static final class Builder<T> {
        private final XStream xStream;
        private final URI cloudEventSource;
        private String contentType = DEFAULT_CONTENT_TYPE;
        private Charset charset = DEFAULT_CHARSET;
        private Function<T, String> idMapper = defaultIdMapperFunction();
        private CloudEventTypeGetter<T> cloudEventTypeGetter = defaultCloudEventTypeGetter();
        private Function<T, OffsetDateTime> timeMapper = defaultTimeMapperFunction();
        private Function<T, String> subjectMapper = defaultSubjectMapperFunction();

        public Builder(XStream xStream, URI cloudEventSource) {
            this.xStream = xStream;
            this.cloudEventSource = cloudEventSource;
        }

        /**
         * @param contentType Specify the content type to use in the generated cloud event
         */
        public Builder<T> contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        /**
         * @param idMapper A function that generates the cloud event id based on the domain event. By default, a random UUID is used.
         */
        public Builder<T> idMapper(Function<T, String> idMapper) {
            this.idMapper = idMapper;
            return this;
        }

        /**
         * @param cloudEventTypeGetter A function that generates the cloud event type based on the domain event. By default, the "simple name" of the domain event is used.
         */
        public Builder<T> typeGetter(CloudEventTypeGetter<T> cloudEventTypeGetter) {
            this.cloudEventTypeGetter = cloudEventTypeGetter;
            return this;
        }

        /**
         * @param timeMapper A function that generates the cloud event time based on the domain event. By default, {@code OffsetDateTime.now(UTC)} is always returned.
         */
        public Builder<T> timeMapper(Function<T, OffsetDateTime> timeMapper) {
            this.timeMapper = timeMapper;
            return this;
        }

        /**
         * @param subjectMapper A function that generates the cloud event subject based on the domain event. By default, {@code null} is always returned.
         */
        public Builder<T> subjectMapper(Function<T, String> subjectMapper) {
            this.subjectMapper = subjectMapper;
            return this;
        }

        /**
         * @param charset Specify the charset that XStream should use when serializing the domain event to bytes that is stored as the cloud event data attribute.
         */
        public Builder<T> charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        /**
         * @return A {@link XStreamCloudEventConverter} instance with the configured settings
         */
        public XStreamCloudEventConverter<T> build() {
            return new XStreamCloudEventConverter<>(xStream, cloudEventSource, idMapper, cloudEventTypeGetter, timeMapper, subjectMapper, contentType, charset);
        }
    }


    private static <T> Function<T, String> defaultIdMapperFunction() {
        return __ -> UUID.randomUUID().toString();
    }

    private static <T> CloudEventTypeGetter<T> defaultCloudEventTypeGetter() {
        return Class::getSimpleName;
    }

    private static <T> Function<T, OffsetDateTime> defaultTimeMapperFunction() {
        return __ -> OffsetDateTime.now(UTC);
    }

    private static <T> Function<T, String> defaultSubjectMapperFunction() {
        return __ -> null;
    }
}