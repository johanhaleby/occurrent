/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.application.converter.generic;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * A generic implementation of a {@link CloudEventConverter} that takes two functions,
 * that converts between a cloud event and a domain event and vice versa.
 *
 * @param <T> The type of the domain event
 */
public class GenericCloudEventConverter<T> implements CloudEventConverter<T> {

    private final Function<CloudEvent, T> convertToDomainEvent;
    private final Function<T, CloudEvent> convertToCloudEvent;
    private final CloudEventTypeGetter<T> getCloudEventType;

    /**
     * Create an instance of {@link GenericCloudEventConverter} that uses reflection (the simple name of the domain event class) as an implementation for {@link #getCloudEventType(Class)}.
     * Use {@link GenericCloudEventConverter#GenericCloudEventConverter(Function, Function, CloudEventTypeGetter)} to configure how the cloud event type should be generated from the domain event type.
     */
    public GenericCloudEventConverter(Function<CloudEvent, T> convertToDomainEvent, Function<T, CloudEvent> convertToCloudEvent) {
        this(convertToDomainEvent, convertToCloudEvent, Class::getSimpleName);
    }

    /**
     * Create an instance of {@link GenericCloudEventConverter} that uses the {@code getCloudEventType} to get the cloud event type from the domain event type.
     */
    public GenericCloudEventConverter(Function<CloudEvent, T> convertToDomainEvent, Function<T, CloudEvent> convertToCloudEvent, CloudEventTypeGetter<T> getCloudEventType) {
        Objects.requireNonNull(convertToDomainEvent, "convertToDomainEvent cannot be null");
        Objects.requireNonNull(convertToCloudEvent, "convertToCloudEvent cannot be null");
        Objects.requireNonNull(getCloudEventType, "getCloudEventType cannot be null");
        this.convertToDomainEvent = convertToDomainEvent;
        this.convertToCloudEvent = convertToCloudEvent;
        this.getCloudEventType = getCloudEventType;
    }

    @Override
    public CloudEvent toCloudEvent(T domainEvent) {
        return convertToCloudEvent.apply(domainEvent);
    }

    @Override
    public T toDomainEvent(CloudEvent cloudEvent) {
        return convertToDomainEvent.apply(cloudEvent);
    }

    @Override
    public String getCloudEventType(Class<? extends T> type) {
        return getCloudEventType.getCloudEventType(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericCloudEventConverter)) return false;
        GenericCloudEventConverter<?> that = (GenericCloudEventConverter<?>) o;
        return Objects.equals(convertToDomainEvent, that.convertToDomainEvent) && Objects.equals(convertToCloudEvent, that.convertToCloudEvent) && Objects.equals(getCloudEventType, that.getCloudEventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(convertToDomainEvent, convertToCloudEvent, getCloudEventType);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", GenericCloudEventConverter.class.getSimpleName() + "[", "]")
                .add("convertToDomainEvent=" + convertToDomainEvent)
                .add("convertToCloudEvent=" + convertToCloudEvent)
                .add("getCloudEventType=" + getCloudEventType)
                .toString();
    }
}