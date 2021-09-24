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

package org.occurrent.application.converter;

import io.cloudevents.CloudEvent;

import java.util.stream.Stream;

/**
 * A cloud event converter interface that is used by Occurrent application services
 * to convert to and from domain events.
 *
 * @param <T> The type of your domain event
 */
public interface CloudEventConverter<T> {

    /**
     * Convert a stream of domain events into a stream of cloud events.
     * The reason for implementing this method is to allow adding things such as correlation id
     * that should be the same for all events in a stream.
     *
     * @param events The domain events to convert to cloud events
     * @return A stram of cloud events.
     */
    default Stream<CloudEvent> toCloudEvents(Stream<T> events) {
        Stream<T> stream = events == null ? Stream.empty() : events;
        return stream.map(this::toCloudEvent);
    }

    /**
     * Convert a domain event into a cloud event
     *
     * @param domainEvent The domain event to convert
     * @return The {@link CloudEvent} instance, converted from the domain event.
     */
    CloudEvent toCloudEvent(T domainEvent);

    /**
     * Convert a cloud event to a domain event
     *
     * @param cloudEvent The cloud event to convert
     * @return The domain event instance, converted from the cloud event.
     */
    T toDomainEvent(CloudEvent cloudEvent);

    /**
     * Convert a stream of cloud events into a stream of domain events.
     * Typically, you only need to implement {@link #toDomainEvent(CloudEvent)}.
     *
     * @param events The cloud events to convert to domain events
     * @return A stram of cloud events.
     */
    default Stream<T> toDomainEvents(Stream<CloudEvent> events) {
        Stream<CloudEvent> stream = events == null ? Stream.empty() : events;
        return stream.map(this::toDomainEvent);
    }
}