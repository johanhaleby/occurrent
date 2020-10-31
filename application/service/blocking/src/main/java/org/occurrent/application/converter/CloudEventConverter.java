package org.occurrent.application.converter;

import io.cloudevents.CloudEvent;

/**
 * A cloud event converter interface that is used by Occurrent application services
 * to convert to and from domain events.
 *
 * @param <T> The type of your domain event
 */
public interface CloudEventConverter<T> {

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
}