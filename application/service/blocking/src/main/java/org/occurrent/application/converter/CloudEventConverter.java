package org.occurrent.application.converter;

import io.cloudevents.CloudEvent;

/**
 * A generic cloud event e
 */
public interface CloudEventConverter<T> {
    CloudEvent toCloudEvent(T domainEvent);

    T toDomainEvent(CloudEvent cloudEvent);
}