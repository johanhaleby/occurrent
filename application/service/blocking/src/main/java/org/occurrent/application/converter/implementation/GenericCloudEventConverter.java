package org.occurrent.application.converter.implementation;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;

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

    public GenericCloudEventConverter(Function<CloudEvent, T> convertToDomainEvent, Function<T, CloudEvent> convertToCloudEvent) {
        this.convertToDomainEvent = convertToDomainEvent;
        this.convertToCloudEvent = convertToCloudEvent;
    }

    @Override
    public CloudEvent toCloudEvent(T domainEvent) {
        return convertToCloudEvent.apply(domainEvent);
    }

    @Override
    public T toDomainEvent(CloudEvent cloudEvent) {
        return convertToDomainEvent.apply(cloudEvent);
    }
}
