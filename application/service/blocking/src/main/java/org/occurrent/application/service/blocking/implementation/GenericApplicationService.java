package org.occurrent.application.service.blocking.implementation;

import io.cloudevents.CloudEvent;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.CloudEventConverter;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A generic application service that works in many scenarios. If you need more complex logic, such as transaction support, you may consider either wrapping it
 * in a custom {@code ApplicationService} implementation, or simply copy and paste the source into your own code base and make changes there.
 *
 * @param <T> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
public class GenericApplicationService<T> implements ApplicationService<T> {

    private final EventStore eventStore;
    private final CloudEventConverter<T> cloudEventConverter;

    public GenericApplicationService(EventStore eventStore, CloudEventConverter<T> cloudEventConverter) {
        this.eventStore = eventStore;
        this.cloudEventConverter = cloudEventConverter;
    }

    @Override
    public void execute(UUID streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        execute(streamId.toString(), functionThatCallsDomainModel);
    }

    @Override
    public void execute(String streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "Function that calls domain model cannot be null");
        // Read all events from the event store for a particular stream
        EventStream<CloudEvent> eventStream = eventStore.read(streamId);

        // Convert the cloud events into domain events
        Stream<T> eventsInStream = eventStream.events().map(cloudEventConverter::toDomainEvent);

        // Call a pure function from the domain model which returns a Stream of events
        Stream<CloudEvent> newEvents = functionThatCallsDomainModel.apply(eventsInStream).map(cloudEventConverter::toCloudEvent);

        // Write the new events to the event store
        eventStore.write(streamId, eventStream.version(), newEvents);
    }
}
