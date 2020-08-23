package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;

import java.util.stream.Stream;

import static org.occurrent.eventstore.api.WriteCondition.streamVersionEq;

/**
 * Event stores that supports conditional writes to an event stream should implement this interface.
 */
public interface ConditionallyWriteToEventStream {

    /**
     * A convenience function that writes events to an event store if the stream version is equal to {@code expectedStreamVersion}.
     *
     * @param streamId              The id of the stream
     * @param expectedStreamVersion The stream must be equal to this version in order for the events to be written
     * @param events                The events to be appended/written to the stream
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     * @see #write(String, WriteCondition, Stream) for more advanced write conditions
     */
    default void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        write(streamId, streamVersionEq(expectedStreamVersion), events);
    }

    /**
     * Conditionally write to an event store
     *
     * @param streamId       The id of the stream
     * @param writeCondition The write condition that must be fulfilled for the events to be written
     * @param events         The events to be appended/written to the stream
     * @throws WriteConditionNotFulfilledException When the <code>writeCondition</code> was not fulfilled and the events couldn't be written
     * @throws DuplicateCloudEventException        If a cloud event in the supplied <code>events</code> stream already exists in the event store
     */
    void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events);
}