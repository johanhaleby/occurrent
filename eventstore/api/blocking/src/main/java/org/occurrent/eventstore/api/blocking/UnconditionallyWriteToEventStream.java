package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

import java.util.stream.Stream;

/**
 * An interface that should be implemented by event streams that supports writing events to a stream without specifying a write condition.
 */
public interface UnconditionallyWriteToEventStream {
    /**
     * Write {@code events} to a stream
     *
     * @param streamId The stream id of the stream to write to
     * @param events The events to write
     */
    void write(String streamId, Stream<CloudEvent> events);
}