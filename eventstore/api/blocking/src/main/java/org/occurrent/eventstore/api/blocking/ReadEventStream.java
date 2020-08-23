package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

/**
 * An interface that should be implemented by event stores that supports reading an {@link EventStream}.
 */
public interface ReadEventStream {

    /**
     * Read all events from a particular event stream
     *
     * @param streamId The id of the stream to read.
     * @return An {@link EventStream} containing the events of the stream. Will return an {@link EventStream} with version {@code 0} if event stream doesn't exists.
     */
    default EventStream<CloudEvent> read(String streamId) {
        return read(streamId, 0, Integer.MAX_VALUE);
    }

    /**
     * Read events from a particular event stream from a particular position.
     *
     * @param streamId The id of the stream to read.
     * @return An {@link EventStream} containing the events of the stream. Will return an {@link EventStream} with version {@code 0} if event stream doesn't exists.
     */
    EventStream<CloudEvent> read(String streamId, int skip, int limit);
}
