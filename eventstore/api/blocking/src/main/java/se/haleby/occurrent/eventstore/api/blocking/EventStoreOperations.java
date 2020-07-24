package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

import java.net.URI;

/**
 * Additional operations that may be supported by an {@link EventStore} implementation that is not typically part of a
 * "transactional" use case.
 */
public interface EventStoreOperations {

    /**
     * Delete all events and metadata associated with a event stream
     *
     * @param streamId The id of the stream to delete
     */
    void deleteEventStream(String streamId);

    /**
     * Delete all events associated with a event stream (metadata is retained if available)
     *
     * @param streamId The id of the event stream whose events to delete
     */
    void deleteAllEventsInEventStream(String streamId);

    /**
     * Delete a specific cloud event from an event stream
     *
     * @param cloudEventId     The id of the cloud event (see {@link CloudEvent#getId()})
     * @param cloudEventSource The source of the cloud event (see {@link CloudEvent#getSource()})
     */
    void deleteEvent(String cloudEventId, URI cloudEventSource);

    // TODO Implement generic delete method with a Condition
}