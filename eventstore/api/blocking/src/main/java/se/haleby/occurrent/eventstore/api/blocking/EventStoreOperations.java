package se.haleby.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;

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

    /**
     * Update a unique cloud event. This is mainly useful as a strategy for complying with e.g. GDPR if you need to
     * remove some attributes that are sensitive.
     *
     * @param cloudEventId     The id of the cloud event (see {@link CloudEvent#getId()})
     * @param cloudEventSource The source of the cloud event (see {@link CloudEvent#getSource()})
     * @param fn               A function that takes the existing cloud event and you're expected to return an
     *                         updated cloud event (cannot be <code>null</code>). If the a cloud event is not found
     *                         for the given <code>cloudEventId</code> and <code>cloudEventSource</code> then the <code>fn</code>
     *                         function will not be called and an empty <code>Optional</code> will be returned.
     * @return The updated cloud event or an empty <code>Optional</code> if no cloud event was found matching the <code>cloudEventId</code> and <code>cloudEventSource</code>.
     */
    Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> fn);
    // TODO Implement generic delete method with a Condition
}