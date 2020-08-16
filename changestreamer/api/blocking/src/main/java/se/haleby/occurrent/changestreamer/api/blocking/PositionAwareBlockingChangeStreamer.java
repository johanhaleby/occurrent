package se.haleby.occurrent.changestreamer.api.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.CloudEventWithChangeStreamPosition;

/**
 * A {@link BlockingChangeStreamer} that produces {@link CloudEventWithChangeStreamPosition} compatible {@link CloudEvent}'s.
 * This is useful for subscribers that want to persist the stream position for a given subscription if the event store doesn't
 * maintain the position for subscriptions.
 */
public interface PositionAwareBlockingChangeStreamer extends BlockingChangeStreamer<CloudEventWithChangeStreamPosition> {

    /**
     * The global change stream position might be e.g. the wall clock time of the server, vector clock, number of events consumed etc.
     * This is useful to get the initial position of a subscription before any message has been consumed by the subscription
     * (and thus no {@link ChangeStreamPosition} has been persisted for the subscription). The reason for doing this would be
     * to make sure that a subscription doesn't loose the very first message if there's an error consuming the first event.
     *
     * @return The global change stream position for the database.
     */
    ChangeStreamPosition globalChangeStreamPosition();
}
