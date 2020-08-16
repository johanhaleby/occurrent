package se.haleby.occurrent.changestreamer.api.blocking;

import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.CloudEventWithChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Common interface for blocking change streamers. The purpose of a change streamer is to read events from an event store
 * and react to these events. Typically a change streamer will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 */
public interface BlockingChangeStreamer {

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriptionId  The id of the subscription, must be unique!
     * @param filter          The filter to use to limit which events that are of interest from the EventStore.
     * @param startAtSupplier A supplier that returns the start position to start the subscription from.
     *                        This is a useful alternative to just passing a fixed "StartAt" value if the stream is broken and re-subscribed to.
     *                        In this cases streams should be restarted from the latest position and not the start position as it were when the application
     * @param action          This action will be invoked for each cloud event that is stored in the EventStore.
     */
    Subscription stream(String subscriptionId, ChangeStreamFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEventWithChangeStreamPosition> action);


    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *  @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, ChangeStreamFilter filter, StartAt startAt, Consumer<CloudEventWithChangeStreamPosition> action) {
        return stream(subscriptionId, filter, () -> startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at the supplied start position.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, StartAt startAt, Consumer<CloudEventWithChangeStreamPosition> action) {
        return stream(subscriptionId, null, startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time with the specified <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, ChangeStreamFilter filter, Consumer<CloudEventWithChangeStreamPosition> action) {
        return stream(subscriptionId, filter, StartAt.now(), action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, Consumer<CloudEventWithChangeStreamPosition> action) {
        return stream(subscriptionId, null, StartAt.now(), action);
    }

    /**
     * Cancel the subscription
     */
    void cancelSubscription(String subscriptionId);

    /**
     * Shutdown the change streamer and close all subscriptions (they can be resumed later if you start from a persisted stream position).
     */
    default void shutdown() {
    }

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