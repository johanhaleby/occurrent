package se.haleby.occurrent.changestreamer.api.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.StartAt;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Common interface for blocking change streamers. The purpose of a change streamer is to read events from an event store
 * and react to these events. Typically a change streamer will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 *
 * @param <T> The type of the {@link CloudEvent} that the change streamer produce. It's common that change streamers
 *            produce "wrappers" around {@code CloudEvent}'s that includes the change stream position if the event store
 *            doesn't maintain this.
 */
public interface BlockingChangeStreamer<T extends CloudEvent> {

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
    Subscription stream(String subscriptionId, ChangeStreamFilter filter, Supplier<StartAt> startAtSupplier, Consumer<T> action);


    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, ChangeStreamFilter filter, StartAt startAt, Consumer<T> action) {
        return stream(subscriptionId, filter, () -> startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at the supplied start position.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, StartAt startAt, Consumer<T> action) {
        return stream(subscriptionId, null, startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time with the specified <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, ChangeStreamFilter filter, Consumer<T> action) {
        return stream(subscriptionId, filter, StartAt.now(), action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription stream(String subscriptionId, Consumer<T> action) {
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
}