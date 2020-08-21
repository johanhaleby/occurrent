package se.haleby.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionFilter;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Common interface for blocking subscriptions. The purpose of a subscription is to read events from an event store
 * and react to these events. Typically a subscription will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 *
 * @param <T> The type of the {@link CloudEvent} that the subscription produce. It's common that subscriptions
 *            produce "wrappers" around {@code CloudEvent}'s that includes the subscription position if the event store
 *            doesn't maintain this.
 */
public interface BlockingSubscription<T extends CloudEvent> {

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
    Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<T> action);


    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<T> action) {
        return subscribe(subscriptionId, filter, () -> startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at the supplied start position.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, StartAt startAt, Consumer<T> action) {
        return subscribe(subscriptionId, null, startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time with the specified <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<T> action) {
        return subscribe(subscriptionId, filter, StartAt.now(), action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, Consumer<T> action) {
        return subscribe(subscriptionId, null, StartAt.now(), action);
    }

    /**
     * Cancel the subscription
     */
    void cancelSubscription(String subscriptionId);

    /**
     * Shutdown the subscription and close all subscriptions (they can be resumed later if you start from a persisted subscription position).
     */
    default void shutdown() {
    }
}