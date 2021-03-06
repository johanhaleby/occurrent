package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.function.Consumer;

/**
 * The purpose of a subscription is to read events from an event store and react to these events.
 * <p>
 * A subscription may be used to create read models (such as views, projections, sagas, snapshots etc) or
 * forward the event to another piece of infrastructure such as a message bus or other eventing infrastructure.
 * <p>
 * A blocking subscription model also you to create and manage subscriptions that'll use blocking IO.
 */
public interface Subscribable {

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action);

    /**
     * Start listening to cloud events persisted to the event store at the supplied start position.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, null, startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time with the specified <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), action);
    }
}
