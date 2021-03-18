package org.occurrent.subscription.api.blocking;

/**
 * The contract for competing consumer strategies. A competing consumer strategy is used with a "competing consumer subscription model" to allow for
 * <a href="https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html">competing consumers</a> (i.e. concurrent message processing).
 * The purpose of a {@code CompetingConsumerStrategy} is to implement an algorithm or use some underlying infrastructure that makes sure that only one subscriber
 * reads events from a particular subscription. I.e. multiple subscribers can subscribe to the same subscription, but only one of them will receive a particular event.
 * If one subscriber crashes, the {@code CompetingConsumerStrategy} implementation, will notify the {@code CompetingConsumerSubscriptionModel} (which implements the
 * {@link CompetingConsumerListener} interface) that another subscriber may take over. This is typically done by (distributed) leader election.
 */
public interface CompetingConsumerStrategy {

    /**
     * Register a new competing consumer that will be able to receive events (given that the conditions maintained by the {@code CompetingConsumerStrategy} allow for it).
     *
     * @param subscriptionId The subscription if to consume from
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the registered competing consumer has access (lock) to consume events, <code>false</code> otherwise.
     */
    boolean registerCompetingConsumer(String subscriptionId, String subscriberId);

    /**
     * Unregister a competing consumer, it'll no longer receive events. If this competing consumer currently has lock to receive events,
     * the lock will be handed to another subscriber for the same subscription.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     */
    void unregisterCompetingConsumer(String subscriptionId, String subscriberId);

    /**
     * Check whether a particular subscriber has the lock (access) to read events for the given subscription.
     *
     * @param subscriptionId The id of of the subscription
     * @param subscriberId   The unique of of the subscriber
     * @return <code>true</code> if the subscriber has the lock, <code>false</code> otherwise.
     */
    boolean hasLock(String subscriptionId, String subscriberId);

    /**
     * Add a {@link CompetingConsumerListener} to this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to add.
     */
    void addListener(CompetingConsumerListener listenerConsumer);

    /**
     * Remove a {@link CompetingConsumerListener} from this {@code CompetingConsumerStrategy} instance.
     *
     * @param listenerConsumer The listener to remove.
     */
    void removeListener(CompetingConsumerListener listenerConsumer);

    /**
     * Perform some cleanup when shutting down the {@link CompetingConsumerStrategy}.
     */
    default void shutdown() {
    }

    /**
     * A {@code CompetingConsumerListener} will be called when certain life-cycle events occurs.
     */
    interface CompetingConsumerListener {

        /**
         * Called when the lock has been granted to the given subscriber for the given subscription. This
         * means that the subscriber has access to consume events.
         *
         * @param subscriptionId The subscription id
         * @param subscriberId   The subscriber id
         */
        default void onConsumeGranted(String subscriptionId, String subscriberId) {
        }

        /**
         * Called when the lock is no longer available for the given subscriber for the given subscription. This
         * means that the subscriber no longer has access to consume events.
         *
         * @param subscriptionId The subscription id
         * @param subscriberId   The subscriber id
         */
        default void onConsumeProhibited(String subscriptionId, String subscriberId) {
        }
    }
}