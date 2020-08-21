package se.haleby.occurrent.subscription.api.blocking;

import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionPosition;


/**
 * A {@code ReactorSubscriptionPositionStorage} provides means to read and write the subscription position to storage.
 * This subscriptions can continue where they left off by passing the {@link SubscriptionPosition} provided by {@link #read(String)}
 * to a {@link PositionAwareBlockingSubscription} when the application is restarted etc.
 */
public interface BlockingSubscriptionPositionStorage {

    /**
     * Read the raw subscription position for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global subscription position
     * (using {@link PositionAwareBlockingSubscription#globalSubscriptionPosition()}) if no {@code SubscriptionPosition} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * SubscriptionPosition subscriptionPosition = storage.read(subscriptionId);
     * if (subscriptionPosition == null) {
     *      subscriptionPosition = positionAwareReactorSubscription.globalSubscriptionPosition();
     *      storage.save(subscriptionId, subscriptionPosition);
     * }
     * StartAt startAt = StartAt.subscriptionPosition(subscriptionPosition);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    SubscriptionPosition read(String subscriptionId);

    /*
     * Save the subscription position for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition);


    /**
     * Delete the {@link SubscriptionPosition} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link SubscriptionPosition} for.
     */
    void delete(String subscriptionId);
}