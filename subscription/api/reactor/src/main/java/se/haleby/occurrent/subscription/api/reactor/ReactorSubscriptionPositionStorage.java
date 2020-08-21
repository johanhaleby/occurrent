package se.haleby.occurrent.subscription.api.reactor;

import reactor.core.publisher.Mono;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionPosition;


/**
 * A {@code ReactorSubscriptionPositionStorage} provides means to read and write
 */
public interface ReactorSubscriptionPositionStorage {

    /**
     * Find the calculate the current {@link StartAt} value for the {@code subscriptionId}. It creates the {@link StartAt} instance from the
     * global subscription position (from {@link PositionAwareReactorSubscription#globalSubscriptionPosition()}) if no
     * {@code SubscriptionPosition} is found for the given subscription.
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    Mono<StartAt> findStartAtForSubscription(String subscriptionId);

    /**
     * Read the raw subscription position for a given subscription. Typically you want to use {@link #findStartAtForSubscription(String)} since it automatically
     * creates {@link StartAt} from the global subscription position (from {@link PositionAwareReactorSubscription#globalSubscriptionPosition()})
     * if no {@code SubscriptionPosition} is found for the given subscription.
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    Mono<SubscriptionPosition> read(String subscriptionId);

    /*
     * Write subscription position for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    Mono<SubscriptionPosition> write(String subscriptionId, SubscriptionPosition subscriptionPosition);
}
