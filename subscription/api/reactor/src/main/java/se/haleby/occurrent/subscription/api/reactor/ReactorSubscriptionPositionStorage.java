package se.haleby.occurrent.subscription.api.reactor;

import reactor.core.publisher.Mono;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionPosition;


/**
 * A {@code ReactorSubscriptionPositionStorage} provides means to read and write the subscription position to storage.
 * This subscriptions can continue where they left off by passing the {@link SubscriptionPosition} provided by {@link #read(String)}
 * to a {@link PositionAwareReactorSubscription} when the application is restarted etc.
 */
public interface ReactorSubscriptionPositionStorage {

    /**
     * Read the raw subscription position for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from theglobal subscription position
     * (using {@link PositionAwareReactorSubscription#globalSubscriptionPosition()}) if no {@code SubscriptionPosition} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * StartAt startAt = storage.read(subscriptionId)
     *                          .switchIfEmpty(Mono.defer(() -> positionAwareReactorSubscription.globalSubscriptionPosition().flatMap(streamPosition -> storage.write(subscriptionId, streamPosition))))
     *                          .map(StartAt::streamPosition);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    Mono<SubscriptionPosition> read(String subscriptionId);

    /*
     * Write subscription position for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    Mono<SubscriptionPosition> write(String subscriptionId, SubscriptionPosition subscriptionPosition);


    /**
     * Delete the {@link SubscriptionPosition} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link SubscriptionPosition} for.
     */
    Mono<Void> delete(String subscriptionId);
}