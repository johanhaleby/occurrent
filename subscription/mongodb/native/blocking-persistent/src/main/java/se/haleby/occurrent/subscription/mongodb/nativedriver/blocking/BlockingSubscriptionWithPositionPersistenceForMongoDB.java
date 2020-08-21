package se.haleby.occurrent.subscription.mongodb.nativedriver.blocking;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.Subscription;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link BlockingSubscriptionForMongoDB} and adds persistent subscription position support. It stores the subscription position
 * after an "action" (the consumer in this method {@link BlockingSubscriptionForMongoDB#subscribe(String, Consumer)}) has completed successfully.
 * It stores the subscription position in MongoDB. Note that it doesn't have to be the same MongoDB database that stores the actual events.
 * <p>
 * Note that this implementation stores the subscription position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class BlockingSubscriptionWithPositionPersistenceForMongoDB implements BlockingSubscription<CloudEvent> {

    private final PositionAwareBlockingSubscription subscription;
    private final BlockingSubscriptionPositionStorage storage;

    /**
     * Create a subscription that uses the Native sync Java MongoDB driver to persists the subscription position in MongoDB.
     *
     * @param subscription The subscription that will read events from the event store
     * @param storage      The storage that holds the subscription positions
     */
    public BlockingSubscriptionWithPositionPersistenceForMongoDB(PositionAwareBlockingSubscription subscription, BlockingSubscriptionPositionStorage storage) {
        requireNonNull(subscription, "subscription cannot be null");
        requireNonNull(storage, BlockingSubscriptionPositionStorage.class.getSimpleName() + " cannot be null");
        this.storage = storage;
        this.subscription = subscription;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        return subscription.subscribe(subscriptionId,
                filter, startAtSupplier, cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    storage.save(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                }
        );
    }

    @Override
    public Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, (SubscriptionFilter) null, action);
    }

    /**
     * Start streaming cloud events from the event store and persist the subscription position in MongoDB
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @return The subscription
     */
    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            SubscriptionPosition subscriptionPosition = storage.read(subscriptionId);
            if (subscriptionPosition == null) {
                subscriptionPosition = storage.save(subscriptionId, subscription.globalSubscriptionPosition());
            }
            return StartAt.subscriptionPosition(subscriptionPosition);
        };

        return subscribe(subscriptionId, filter, startAtSupplier, action);
    }

    void pauseSubscription(String subscriptionId) {
        subscription.cancelSubscription(subscriptionId);
    }

    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        storage.delete(subscriptionId);
    }

    public void shutdown() {
        subscription.shutdown();
    }
}