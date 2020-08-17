package se.haleby.occurrent.subscription.redis.spring.blocking;

import io.cloudevents.CloudEvent;
import org.springframework.data.redis.core.RedisOperations;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.StringBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.Subscription;

import javax.annotation.PreDestroy;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link BlockingSubscription} and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link SpringBlockingSubscriptionWithPositionPersistenceForRedis#subscribe(String, Consumer)}) has completed successfully.
 * It stores the stream position in Redis, one value for each subscription.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringBlockingSubscriptionWithPositionPersistenceForRedis implements BlockingSubscription<CloudEvent> {

    private final RedisOperations<String, String> redis;
    private final PositionAwareBlockingSubscription subscription;

    /**
     * Create a subscription that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param subscription The subscription that will read events from the event store
     * @param redis          The {@link RedisOperations} that'll be used to store the stream position
     */
    public SpringBlockingSubscriptionWithPositionPersistenceForRedis(PositionAwareBlockingSubscription subscription, RedisOperations<String, String> redis) {
        requireNonNull(subscription, "subscription cannot be null");
        requireNonNull(redis, "Redis operations cannot be null");
        this.subscription = subscription;
        this.redis = redis;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        return subscription.subscribe(subscriptionId,
                filter, startAtSupplier, cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistSubscriptionPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                }
        );
    }

    @Override
    public Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, (SubscriptionFilter) null, action);
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     */
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Consumer<CloudEvent> action) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            String changeStreamPosition = redis.opsForValue().get(subscriptionId);
            if (changeStreamPosition == null) {
                changeStreamPosition = persistSubscriptionPosition(subscriptionId, subscription.globalSubscriptionPosition());
            }
            return StartAt.streamPosition(new StringBasedSubscriptionPosition(changeStreamPosition));
        };
        return subscribe(subscriptionId, filter, startAtSupplier, action);
    }

    void pauseSubscription(String subscriptionId) {
        subscription.cancelSubscription(subscriptionId);
    }

    /**
     * Cancel a subscription. This means that it'll no longer receive events as they are persisted to the event store.
     * The stream position that is persisted to MongoDB will also be removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    public void cancelSubscription(String subscriptionId) {
        pauseSubscription(subscriptionId);
        redis.delete(subscriptionId);
    }


    private String persistSubscriptionPosition(String subscriptionId, SubscriptionPosition changeStreamPosition) {
        String changeStreamPositionAsString = changeStreamPosition.asString();
        redis.opsForValue().set(subscriptionId, changeStreamPositionAsString);
        return changeStreamPositionAsString;
    }


    @PreDestroy
    public void shutdownSubscribers() {
        subscription.shutdown();
    }
}