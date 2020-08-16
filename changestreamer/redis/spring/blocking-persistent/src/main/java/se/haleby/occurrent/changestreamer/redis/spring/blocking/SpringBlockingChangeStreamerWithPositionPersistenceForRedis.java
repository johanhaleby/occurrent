package se.haleby.occurrent.changestreamer.redis.spring.blocking;

import io.cloudevents.CloudEvent;
import org.springframework.data.redis.core.RedisOperations;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.StringBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.api.blocking.BlockingChangeStreamer;
import se.haleby.occurrent.changestreamer.api.blocking.Subscription;

import javax.annotation.PreDestroy;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link BlockingChangeStreamer} and adds persistent stream position support. It stores the stream position
 * after an "action" (the consumer in this method {@link SpringBlockingChangeStreamerWithPositionPersistenceForRedis#stream(String, Consumer)}) has completed successfully.
 * It stores the stream position in Redis, one value for each subscription.
 * <p>
 * Note that this implementation stores the stream position after _every_ action. If you have a lot of events and duplication is not
 * that much of a deal consider cloning/extending this class and add your own customizations.
 */
public class SpringBlockingChangeStreamerWithPositionPersistenceForRedis {

    private final RedisOperations<String, String> redis;
    private final BlockingChangeStreamer changeStreamer;

    /**
     * Create a change streamer that uses the Native sync Java MongoDB driver to persists the stream position in MongoDB.
     *
     * @param changeStreamer The change streamer that will read events from the event store
     * @param redis          The {@link RedisOperations} that'll be used to store the stream position
     */
    public SpringBlockingChangeStreamerWithPositionPersistenceForRedis(BlockingChangeStreamer changeStreamer, RedisOperations<String, String> redis) {
        requireNonNull(changeStreamer, "changeStreamer cannot be null");
        requireNonNull(redis, "Redis operations cannot be null");
        this.changeStreamer = changeStreamer;
        this.redis = redis;
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEvent> action) {
        return stream(subscriptionId, null, action);
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     */
    public Subscription stream(String subscriptionId, ChangeStreamFilter filter, Consumer<CloudEvent> action) {
        Supplier<StartAt> startAtSupplier = () -> {
            // It's important that we find the document inside the supplier so that we lookup the latest resume token on retry
            String changeStreamPosition = redis.opsForValue().get(subscriptionId);
            if (changeStreamPosition == null) {
                changeStreamPosition = persistChangeStreamPosition(subscriptionId, changeStreamer.globalChangeStreamPosition());
            }
            return StartAt.streamPosition(new StringBasedChangeStreamPosition(changeStreamPosition));
        };

        return changeStreamer.stream(subscriptionId,
                filter, startAtSupplier, cloudEventWithStreamPosition -> {
                    action.accept(cloudEventWithStreamPosition);
                    persistChangeStreamPosition(subscriptionId, cloudEventWithStreamPosition.getStreamPosition());
                }
        );
    }

    void pauseSubscription(String subscriptionId) {
        changeStreamer.cancelSubscription(subscriptionId);
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


    private String persistChangeStreamPosition(String subscriptionId, ChangeStreamPosition changeStreamPosition) {
        String changeStreamPositionAsString = changeStreamPosition.asString();
        redis.opsForValue().set(subscriptionId, changeStreamPositionAsString);
        return changeStreamPositionAsString;
    }


    @PreDestroy
    public void shutdownSubscribers() {
        changeStreamer.shutdown();
    }
}