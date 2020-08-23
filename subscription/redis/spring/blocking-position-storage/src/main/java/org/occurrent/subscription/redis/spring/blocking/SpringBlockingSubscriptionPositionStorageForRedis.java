package org.occurrent.subscription.redis.spring.blocking;

import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.springframework.data.redis.core.RedisOperations;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;

import static java.util.Objects.requireNonNull;

/**
 * A Spring implementation of {@link BlockingSubscriptionPositionStorage} that stores {@link SubscriptionPosition} in Redis.
 */
public class SpringBlockingSubscriptionPositionStorageForRedis implements BlockingSubscriptionPositionStorage {

    private final RedisOperations<String, String> redis;

    /**
     * Create a {@link BlockingSubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in Redis.
     *
     * @param redis The {@link RedisOperations} that'll be used to store the subscription position
     */
    public SpringBlockingSubscriptionPositionStorageForRedis(RedisOperations<String, String> redis) {
        requireNonNull(redis, "Redis operations cannot be null");
        this.redis = redis;
    }

    @Override
    public SubscriptionPosition read(String subscriptionId) {
        String subscriptionPosition = redis.opsForValue().get(subscriptionId);
        if (subscriptionPosition == null) {
            return null;
        }
        return new StringBasedSubscriptionPosition(subscriptionPosition);
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");
        String changeStreamPositionAsString = subscriptionPosition.asString();
        redis.opsForValue().set(subscriptionId, changeStreamPositionAsString);
        return subscriptionPosition;
    }

    @Override
    public void delete(String subscriptionId) {
        redis.delete(subscriptionId);
    }
}