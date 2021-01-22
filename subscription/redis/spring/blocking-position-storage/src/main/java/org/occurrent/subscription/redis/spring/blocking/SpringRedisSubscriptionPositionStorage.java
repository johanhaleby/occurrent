/*
 * Copyright 2021 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.redis.spring.blocking;

import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.springframework.data.redis.core.RedisOperations;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.convertToDelayStream;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * A Spring implementation of {@link SubscriptionPositionStorage} that stores {@link SubscriptionPosition} in Redis.
 */
public class SpringRedisSubscriptionPositionStorage implements SubscriptionPositionStorage {

    private final RedisOperations<String, String> redis;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown;

    /**
     * Create a {@link SubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in Redis.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the subscription position.
     *
     * @param redis The {@link RedisOperations} that'll be used to store the subscription position
     */
    public SpringRedisSubscriptionPositionStorage(RedisOperations<String, String> redis) {
        this(redis, RetryStrategy.backoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a {@link SubscriptionPositionStorage} that uses the Native sync Java MongoDB driver to persists the subscription position in Redis.
     *
     * @param redis         The {@link RedisOperations} that'll be used to store the subscription position
     * @param retryStrategy A custom retry strategy to use if there's a problem reading/saving/deleting the position to the Redis storage.
     */
    public SpringRedisSubscriptionPositionStorage(RedisOperations<String, String> redis, RetryStrategy retryStrategy) {
        requireNonNull(redis, "Redis operations cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.retryStrategy = retryStrategy;
        this.redis = redis;
    }

    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Supplier<SubscriptionPosition> read = () -> {
            String subscriptionPosition = redis.opsForValue().get(subscriptionId);
            if (subscriptionPosition == null) {
                return null;
            }
            return new StringBasedSubscriptionPosition(subscriptionPosition);
        };

        return executeWithRetry(read, __ -> !shutdown, convertToDelayStream(retryStrategy)).get();
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");

        Supplier<SubscriptionPosition> save = () -> {
            String changeStreamPositionAsString = subscriptionPosition.asString();
            redis.opsForValue().set(subscriptionId, changeStreamPositionAsString);
            return subscriptionPosition;
        };

        return executeWithRetry(save, __ -> !shutdown, convertToDelayStream(retryStrategy)).get();
    }

    @Override
    public void delete(String subscriptionId) {
        executeWithRetry(() -> redis.delete(subscriptionId), __ -> !shutdown, convertToDelayStream(retryStrategy)).get();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> {
            Boolean result = redis.hasKey(subscriptionId);
            return result != null && result;
        };
        return executeWithRetry(exists, __ -> !shutdown, convertToDelayStream(retryStrategy)).get();
    }

    @PreDestroy
    void shutdown() {
        this.shutdown = true;
    }
}