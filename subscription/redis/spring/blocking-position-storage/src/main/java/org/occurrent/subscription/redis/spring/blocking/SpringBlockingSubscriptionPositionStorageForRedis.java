/*
 * Copyright 2020 Johan Haleby
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