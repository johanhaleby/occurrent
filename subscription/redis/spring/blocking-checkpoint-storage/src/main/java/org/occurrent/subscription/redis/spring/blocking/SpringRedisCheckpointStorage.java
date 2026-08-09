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

import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.springframework.data.redis.core.RedisOperations;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in Redis.
 */
@NullMarked
public class SpringRedisCheckpointStorage implements CheckpointStorage {

    private final RedisOperations<String, String> redis;
    private final RetryStrategy retryStrategy;

    private volatile boolean shutdown;

    /**
     * Create a {@link CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in Redis.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the checkpoint.
     *
     * @param redis The {@link RedisOperations} that'll be used to store the checkpoint
     */
    public SpringRedisCheckpointStorage(RedisOperations<String, String> redis) {
        this(redis, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a {@link CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in Redis.
     *
     * @param redis         The {@link RedisOperations} that'll be used to store the checkpoint
     * @param retryStrategy A custom retry strategy to use if there's a problem reading/saving/deleting the checkpoint to the Redis storage.
     */
    public SpringRedisCheckpointStorage(RedisOperations<String, String> redis, RetryStrategy retryStrategy) {
        requireNonNull(redis, "Redis operations cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.retryStrategy = retryStrategy;
        this.redis = redis;
    }

    @Nullable
    @Override
    public Checkpoint read(String subscriptionId) {
        Supplier<@Nullable Checkpoint> read = () -> {
            String checkpoint = redis.opsForValue().get(subscriptionId);
            if (checkpoint == null) {
                return null;
            }
            return new StringBasedCheckpoint(checkpoint);
        };

        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        // This storage does not evaluate a condition yet, so only the unconditional one is honoured.
        if (!(condition instanceof CheckpointWriteCondition.Any)) {
            throw new UnsupportedOperationException(
                    SpringRedisCheckpointStorage.class.getSimpleName() + " cannot evaluate " + condition + " yet, only "
                            + CheckpointWriteCondition.any() + " is supported.");
        }

        Supplier<Checkpoint> save = () -> {
            String changeStreamPositionAsString = checkpoint.asString();
            redis.opsForValue().set(subscriptionId, changeStreamPositionAsString);
            return checkpoint;
        };

        return requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        // No version is recorded yet, since this storage does not evaluate a condition. See save(..).
        return OptionalLong.empty();
    }

    @Override
    public void delete(String subscriptionId) {
        executeWithRetry(() -> redis.delete(subscriptionId), __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> {
            Boolean result = redis.hasKey(subscriptionId);
            return result != null && result;
        };
        return Boolean.TRUE.equals(executeWithRetry(exists, __ -> !shutdown, retryStrategy).get());
    }

    @PreDestroy
    void shutdown() {
        this.shutdown = true;
    }
}