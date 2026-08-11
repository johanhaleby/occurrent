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
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * A Spring implementation of {@link CheckpointStorage} that stores {@link Checkpoint} in Redis.
 * <p>
 * The checkpoint itself is an unprefixed, plain string value at a key named after the subscription id, exactly as it
 * has been since this storage shipped. A {@link CheckpointWriteCondition} needs somewhere to remember the version it
 * was judged against, and that goes in a second key, prefixed to keep it out of the checkpoint's own namespace. A
 * node still running a release before this one only ever does a plain {@code GET} against the first key, and that
 * value has not changed shape, so a rolling deploy stays safe.
 * <p>
 * {@link CheckpointWriteCondition#notOlderThan(long)} and {@link CheckpointWriteCondition#ifAbsent()} are evaluated
 * by a Lua script that compares the stored version and writes both keys in one round trip, so no other writer can
 * land between the comparison and the write. {@link CheckpointWriteCondition#any()} needs no comparison and keeps
 * writing through {@code opsForValue().set}, exactly as before, leaving the version key untouched.
 * <p>
 * <strong>Redis Cluster.</strong> The checkpoint key and the version key are not guaranteed to hash to the same
 * slot, and Cluster refuses a script that touches keys in different slots. A caller that never passes a condition
 * other than {@link CheckpointWriteCondition#any()} is unaffected, since that path never runs the script. A caller
 * that does is refused immediately, on the first conditional write, with the error Cluster itself reports for
 * crossing slots.
 */
@NullMarked
public class SpringRedisCheckpointStorage implements CheckpointStorage {

    private static final String VERSION_KEY_PREFIX = "occurrent:checkpoint-version:";

    /**
     * Returned by the write scripts below when the write went through.
     */
    private static final long WRITE_SUCCEEDED = -1L;

    /**
     * Returned by the write and read scripts below when no version is stored for the subscription id, which is
     * distinct from {@link #WRITE_SUCCEEDED} and from every version this storage ever writes, since a write version
     * is always the non-negative fencing token a competing-consumer strategy hands the caller.
     */
    private static final long NO_VERSION_STORED = -2L;

    // KEYS[1] = checkpoint key, KEYS[2] = version key. ARGV[1] = checkpoint value, ARGV[2] = write version as
    // plain decimal digits. Nothing is stored is accepted, since that is a checkpoint written before this
    // condition existed. Otherwise the write is accepted when the stored version is not greater than the one
    // offered, and refused with the stored version otherwise.
    private static final RedisScript<Long> NOT_OLDER_THAN_SCRIPT = RedisScript.of("""
            local storedVersionRaw = redis.call('GET', KEYS[2])
            if storedVersionRaw then
                local storedVersion = tonumber(storedVersionRaw)
                local writeVersion = tonumber(ARGV[2])
                if storedVersion > writeVersion then
                    return storedVersion
                end
            end
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('SET', KEYS[2], ARGV[2])
            return -1
            """, Long.class);

    // KEYS[1] = checkpoint key, KEYS[2] = version key. ARGV[1] = checkpoint value. Accepted only when the
    // checkpoint key does not exist yet, whatever version would be stored. The version key is left untouched
    // either way, since ifAbsent is about whether a checkpoint is stored, not a version.
    private static final RedisScript<Long> IF_ABSENT_SCRIPT = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 1 then
                local storedVersionRaw = redis.call('GET', KEYS[2])
                if storedVersionRaw then
                    return tonumber(storedVersionRaw)
                end
                return -2
            end
            redis.call('SET', KEYS[1], ARGV[1])
            return -1
            """, Long.class);

    // KEYS[1] = version key. No ARGV.
    private static final RedisScript<Long> READ_VERSION_SCRIPT = RedisScript.of("""
            local storedVersionRaw = redis.call('GET', KEYS[1])
            if storedVersionRaw then
                return tonumber(storedVersionRaw)
            end
            return -2
            """, Long.class);

    // The three scripts above only ever return a Long, and this is never asked to deserialize one, since a Long
    // reply comes back from the driver as a Long already. It exists because the execute overload that takes
    // explicit serializers demands one of the right type.
    private static final RedisSerializer<Long> RESULT_SERIALIZER = new RedisSerializer<Long>() {
        @Override
        public byte @Nullable [] serialize(@Nullable Long value) {
            return value == null ? null : Long.toString(value).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public @Nullable Long deserialize(byte @Nullable [] bytes) {
            return bytes == null ? null : Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
        }
    };

    private final RedisOperations<String, String> redis;
    private final RetryStrategy retryStrategy;

    // Encodes a script's ARGV. A checkpoint value is handed to the RedisOperations' own value serializer, the same
    // one opsForValue().set(..) uses, since a byte-identical encoding there is what keeps a plain GET reading it
    // correctly. Everything else this storage passes as a script argument is already-encoded plain digits, handed
    // through unchanged, since the script parses them with Lua's tonumber and nothing requires the value serializer
    // to produce a decimal string.
    private final RedisSerializer<Object> conditionArgsSerializer;

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
        this.conditionArgsSerializer = conditionArgsSerializer(redis);
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
        return switch (condition) {
            case CheckpointWriteCondition.Any ignored -> saveUnconditionally(subscriptionId, checkpoint);
            case CheckpointWriteCondition.NotOlderThan notOlderThan -> saveConditionally(subscriptionId, checkpoint, condition, NOT_OLDER_THAN_SCRIPT,
                    Long.toString(notOlderThan.writeVersion()).getBytes(StandardCharsets.UTF_8));
            case CheckpointWriteCondition.IfAbsent ignored -> saveConditionally(subscriptionId, checkpoint, condition, IF_ABSENT_SCRIPT);
        };
    }

    private Checkpoint saveUnconditionally(String subscriptionId, Checkpoint checkpoint) {
        Supplier<Checkpoint> save = () -> {
            redis.opsForValue().set(subscriptionId, checkpoint.asString());
            return checkpoint;
        };
        return requireNonNull(executeWithRetry(save, __ -> !shutdown, retryStrategy).get());
    }

    /**
     * Runs a write script and turns its return code into either the saved checkpoint or a refusal.
     * <p>
     * The refusal is excluded from the retry strategy's own predicate, not merely allowed to exhaust it. This
     * storage's default {@link RetryStrategy} is {@code exponentialBackoff}, whose default max attempts is infinite,
     * so a refusal left inside the ordinary retry path would retry a write that can never succeed and hang the
     * delivery thread forever. Excluding it is what {@link CheckpointWriteConditionNotFulfilledException}'s javadoc
     * means by "must never be retried on the path that threw it".
     */
    private Checkpoint saveConditionally(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition, RedisScript<Long> script, byte[]... extraArgs) {
        Supplier<Checkpoint> save = () -> {
            Object[] args = new Object[1 + extraArgs.length];
            args[0] = checkpoint.asString();
            System.arraycopy(extraArgs, 0, args, 1, extraArgs.length);

            long outcome = requireNonNull(redis.execute(script, conditionArgsSerializer, RESULT_SERIALIZER,
                    List.of(subscriptionId, versionKey(subscriptionId)), args));
            if (outcome == WRITE_SUCCEEDED) {
                return checkpoint;
            }
            OptionalLong storedVersion = outcome == NO_VERSION_STORED ? OptionalLong.empty() : OptionalLong.of(outcome);
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, storedVersion, condition);
        };

        Predicate<Throwable> retryUnlessShutdownOrRefused = e -> !shutdown && !(e instanceof CheckpointWriteConditionNotFulfilledException);
        return requireNonNull(executeWithRetry(save, retryUnlessShutdownOrRefused, retryStrategy).get());
    }

    // True because the comparison is real on a standalone or replicated server, which is where this storage is
    // supported. On Cluster the script is refused for crossing slots, and nothing here can tell the two deployments
    // apart without a round trip to the server.
    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        Supplier<@Nullable Long> read = () -> redis.execute(READ_VERSION_SCRIPT, RedisSerializer.string(), RESULT_SERIALIZER,
                List.of(versionKey(subscriptionId)));
        Long storedVersion = executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
        return storedVersion == null || storedVersion == NO_VERSION_STORED ? OptionalLong.empty() : OptionalLong.of(storedVersion);
    }

    @Override
    public void delete(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        Supplier<Long> deleteBoth = () -> redis.delete(List.of(subscriptionId, versionKey(subscriptionId)));
        executeWithRetry(deleteBoth, __ -> !shutdown, retryStrategy).get();
    }

    @Override
    public boolean exists(String subscriptionId) {
        Supplier<Boolean> exists = () -> {
            Boolean result = redis.hasKey(subscriptionId);
            return result != null && result;
        };
        return Boolean.TRUE.equals(executeWithRetry(exists, __ -> !shutdown, retryStrategy).get());
    }

    private static String versionKey(String subscriptionId) {
        return VERSION_KEY_PREFIX + subscriptionId;
    }

    @SuppressWarnings("unchecked")
    private static RedisSerializer<Object> conditionArgsSerializer(RedisOperations<String, String> redis) {
        RedisSerializer<Object> checkpointValueSerializer = (RedisSerializer<Object>) redis.getValueSerializer();
        return new RedisSerializer<Object>() {
            @Override
            public byte @Nullable [] serialize(@Nullable Object value) {
                // A write version arrives pre-encoded as plain decimal digits, passed through unchanged so the
                // script's tonumber can read it. Anything else is a checkpoint value, encoded the same way
                // opsForValue().set(..) would encode it.
                return value instanceof byte[] alreadyEncoded ? alreadyEncoded : checkpointValueSerializer.serialize(value);
            }

            @Override
            public Object deserialize(byte @Nullable [] bytes) {
                throw new UnsupportedOperationException(getClass().getName() + " only encodes script arguments, it never decodes a script result.");
            }
        };
    }

    @PreDestroy
    void shutdown() {
        this.shutdown = true;
    }
}
