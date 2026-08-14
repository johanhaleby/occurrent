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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
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
 * Because the checkpoint key is a caller-chosen subscription id with no prefix of its own, a subscription could in
 * principle choose an id equal to the exact text of some other subscription's version key, and land its own
 * checkpoint on that other subscription's stored version. {@link #read(String)}, {@link #save(String, Checkpoint,
 * CheckpointWriteCondition)}, {@link #delete(String)}, and {@link #exists(String)} all refuse a subscription id
 * that starts with the version key's own prefix, which every version key does and no id a real caller would pick
 * does by accident, closing that off entirely rather than leaving it as a documented risk.
 * <p>
 * {@link CheckpointWriteCondition#notOlderThan(long)} and {@link CheckpointWriteCondition#ifAbsent()} are evaluated
 * by a Lua script that compares the stored version and writes both keys in one round trip, so no other writer can
 * land between the comparison and the write. {@link CheckpointWriteCondition#any()} needs no comparison and keeps
 * writing through {@code opsForValue().set}, exactly as before, leaving the version key untouched.
 * <p>
 * <strong>Redis Cluster.</strong> The version key carries a hash tag built from whatever the checkpoint key itself
 * hashes on, so Cluster places both keys in the same slot and the scripts above are never refused for crossing
 * slots. The two-key {@code DEL} in {@link #delete(String)} is not covered by that same guarantee. It still refuses
 * a subscription id in the version key's own namespace, same as every method above, but not the one shape below a
 * conditional {@link #save(String, Checkpoint, CheckpointWriteCondition) save} refuses for slot alignment, so it
 * falls back to two single-key deletes when Cluster refuses that shape's two-key {@code DEL} for crossing slots.
 * A subscription id with no braces of its own hashes
 * on its full text either way, and one that already contains a matched, non-empty pair hashes on the text between
 * them, the same substring Cluster would use for the checkpoint key. A SHA-256 digest of the full subscription id
 * follows the tag, outside its braces where Cluster never looks once it has found the closing one, so two ids that
 * happen to share a hash tag, two ids tenant-scoped under the same {@code "{tenant}"} for instance, still get
 * distinct version keys instead of silently sharing one fencing version. The digest, not a raw or delimited copy of
 * the id, is what makes that collision-resistant. The tag can equal the whole subscription id (see the first shape
 * below), so a raw copy sitting next to it lets one id's own text be misread as a different id's tag plus copy.
 * Two earlier constructions built this way, a plain separator and a length prefix, both broke on exactly that
 * doubling during review.
 * <p>
 * One shape this cannot help is a subscription id where Cluster itself falls back to hashing the whole id (no brace
 * pair, an unmatched brace, or an empty pair like {@code {}}) and that whole id is either empty or contains a
 * closing brace somewhere in it, for example {@code ""}, {@code "{}orders"} or {@code "a}b{c"}. Wrapping such text
 * in a fresh hash tag only reproduces it when the text has no closing brace of its own, since Cluster stops at the
 * first one it finds, and that is then the wrap's own or an earlier one already inside the id, not the one this
 * class appended. This library's own tests use ids of that shape deliberately, to exercise the refusal below, but
 * no subscription id a real caller would choose takes it by accident.
 * <p>
 * <strong>Two modes.</strong> The constructors below build the Cluster-safe mode this class has always had, where
 * {@link #save(String, Checkpoint, CheckpointWriteCondition)} refuses an id of that shape outright, with an
 * {@link IllegalArgumentException} naming the reason, whenever the condition is {@link CheckpointWriteCondition#notOlderThan(long)}
 * or {@link CheckpointWriteCondition#ifAbsent()}. That refusal is what keeps {@link #evaluatesWriteConditions()}
 * true without exception in that mode, rather than true for every id except the one shape Cluster would otherwise
 * refuse two calls downstream, and {@link #evaluatesWriteConditionsFor(String)} answers {@code false} for exactly
 * that shape and {@code true} for every other id. {@link #forStandalone(RedisOperations)} builds the other mode
 * instead, for a deployment that is standalone or replicated rather than Cluster, where slot alignment is not a
 * concept a server has, so nothing needs protecting from it. There, a conditional write accepts every subscription
 * id, including the shape the Cluster-safe mode refuses, and {@link #evaluatesWriteConditionsFor(String)} answers
 * {@code true} for every id. {@link CheckpointWriteCondition#any()} never refuses one in either mode, since it
 * writes only the checkpoint key and never touches the version key at all.
 * <p>
 * {@link #delete(String)} never refuses one. A subscription id of this shape can only ever have had a checkpoint
 * written for it through {@code any()}, since a conditional write already refuses one before touching Redis at
 * all, so its version key can never exist to strand. On a {@code CROSSSLOT} failure, falling back to two
 * single-key deletes is provably safe for that reason, not merely convenient. The checkpoint key is deleted first
 * regardless, a defensive ordering rather than one this specific fallback depends on. If a version key ever did
 * exist here, deleting it first and failing before the checkpoint would leave a checkpoint with no stored version,
 * which a later {@code notOlderThan} write would then accept unconditionally, letting a lease-holder that has
 * already moved on win a write it should have lost.
 * <p>
 * This also assumes the {@link RedisOperations} passed in serializes a key to its own literal bytes, the same
 * assumption the checkpoint's plain {@code GET} already makes. A key serializer that reshapes the string changes
 * what Cluster actually hashes, and nothing in this class can see that reshaping to compensate for it.
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

    // false for every constructor below, so an existing caller keeps the Cluster-safe id-shape restriction it always
    // had. true only for an instance forStandalone builds, where that restriction is lifted since a standalone or
    // replicated server has no slot to protect it from.
    private final boolean standalone;

    private volatile boolean shutdown;

    /**
     * Create a {@link CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in Redis.
     * It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms and progressively go up to max 2 seconds wait time between
     * each retry when reading/saving/deleting the checkpoint.
     * <p>
     * Cluster-safe. A conditional write refuses the one subscription id shape the class javadoc names, whether or
     * not this instance ever runs against Cluster. Use {@link #forStandalone(RedisOperations)} instead for a
     * standalone or replicated deployment that needs a conditional write to accept every id.
     *
     * @param redis The {@link RedisOperations} that'll be used to store the checkpoint
     */
    public SpringRedisCheckpointStorage(RedisOperations<String, String> redis) {
        this(redis, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f));
    }

    /**
     * Create a {@link CheckpointStorage} that uses the Native sync Java MongoDB driver to persists the checkpoint in Redis.
     * <p>
     * Cluster-safe. A conditional write refuses the one subscription id shape the class javadoc names, whether or
     * not this instance ever runs against Cluster. Use {@link #forStandalone(RedisOperations, RetryStrategy)}
     * instead for a standalone or replicated deployment that needs a conditional write to accept every id.
     *
     * @param redis         The {@link RedisOperations} that'll be used to store the checkpoint
     * @param retryStrategy A custom retry strategy to use if there's a problem reading/saving/deleting the checkpoint to the Redis storage.
     */
    public SpringRedisCheckpointStorage(RedisOperations<String, String> redis, RetryStrategy retryStrategy) {
        this(redis, retryStrategy, false);
    }

    /**
     * Create a {@link CheckpointStorage} for a standalone or replicated Redis deployment, where a conditional write
     * accepts every subscription id, including the one shape the class javadoc names Cluster cannot align a slot
     * for. It will by default use a {@link RetryStrategy} for retries, with exponential backoff starting with 100 ms
     * and progressively go up to max 2 seconds wait time between each retry when reading/saving/deleting the
     * checkpoint.
     * <p>
     * Do not use this against a Cluster deployment. Nothing here can tell one apart from a standalone or replicated
     * server, and a conditional write for one of the ids this mode accepts fails against Cluster with a
     * {@code CROSSSLOT} error, from Redis itself rather than from this class, instead of the refusal the class
     * javadoc describes for the other constructors.
     *
     * @param redis The {@link RedisOperations} that'll be used to store the checkpoint
     * @return A {@link CheckpointStorage} that accepts every subscription id for a conditional write
     */
    public static SpringRedisCheckpointStorage forStandalone(RedisOperations<String, String> redis) {
        return new SpringRedisCheckpointStorage(redis, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), true);
    }

    /**
     * Create a {@link CheckpointStorage} for a standalone or replicated Redis deployment, where a conditional write
     * accepts every subscription id, including the one shape the class javadoc names Cluster cannot align a slot
     * for.
     * <p>
     * Do not use this against a Cluster deployment. Nothing here can tell one apart from a standalone or replicated
     * server, and a conditional write for one of the ids this mode accepts fails against Cluster with a
     * {@code CROSSSLOT} error, from Redis itself rather than from this class, instead of the refusal the class
     * javadoc describes for the other constructors.
     *
     * @param redis         The {@link RedisOperations} that'll be used to store the checkpoint
     * @param retryStrategy A custom retry strategy to use if there's a problem reading/saving/deleting the checkpoint to the Redis storage.
     * @return A {@link CheckpointStorage} that accepts every subscription id for a conditional write
     */
    public static SpringRedisCheckpointStorage forStandalone(RedisOperations<String, String> redis, RetryStrategy retryStrategy) {
        return new SpringRedisCheckpointStorage(redis, retryStrategy, true);
    }

    private SpringRedisCheckpointStorage(RedisOperations<String, String> redis, RetryStrategy retryStrategy, boolean standalone) {
        requireNonNull(redis, "Redis operations cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.retryStrategy = retryStrategy;
        this.redis = redis;
        this.conditionArgsSerializer = conditionArgsSerializer(redis);
        this.standalone = standalone;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code subscriptionId} starts with the prefix this storage reserves for
     *                                   its own version keys, see the class javadoc. Specific to this
     *                                   implementation, not part of the {@link CheckpointStorage} contract.
     */
    @Nullable
    @Override
    public Checkpoint read(String subscriptionId) {
        requireOutsideVersionKeyNamespace(subscriptionId);
        Supplier<@Nullable Checkpoint> read = () -> {
            String checkpoint = redis.opsForValue().get(subscriptionId);
            if (checkpoint == null) {
                return null;
            }
            return new StringBasedCheckpoint(checkpoint);
        };

        return executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code subscriptionId} starts with the prefix this storage reserves for
     *                                  its own version keys, see the class javadoc, or if {@code condition} is
     *                                  {@link CheckpointWriteCondition#notOlderThan(long)} or
     *                                  {@link CheckpointWriteCondition#ifAbsent()} and {@code subscriptionId} is
     *                                  one of the shapes the class javadoc names Redis Cluster cannot align a slot
     *                                  for. Neither is part of the {@link CheckpointStorage} contract, since no
     *                                  other storage this library ships has an analogous reserved namespace or
     *                                  unsupported shape.
     */
    @Override
    public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        requireNonNull(condition, CheckpointWriteCondition.class.getSimpleName() + " cannot be null");
        requireOutsideVersionKeyNamespace(subscriptionId);
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
     * The refusal, and a Cluster {@code CROSSSLOT} failure, are both excluded from the retry strategy's own
     * predicate, not merely allowed to exhaust it. This storage's default {@link RetryStrategy} is
     * {@code exponentialBackoff}, whose default max attempts is infinite, so either one left inside the ordinary
     * retry path would retry a write that can never succeed and hang the delivery thread forever. Excluding the
     * refusal is what {@link CheckpointWriteConditionNotFulfilledException}'s javadoc means by "must never be
     * retried on the path that threw it", and a slot mismatch is the same kind of failure, just reported by Cluster
     * instead of by this class.
     */
    private Checkpoint saveConditionally(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition, RedisScript<Long> script, byte[]... extraArgs) {
        // Standalone mode has no slot to protect, so it skips this restriction entirely rather than narrowing it.
        if (!standalone) {
            requireClusterSlotAlignable(subscriptionId);
        }
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

        Predicate<Throwable> retryUnlessShutdownOrRefused = e -> !shutdown && !(e instanceof CheckpointWriteConditionNotFulfilledException) && !isClusterSlotMismatch(e);
        return requireNonNull(executeWithRetry(save, retryUnlessShutdownOrRefused, retryStrategy).get());
    }

    // The checkpoint key is subscriptionId itself, unprefixed, and every version key starts with VERSION_KEY_PREFIX,
    // so a subscription id that starts with it too could be the exact text of some other subscription's version
    // key. A save, delete, or read against that id would then land on the wrong subscription's stored version
    // instead of its own checkpoint. Refusing the prefix outright is enough to rule that out entirely, since no
    // version key this class ever builds starts with anything else.
    private static void requireOutsideVersionKeyNamespace(String subscriptionId) {
        if (subscriptionId.startsWith(VERSION_KEY_PREFIX)) {
            throw new IllegalArgumentException("Subscription id \"" + subscriptionId + "\" cannot be used, since it starts with \"" + VERSION_KEY_PREFIX + "\", the prefix this storage reserves for its own version keys, and could otherwise be the exact key some other subscription's version is stored under.");
        }
    }

    // False for the one subscription id shape the class javadoc names, where clusterHashTag falls back to the whole
    // id and that fallback text is empty or itself contains a closing brace. True otherwise. Shared by the throwing
    // path below and by evaluatesWriteConditionsFor, so the two can never drift apart on which ids they mean.
    private static boolean isClusterSlotAlignable(String subscriptionId) {
        String tag = clusterHashTag(subscriptionId);
        return !tag.isEmpty() && !tag.contains("}");
    }

    // Refused here, immediately and by name, rather than left to surface as Cluster's own crossed-slots error, which
    // is what makes evaluatesWriteConditions() true without exception in Cluster-safe mode for every subscription id
    // a conditional write actually accepts there. No caller in this library, or a realistic one, needs an id of this
    // shape. Only called in Cluster-safe mode. Standalone mode skips it, see saveConditionally.
    private static void requireClusterSlotAlignable(String subscriptionId) {
        if (!isClusterSlotAlignable(subscriptionId)) {
            throw new IllegalArgumentException("Subscription id \"" + subscriptionId + "\" cannot be used with a conditional write, since Redis Cluster would hash the checkpoint key and this storage's version key for it to different slots and refuse the write for crossing slots.");
        }
    }

    // Cluster reports two script keys in different slots as CROSSSLOT, a deterministic failure no retry turns into
    // a success. Neither Lettuce nor Spring Data Redis gives this its own exception type, so this walks the cause
    // chain for the stable error-code word Redis itself puts at the start of the reply.
    private static boolean isClusterSlotMismatch(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message != null && message.startsWith("CROSSSLOT")) {
                return true;
            }
        }
        return false;
    }

    // True unconditionally, in both modes. The comparison is real on a standalone server, a replicated one, and on
    // Cluster, since versionKey's hash tag keeps both keys the script touches in the same slot for every
    // subscription id a conditional write accepts. In Cluster-safe mode, the one shape it does not accept,
    // requireClusterSlotAlignable refuses outright before either key is touched, so true never quietly stops being
    // true for an id this class lets through there. evaluatesWriteConditionsFor is the precise, per-id answer for
    // that one shape. In standalone mode every id is let through, so the two methods agree everywhere.
    @Override
    public boolean evaluatesWriteConditions() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Answers {@code true} for every subscription id in standalone mode ({@link #forStandalone(RedisOperations)}).
     * In Cluster-safe mode (the other constructors), answers {@code false} for exactly the one subscription id
     * shape the class javadoc names Redis Cluster cannot align a slot for, and {@code true} for every other id.
     */
    @Override
    public boolean evaluatesWriteConditionsFor(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        return standalone || isClusterSlotAlignable(subscriptionId);
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        Supplier<@Nullable Long> read = () -> redis.execute(READ_VERSION_SCRIPT, RedisSerializer.string(), RESULT_SERIALIZER,
                List.of(versionKey(subscriptionId)));
        Long storedVersion = executeWithRetry(read, __ -> !shutdown, retryStrategy).get();
        return storedVersion == null || storedVersion == NO_VERSION_STORED ? OptionalLong.empty() : OptionalLong.of(storedVersion);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Unlike {@link #save(String, Checkpoint, CheckpointWriteCondition)}, this never throws for a subscription id
     * shape Redis Cluster cannot align a slot for, see the class javadoc for why deleting one is always safe.
     *
     * @throws IllegalArgumentException if {@code subscriptionId} starts with the prefix this storage reserves for
     *                                  its own version keys, see the class javadoc. Specific to this
     *                                  implementation, not part of the {@link CheckpointStorage} contract.
     */
    @Override
    public void delete(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireOutsideVersionKeyNamespace(subscriptionId);
        Supplier<Long> deleteBoth = () -> {
            try {
                return redis.delete(List.of(subscriptionId, versionKey(subscriptionId)));
            } catch (RuntimeException e) {
                if (!isClusterSlotMismatch(e)) {
                    throw e;
                }
                // Only the one subscription id shape requireClusterSlotAlignable refuses can land here, and a
                // conditional write already refuses that shape before ever writing a version key, so this id's
                // version key can never exist to strand. The second delete below is provably a no-op, not merely
                // convenient. The checkpoint is still deleted first, defensively. If a version key ever did exist,
                // deleting it first and failing before the checkpoint would leave a checkpoint with no stored
                // version, which a later notOlderThan write would then accept unconditionally.
                redis.delete(subscriptionId);
                redis.delete(versionKey(subscriptionId));
                return 0L;
            }
        };
        executeWithRetry(deleteBoth, __ -> !shutdown, retryStrategy).get();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code subscriptionId} starts with the prefix this storage reserves for
     *                                  its own version keys, see the class javadoc. Specific to this
     *                                  implementation, not part of the {@link CheckpointStorage} contract.
     */
    @Override
    public boolean exists(String subscriptionId) {
        requireOutsideVersionKeyNamespace(subscriptionId);
        Supplier<Boolean> exists = () -> {
            Boolean result = redis.hasKey(subscriptionId);
            return result != null && result;
        };
        return Boolean.TRUE.equals(executeWithRetry(exists, __ -> !shutdown, retryStrategy).get());
    }

    // The hash tag wraps the same substring Redis Cluster's own slot algorithm would pick out of the checkpoint key
    // (subscriptionId itself, unprefixed), so the two keys the write scripts touch land in the same slot. A SHA-256
    // digest of the full subscription id follows it, outside the braces, where Cluster never looks once it has
    // found the hash tag's closing brace, so two ids that share a hash tag (two tenant-scoped ids under the same
    // "{tenant}" tag, for instance) still get distinct version keys instead of silently sharing one fencing
    // version. The digest, not the raw id or a length-prefixed copy of it, is what makes this collision-resistant.
    // The tag can equal the whole subscription id (the fallback branch in clusterHashTag), so a raw or delimited
    // copy of the id sitting next to that tag lets one subscription id's own text be read as a completely different
    // subscription id's tag-plus-copy. Two adversarially constructed pairs broke that this way in review, a plain
    // "}:" separator ("a}:{a" vs "{a}:a}:{a") and a length-prefixed one ("a}12:{a" vs "{a}7:a}12:{a"), both
    // exploiting the same doubling. A digest carries none of the id's own structure for an adversary to reuse.
    // Package-private, not private, so a test can compute it against an independent Cluster slot implementation.
    static String versionKey(String subscriptionId) {
        return VERSION_KEY_PREFIX + "{" + clusterHashTag(subscriptionId) + "}" + sha256Hex(subscriptionId);
    }

    // SHA-256 is a JDK-guaranteed algorithm (Java Cryptography Architecture Standard Algorithm Names), so this can
    // never actually throw. Hex, not the raw digest bytes, because this key is a Java String, and hex keeps it to
    // characters any reasonable key serializer round-trips cleanly, rather than raw bytes that decode to arbitrary
    // code points.
    private static String sha256Hex(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is a mandatory algorithm for every Java implementation", e);
        }
    }

    // Redis Cluster's own slot algorithm (CLUSTER-SPEC, "Keys hash tags"): the first '{', then the first '}' after
    // it; the whole key stands in for the tag when either brace is missing or none of the key sits between them.
    // Mirrored here, rather than pulled from a client library, because keeping it beside versionKey is what makes it
    // obvious the two must never drift apart.
    private static String clusterHashTag(String key) {
        int openBrace = key.indexOf('{');
        if (openBrace < 0) {
            return key;
        }
        int closeBrace = key.indexOf('}', openBrace + 1);
        if (closeBrace < 0 || closeBrace == openBrace + 1) {
            return key;
        }
        return key.substring(openBrace + 1, closeBrace);
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
