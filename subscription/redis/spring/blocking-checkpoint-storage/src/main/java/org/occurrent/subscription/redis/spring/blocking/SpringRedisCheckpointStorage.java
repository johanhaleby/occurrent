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
 * {@link CheckpointWriteCondition#notOlderThan(long)} and {@link CheckpointWriteCondition#ifAbsent()} are evaluated
 * by a Lua script that compares the stored version and writes both keys in one round trip, so no other writer can
 * land between the comparison and the write. {@link CheckpointWriteCondition#any()} needs no comparison and keeps
 * writing through {@code opsForValue().set}, exactly as before, leaving the version key untouched.
 * <p>
 * <strong>Redis Cluster.</strong> The version key carries a hash tag built from whatever the checkpoint key itself
 * hashes on, so Cluster places both keys in the same slot and the scripts above, and the two-key {@code DEL} in
 * {@link #delete(String)}, are never refused for crossing slots. A subscription id with no braces of its own hashes
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
 * pair, an unmatched brace, or an empty pair like {@code {}}) and that whole id contains a closing brace somewhere
 * in it, for example {@code "{}orders"} or {@code "a}b{c"}. Wrapping such text in a fresh hash tag only reproduces
 * it when the text has no closing brace of its own, since Cluster stops at the first one it finds, and that is then
 * the wrap's own or an earlier one already inside the id, not the one this class appended. Such an id still refuses
 * a conditional write immediately, with the error Cluster reports for crossing slots, the same way every id used
 * to. No subscription id this library or its tests generate takes that shape.
 * <p>
 * An empty subscription id is a second, narrower shape this cannot help, for a different reason. The checkpoint key
 * is then empty text with nothing to hash a tag from, while the version key's own hash tag, built from that same
 * empty text, comes out as an empty pair that Cluster falls back on hashing whole instead. Unlike the shape above,
 * {@link #save(String, Checkpoint, CheckpointWriteCondition)} and {@link #delete(String)} refuse an empty
 * subscription id outright, with an {@link IllegalArgumentException} naming the reason, rather than let Cluster's
 * own crossed-slots error surface two calls downstream. Nothing in this library needs an empty subscription id, so
 * the refusal applies on every deployment, not only Cluster.
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
     * The refusal, and a Cluster {@code CROSSSLOT} failure, are both excluded from the retry strategy's own
     * predicate, not merely allowed to exhaust it. This storage's default {@link RetryStrategy} is
     * {@code exponentialBackoff}, whose default max attempts is infinite, so either one left inside the ordinary
     * retry path would retry a write that can never succeed and hang the delivery thread forever. Excluding the
     * refusal is what {@link CheckpointWriteConditionNotFulfilledException}'s javadoc means by "must never be
     * retried on the path that threw it", and a slot mismatch is the same kind of failure, just reported by Cluster
     * instead of by this class.
     */
    private Checkpoint saveConditionally(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition, RedisScript<Long> script, byte[]... extraArgs) {
        requireNonEmptySubscriptionId(subscriptionId);
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

    // An empty subscription id is the one input the hash tag in versionKey cannot help, since Cluster hashes an
    // empty checkpoint key on nothing to tag at all, while the version key's own hash tag, built from that same
    // empty text, comes out as an empty pair Cluster falls back on hashing whole instead. No caller in this
    // library, or a realistic one, needs an empty subscription id, so this refuses it at the boundary rather than
    // let it surface as Cluster's own crossed-slots error two calls downstream.
    private static void requireNonEmptySubscriptionId(String subscriptionId) {
        if (subscriptionId.isEmpty()) {
            throw new IllegalArgumentException("Subscription id cannot be empty, since Redis Cluster would hash the checkpoint key and this storage's version key for it to different slots and refuse a conditional write for crossing slots.");
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

    // True unconditionally. The comparison is real on a standalone server, a replicated one, and on Cluster, since
    // versionKey's hash tag keeps both keys the script touches in the same slot.
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
        requireNonEmptySubscriptionId(subscriptionId);
        Supplier<Long> deleteBoth = () -> redis.delete(List.of(subscriptionId, versionKey(subscriptionId)));
        // A CROSSSLOT failure here is excluded from retry for the same reason as in saveConditionally, the same
        // two-key pair crosses slots the same way, and the default infinite backoff would otherwise hang whatever
        // called delete().
        executeWithRetry(deleteBoth, e -> !shutdown && !isClusterSlotMismatch(e), retryStrategy).get();
    }

    @Override
    public boolean exists(String subscriptionId) {
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
    // version. The digest, not the raw id or a length-prefixed copy of it, is what makes this collision-resistant:
    // the tag can equal the whole subscription id (the fallback branch in clusterHashTag), so a raw or delimited
    // copy of the id sitting next to that tag lets one subscription id's own text be read as a completely different
    // subscription id's tag-plus-copy. Two adversarially constructed pairs broke that this way in review, a plain
    // "}:" separator ("a}:{a" vs "{a}:a}:{a") and a length-prefixed one ("a}12:{a" vs "{a}7:a}12:{a}"), both
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
