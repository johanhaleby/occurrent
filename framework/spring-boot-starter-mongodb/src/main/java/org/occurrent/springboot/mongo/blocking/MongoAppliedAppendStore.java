/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.springboot.mongo.blocking;

import com.mongodb.MongoCommandException;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.IndexOptions;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Duration;
import java.util.Date;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedAppendStore} the Mongo starter contributes as {@code @Projection(recordAppliedAppends = true)}'s
 * zero-config default. One document per (projection id, append id) pair, indexed by a unique compound index on
 * both fields. Calling {@link #recordApplied(String, AppendId)} twice for the same pair upserts the same document
 * rather than inserting a duplicate, and {@link #hasApplied(String, AppendId)} is an indexed point lookup. The same
 * compound index serves {@link #clear(String)}'s delete-by-projection-id, since a query on the index's leading field
 * is an indexed prefix scan.
 * <p>
 * A separate TTL index on {@code recordedAt} bounds storage. It is housekeeping only, never a correctness
 * parameter, see ADR 132 decision 11. A wait for an append whose record has been evicted simply times out, which is
 * the safe direction. Changing the configured retention across a restart alters the existing TTL index in place
 * with a {@code collMod}, rather than failing startup, since MongoDB refuses to create an index with the same key
 * pattern but a different {@code expireAfterSeconds} on top of one that already exists.
 * <p>
 * Both indexes are created on first use rather than in the constructor, so building this bean never requires a
 * reachable database. Application-context tests that construct the auto-configuration against a stub
 * {@code MongoOperations} would otherwise fail the moment this store is instantiated, before it is ever asked to do
 * anything.
 * <p>
 * Two different mechanisms pace two different things here, and they do not overlap. The {@link RetryStrategy}
 * retries a read or a write that failed, so a transient store error neither fails {@link #recordApplied(String, AppendId)}
 * nor a plain {@link #hasApplied(String, AppendId)} call. It gives up after {@link #DEFAULT_MAX_ATTEMPTS} attempts
 * and throws, so a store that stays unreachable stops a projection's delivery thread rather than holding it for as
 * long as the outage lasts, which is what lets a clear that keeps failing stop its recorder (ADR 132 decision 7).
 * {@link #waitUntilApplied(String, AppendId, Duration)} is the one exception. Its own reads retry on the same
 * {@link RetryStrategy}, but only until the wait's own deadline, and a read still failing when the deadline arrives
 * answers {@code false}, so a sustained store outage ends a wait as a timeout rather than as a failure. The
 * {@link Backoff} decides how long a wait sleeps between polls that succeeded and simply found the append not yet
 * applied.
 * <p>
 * An index whose options this store can neither match nor alter is the one failure it does not retry at all, since
 * error 85 never becomes anything but error 85.
 */
@NullMarked
public class MongoAppliedAppendStore implements AppliedAppendStore {

    private static final String PROJECTION_ID = "projectionId";
    private static final String APPEND_ID = "appendId";
    private static final String RECORDED_AT = "recordedAt";
    private static final String PROJECTION_ID_APPEND_ID_INDEX = "projectionId_appendId";
    private static final String RECORDED_AT_TTL_INDEX = "recordedAt_ttl";
    private static final String INDEX_OPTIONS_CONFLICT = "IndexOptionsConflict";

    /**
     * How many times a read or a write is attempted before it gives up, 20. With this store's own 100 ms to 2 s
     * backoff that spans about 31 seconds, deliberately just past the MongoDB driver's 30 second default server
     * selection timeout, so an ordinary primary failover is ridden out rather than turned into a failure. A store
     * that stays unreachable past that stops blocking the projection's delivery thread instead of retrying for as
     * long as the outage lasts.
     */
    public static final int DEFAULT_MAX_ATTEMPTS = 20;

    private final MongoOperations mongoOperations;
    private final String collection;
    private final Duration retention;
    private final RetryStrategy retryStrategy;
    private final Backoff pollBackoff;

    private volatile boolean shutdown = false;
    private volatile boolean indexesEnsured = false;

    /**
     * Retries a failing read or write with exponential backoff from 100 ms up to 2 seconds, the same default
     * {@code NativeMongoCheckpointStorage} uses, and polls a wait at {@link AppliedAppendStore#DEFAULT_POLL_BACKOFF}.
     */
    public MongoAppliedAppendStore(MongoOperations mongoOperations, String collection, Duration retention) {
        this(mongoOperations, collection, retention, defaultRetryStrategy(), DEFAULT_POLL_BACKOFF);
    }

    /**
     * @param collection   rejected if blank, for the same reason a negative {@code retention} is. MongoDB rejects
     *                     the name, and a permanent configuration error belongs at startup rather than on every
     *                     read and write this store makes.
     * @param retention    rejected if negative, since MongoDB would then reject the TTL index this store creates
     *                      from it, and a {@code RetryStrategy} would otherwise retry that permanent
     *                      configuration error rather than fail once at startup.
     * @param pollBackoff  rejected the same way {@link #waitUntilApplied(String, AppendId, Duration, Backoff)}
     *                     rejects one, so a {@code pollBackoff} that would busy-loop the store fails here, when the
     *                     bean is built, rather than at the first wait a caller happens to make.
     */
    public MongoAppliedAppendStore(MongoOperations mongoOperations, String collection, Duration retention, RetryStrategy retryStrategy, Backoff pollBackoff) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
        if (collection.isBlank()) {
            throw new IllegalArgumentException("collection cannot be blank, MongoDB rejects the name and this store would retry that permanent error on every read and write rather than failing once when the bean is built.");
        }
        requireNonNull(retention, "retention cannot be null");
        if (retention.isNegative()) {
            throw new IllegalArgumentException("retention cannot be negative, a TTL index cannot expire a document before it was inserted.");
        }
        this.retention = retention;
        this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        AppliedAppendStore.rejectBusyLoopBackoff(requireNonNull(pollBackoff, "pollBackoff cannot be null"));
        this.pollBackoff = pollBackoff;
    }

    /**
     * Ensures the compound unique index and the TTL index exist, once, the first time this store is actually asked
     * to do anything. Synchronized rather than a lock-free check-then-act, since a race here would mean two threads
     * both attempting index creation concurrently, which is at worst wasted work and at best exactly the
     * {@code IndexOptionsConflict} path {@link #ensureIndexes} already handles, but is not worth risking on a
     * one-time setup step.
     */
    private synchronized void ensureIndexesOnce() {
        if (indexesEnsured) {
            return;
        }
        ensureIndexes(mongoOperations, collection, retention);
        indexesEnsured = true;
    }

    @Override
    public void recordApplied(String projectionId, AppendId appendId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        Runnable write = () -> {
            ensureIndexesOnce();
            mongoOperations.upsert(
                    query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())),
                    new Update().setOnInsert(RECORDED_AT, new Date()),
                    collection);
        };
        executeWithRetry(write, this::isRetryable, retryStrategy).run();
    }

    @Override
    public boolean hasApplied(String projectionId, AppendId appendId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        Supplier<Boolean> read = () -> readOnce(projectionId, appendId);
        return requireNonNull(executeWithRetry(read, this::isRetryable, retryStrategy).get());
    }

    private boolean readOnce(String projectionId, AppendId appendId) {
        ensureIndexesOnce();
        return mongoOperations.exists(query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())), collection);
    }

    /**
     * A read for {@link #waitUntilApplied(String, AppendId, Duration, Backoff)} whose retries stop once
     * {@code deadlineNanos} ({@link System#nanoTime()} scale) passes, rather than running out
     * {@link #retryStrategy}'s own attempts, which take about half a minute by default and can be configured to
     * take much longer. {@link #readOnce} also
     * ensures the indexes exist, so a fresh store whose index setup fails during an outage is retried, and limited
     * to this same deadline, exactly like a failing read, rather than throwing out of a method documented to never
     * throw.
     * A store that is still failing once the deadline arrives answers {@code false} instead of retrying past it, so
     * the wait's own deadline check is what ends the wait. ADR 132 decision 5 states this unconditionally, a store
     * that cannot be read keeps the wait polling until its timeout expires, so a caller-supplied
     * {@link #retryStrategy} that exhausts its own attempt limit before the deadline also answers {@code false}
     * here rather than surfacing that exhaustion, the same as {@link #recordApplied(String, AppendId)} or
     * {@link #hasApplied(String, AppendId)} would surface it outside a wait. A thread interrupted during a retry's
     * backoff sleep answers {@code false} the same way, the same as an interrupted poll sleep in the loop above it,
     * rather than propagating the {@code RuntimeException} {@code RetryExecution} wraps that interrupt in. Any
     * interrupt flag is already restored by {@code RetryExecution} before this method ever sees the exception.
     */
    private boolean readOnceBoundedBy(String projectionId, AppendId appendId, long deadlineNanos) {
        Supplier<Boolean> read = () -> readOnce(projectionId, appendId);
        Predicate<Throwable> notShutdownAndBeforeDeadline = e -> isRetryable(e) && System.nanoTime() < deadlineNanos;
        try {
            return requireNonNull(executeWithRetry(read, notShutdownAndBeforeDeadline, retryStrategy).get());
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public void clear(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Runnable delete = () -> {
            ensureIndexesOnce();
            mongoOperations.remove(query(where(PROJECTION_ID).is(projectionId)), collection);
        };
        executeWithRetry(delete, this::isRetryable, retryStrategy).run();
    }

    @Override
    public boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout) {
        return waitUntilApplied(projectionId, appendId, timeout, pollBackoff);
    }

    /**
     * Overrides {@link AppliedAppendStore}'s default loop so each poll's read retries against {@link #retryStrategy}
     * only until this wait's own deadline, rather than running out its own attempts first. Without this, a
     * sustained store outage keeps a single read retrying past a deadline shorter than the retry budget, and the
     * wait never reaches the deadline check that is supposed to end it. The loop shape (read, check, sleep, grow
     * the backoff) otherwise matches the interface default. Only the read is store-specific.
     * <p>
     * The deadline bounds the retries, not the individual read. It is checked before a retried read sleeps, so the
     * last attempt can already be in flight when the deadline passes, and this method returns once that attempt
     * answers. A MongoDB client left with no timeout of its own does not answer at all while a connection it has
     * accepted stops responding, so the timeout a caller asked for holds only as far as the client's own
     * {@code timeoutMS} or socket timeout holds. Configure one on the client if the wait's deadline has to be the
     * one a caller gets, for example through {@code spring.mongodb.uri}. The reactive store bounds the same
     * read with {@code block(Duration)}, which the blocking driver has no equivalent of.
     */
    @Override
    public boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout, Backoff backoff) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        requireNonNull(backoff, "backoff cannot be null");
        AppliedAppendStore.rejectBusyLoopBackoff(backoff);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long intervalNanos = switch (backoff) {
            case Backoff.Fixed fixed -> Duration.ofMillis(fixed.millis).toNanos();
            case Backoff.Exponential exponential -> exponential.initial.toNanos();
            case Backoff.None ignored -> throw new IllegalStateException("unreachable, rejected above");
        };
        while (true) {
            if (readOnceBoundedBy(projectionId, appendId, deadlineNanos)) {
                return true;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            long sleepNanos = Math.min(intervalNanos, remainingNanos);
            try {
                Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (backoff instanceof Backoff.Exponential exponential) {
                intervalNanos = Math.min((long) (intervalNanos * exponential.multiplier), exponential.max.toNanos());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        shutdown = true;
    }

    /**
     * Package-private and returning {@link RetryStrategy.Retry} so a test can swap the backoff for a fast one and
     * still exercise the attempt limit this store actually ships with.
     */
    static RetryStrategy.Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * Retries anything except a shutdown and an index whose options this store can neither match nor alter. Error
     * 85 on the compound index never becomes anything else, so retrying it turns a configuration mistake into a
     * call that never returns, which is the whole reason a permanent error is told apart from a transient one here.
     */
    private boolean isRetryable(Throwable e) {
        return !shutdown && !(e instanceof ConflictingIndexException);
    }

    /**
     * Thrown rather than retried, so a compound index this store cannot create fails the call that needed it
     * instead of being attempted again on a schedule.
     */
    public static final class ConflictingIndexException extends IllegalStateException {
        ConflictingIndexException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static void ensureIndexes(MongoOperations mongoOperations, String collection, Duration retention) {
        IndexOperations indexOps = mongoOperations.indexOps(collection);
        try {
            indexOps.ensureIndex(new Index().on(PROJECTION_ID, Direction.ASC).on(APPEND_ID, Direction.ASC).named(PROJECTION_ID_APPEND_ID_INDEX).unique());
        } catch (DataAccessException e) {
            if (!isIndexOptionsConflict(e)) {
                throw e;
            }
            // No collMod for this one, unlike the TTL index. collMod changes expireAfterSeconds and hidden, and it
            // cannot change a partial filter or a collation at all, so there is nothing to alter this index into.
            // An operator drops it, and the message says so rather than leaving error 85 to be retried.
            throw new ConflictingIndexException("Collection '" + collection + "' already has an index named '" + PROJECTION_ID_APPEND_ID_INDEX + "' whose options differ from the unique index on " + PROJECTION_ID + " and " + APPEND_ID + " this store needs. Drop that index and let this store create its own.", e);
        }
        try {
            indexOps.ensureIndex(new Index().on(RECORDED_AT, Direction.ASC).named(RECORDED_AT_TTL_INDEX).expire(retention));
        } catch (DataAccessException e) {
            if (!isIndexOptionsConflict(e)) {
                throw e;
            }
            indexOps.alterIndex(RECORDED_AT_TTL_INDEX, IndexOptions.expireAfter(retention));
        }
    }

    /**
     * MongoDB refuses to (re)create an index with the same key pattern but different options, error code 85,
     * {@code IndexOptionsConflict}, which is exactly what a changed {@code retention} across a restart does to the
     * TTL index. Caught here and handled with {@link IndexOperations#alterIndex(String, IndexOptions)} instead of
     * failing startup.
     */
    private static boolean isIndexOptionsConflict(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof MongoCommandException commandException && INDEX_OPTIONS_CONFLICT.equals(commandException.getErrorCodeName())) {
                return true;
            }
        }
        return false;
    }
}
