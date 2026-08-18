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
 * nor a plain {@link #hasApplied(String, AppendId)} call. {@link #waitUntilApplied(String, AppendId, Duration)} is
 * the one exception. Its own reads retry on the same {@link RetryStrategy}, but only until the wait's own deadline,
 * so a sustained store outage still surfaces as a timeout rather than a block with no limit. The {@link Backoff}
 * decides how long a wait sleeps between polls that succeeded and simply found the append not yet applied.
 */
@NullMarked
class MongoAppliedAppendStore implements AppliedAppendStore {

    private static final String PROJECTION_ID = "projectionId";
    private static final String APPEND_ID = "appendId";
    private static final String RECORDED_AT = "recordedAt";
    private static final String PROJECTION_ID_APPEND_ID_INDEX = "projectionId_appendId";
    private static final String RECORDED_AT_TTL_INDEX = "recordedAt_ttl";
    private static final String INDEX_OPTIONS_CONFLICT = "IndexOptionsConflict";

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
    MongoAppliedAppendStore(MongoOperations mongoOperations, String collection, Duration retention) {
        this(mongoOperations, collection, retention, defaultRetryStrategy(), DEFAULT_POLL_BACKOFF);
    }

    MongoAppliedAppendStore(MongoOperations mongoOperations, String collection, Duration retention, RetryStrategy retryStrategy, Backoff pollBackoff) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
        this.retention = requireNonNull(retention, "retention cannot be null");
        this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.pollBackoff = requireNonNull(pollBackoff, "pollBackoff cannot be null");
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
        executeWithRetry(write, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean hasApplied(String projectionId, AppendId appendId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        Supplier<Boolean> read = () -> readOnce(projectionId, appendId);
        return requireNonNull(executeWithRetry(read, __ -> !shutdown, retryStrategy).get());
    }

    private boolean readOnce(String projectionId, AppendId appendId) {
        ensureIndexesOnce();
        return mongoOperations.exists(query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())), collection);
    }

    /**
     * A read for {@link #waitUntilApplied(String, AppendId, Duration, Backoff)} whose retries stop once
     * {@code deadlineNanos} ({@link System#nanoTime()} scale) passes, rather than continuing on
     * {@link #retryStrategy}'s own schedule, which otherwise keeps retrying with no limit. {@link #readOnce} also
     * ensures the indexes exist, so a fresh store whose index setup fails during an outage is retried, and limited
     * to this same deadline, exactly like a failing read, rather than throwing out of a method documented to never
     * throw.
     * A store that is still failing once the deadline arrives answers {@code false} instead of retrying past it, so
     * the wait's own deadline check is what ends the wait.
     */
    private boolean readOnceBoundedBy(String projectionId, AppendId appendId, long deadlineNanos) {
        Supplier<Boolean> read = () -> readOnce(projectionId, appendId);
        Predicate<Throwable> notShutdownAndBeforeDeadline = __ -> !shutdown && System.nanoTime() < deadlineNanos;
        try {
            return requireNonNull(executeWithRetry(read, notShutdownAndBeforeDeadline, retryStrategy).get());
        } catch (RuntimeException e) {
            if (System.nanoTime() >= deadlineNanos) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public void clear(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Runnable delete = () -> {
            ensureIndexesOnce();
            mongoOperations.remove(query(where(PROJECTION_ID).is(projectionId)), collection);
        };
        executeWithRetry(delete, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout) {
        return waitUntilApplied(projectionId, appendId, timeout, pollBackoff);
    }

    /**
     * Overrides {@link AppliedAppendStore}'s default loop so each poll's read retries against {@link #retryStrategy}
     * only until this wait's own deadline, rather than on {@link #retryStrategy}'s own schedule, which otherwise
     * keeps retrying with no limit. Without this, a sustained store outage keeps a single read retrying forever and
     * the wait never reaches the deadline check that is supposed to end it. The loop shape (read, check, sleep, grow
     * the backoff) otherwise matches the interface default. Only the read is store-specific.
     * <p>
     * The deadline is checked before a retried read sleeps, not after, so the last in-flight attempt can run past
     * the deadline by up to one of {@link #retryStrategy}'s own backoff intervals before the wait gives up.
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

    private static RetryStrategy defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f);
    }

    private static void ensureIndexes(MongoOperations mongoOperations, String collection, Duration retention) {
        IndexOperations indexOps = mongoOperations.indexOps(collection);
        indexOps.ensureIndex(new Index().on(PROJECTION_ID, Direction.ASC).on(APPEND_ID, Direction.ASC).named(PROJECTION_ID_APPEND_ID_INDEX).unique());
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
