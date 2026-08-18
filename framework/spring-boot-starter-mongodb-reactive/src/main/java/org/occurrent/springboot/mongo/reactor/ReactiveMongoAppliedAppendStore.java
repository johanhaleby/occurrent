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

package org.occurrent.springboot.mongo.reactor;

import com.mongodb.MongoCommandException;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOptions;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Date;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedAppendStore} the reactive Mongo starter contributes as
 * {@code @Projection(recordAppliedAppends = true)}'s zero-config default. {@link AppliedAppendStore} is a
 * blocking-shaped interface on both stacks, called from the reactor recorder's {@code doOnSuccess} callback, which
 * already runs on {@code boundedElastic}, so blocking on the underlying reactive Mongo call here is the same bridge
 * the rest of this reactor stack makes in the other direction.
 * <p>
 * One document per (projection id, append id) pair, indexed by a unique compound index on both fields. Calling
 * {@link #recordApplied(String, AppendId)} twice for the same pair upserts the same document rather than inserting
 * a duplicate, and {@link #hasApplied(String, AppendId)} is an indexed point lookup. The same compound index serves
 * {@link #clear(String)}'s delete-by-projection-id, since a query on the index's leading field is an indexed prefix
 * scan.
 * <p>
 * A separate TTL index on {@code recordedAt} bounds storage. It is housekeeping only, never a correctness
 * parameter, see ADR 132 decision 11. Changing the configured retention across a restart alters the existing TTL
 * index in place with a {@code collMod}, rather than failing startup, since MongoDB refuses to create an index with
 * the same key pattern but a different {@code expireAfterSeconds} on top of one that already exists.
 * <p>
 * Both indexes are created on first use rather than in the constructor, so building this bean never requires a
 * reachable database. Application-context tests that construct the auto-configuration against a stub
 * {@code ReactiveMongoOperations} would otherwise fail the moment this store is instantiated, before it is ever
 * asked to do anything.
 * <p>
 * Two different mechanisms pace two different things here, and they do not overlap. The {@link Retry} retries a
 * read or a write that failed, so a transient store error neither fails the projection's own handling of a
 * delivered event nor a plain {@link #hasApplied(String, AppendId)} call. {@link #waitUntilApplied(String, AppendId, Duration)}
 * is the one exception. Its own reads retry on the same {@link Retry}, but only until the wait's own deadline, and a
 * read that is still failing once the deadline arrives, or once {@link #retry} itself gives up, answers {@code false}
 * rather than throwing, so a sustained store outage always surfaces as a timeout, never as an exception. The
 * {@link Backoff} decides how long a wait sleeps between polls that succeeded and simply found the append not yet
 * applied. {@link Retry} rather than the blocking {@code RetryStrategy} because this is the reactive stack, matching
 * how the reactive starter retries elsewhere.
 * <p>
 * {@link #defaultRetry()} gives up after a fixed number of attempts rather than retrying forever, a deliberate
 * divergence from the blocking store's default, which keeps {@link #hasApplied(String, AppendId)} and
 * {@link #recordApplied(String, AppendId)} from blocking a direct caller indefinitely under a sustained outage. An
 * application that needs parity with the blocking store's retry, which keeps retrying with no limit, should supply
 * its own {@link Retry}.
 */
@NullMarked
public class ReactiveMongoAppliedAppendStore implements AppliedAppendStore {

    private static final String PROJECTION_ID = "projectionId";
    private static final String APPEND_ID = "appendId";
    private static final String RECORDED_AT = "recordedAt";
    private static final String PROJECTION_ID_APPEND_ID_INDEX = "projectionId_appendId";
    private static final String RECORDED_AT_TTL_INDEX = "recordedAt_ttl";
    private static final String INDEX_OPTIONS_CONFLICT = "IndexOptionsConflict";

    private final ReactiveMongoOperations mongoOperations;
    private final String collection;
    private final Duration retention;
    private final Retry retry;
    private final Backoff pollBackoff;

    private volatile boolean indexesEnsured = false;

    /**
     * Retries a failing read or write with backoff from 100 ms up to 2 seconds, giving up after 5 retries (6
     * attempts total) and surfacing the last failure. The blocking store does not give up this way, it retries the
     * same backoff forever, since it keeps {@code recordApplied(..)} durable under an outage. This store's default
     * gives up so a direct call to {@link #hasApplied(String, AppendId)} or {@link #recordApplied(String, AppendId)}
     * does not block a caller indefinitely. {@link #waitUntilApplied(String, AppendId, Duration)} never inherits
     * that exhaustion though, its own reads answer {@code false} rather than surfacing the failure, so a wait
     * against this default still resolves by its own deadline. Polls a wait at
     * {@link AppliedAppendStore#DEFAULT_POLL_BACKOFF}.
     */
    public ReactiveMongoAppliedAppendStore(ReactiveMongoOperations mongoOperations, String collection, Duration retention) {
        this(mongoOperations, collection, retention, defaultRetry(), DEFAULT_POLL_BACKOFF);
    }

    public ReactiveMongoAppliedAppendStore(ReactiveMongoOperations mongoOperations, String collection, Duration retention, Retry retry, Backoff pollBackoff) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
        this.retention = requireNonNull(retention, "retention cannot be null");
        this.retry = requireNonNull(retry, "retry cannot be null");
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
        // Mono.defer so a retried upsert builds a fresh Update, and with it a fresh new Date(), on every attempt.
        // Built once outside a defer, a retry would resubscribe to the same upsert Mono and reuse the first
        // attempt's timestamp, so a record that only succeeds after several retries would carry a recordedAt from
        // well before the insert, shortening its actual time in the TTL index.
        Mono.fromRunnable(this::ensureIndexesOnce)
                .then(Mono.defer(() -> mongoOperations.upsert(
                        query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())),
                        new Update().setOnInsert(RECORDED_AT, new Date()),
                        collection)))
                .retryWhen(retry)
                .block();
    }

    @Override
    public boolean hasApplied(String projectionId, AppendId appendId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        Boolean applied = existsWithIndexesEnsured(projectionId, appendId)
                .retryWhen(retry)
                .block();
        return Boolean.TRUE.equals(applied);
    }

    /**
     * Runs {@link #ensureIndexesOnce()} ahead of the existence check in the same reactive chain, so
     * {@code .retryWhen(retry)} retries index setup exactly like a failing read rather than letting it throw
     * straight out of the caller.
     */
    private Mono<Boolean> existsWithIndexesEnsured(String projectionId, AppendId appendId) {
        return Mono.fromRunnable(this::ensureIndexesOnce)
                .then(mongoOperations.exists(query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())), collection));
    }

    /**
     * A read for {@link #waitUntilApplied(String, AppendId, Duration, Backoff)} whose retries stop once
     * {@code deadlineNanos} ({@link System#nanoTime()} scale) passes, rather than continuing on {@link #retry}'s own
     * schedule. Blocking on the retried read with the remaining duration as the block timeout is what stops the
     * retries themselves at the deadline, not just the wait loop around them. {@link #retry}'s default is finite,
     * so a sustained outage can also exhaust the retry itself well before the deadline. Either way the read answers
     * {@code false} rather than throwing. A wait polls for "not applied yet", and its own deadline check, not the
     * read's failure, is what ends it. This also covers a fresh store's index setup, since
     * {@link #existsWithIndexesEnsured} runs it in the same chain as the read, retried and limited to the same
     * deadline.
     */
    private boolean readOnceBoundedBy(String projectionId, AppendId appendId, long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return false;
        }
        try {
            Boolean applied = existsWithIndexesEnsured(projectionId, appendId)
                    .retryWhen(retry)
                    .block(Duration.ofNanos(remainingNanos));
            return Boolean.TRUE.equals(applied);
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    @Override
    public void clear(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Mono.fromRunnable(this::ensureIndexesOnce)
                .then(mongoOperations.remove(query(where(PROJECTION_ID).is(projectionId)), collection))
                .retryWhen(retry)
                .block();
    }

    @Override
    public boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout) {
        return waitUntilApplied(projectionId, appendId, timeout, pollBackoff);
    }

    /**
     * Overrides {@link AppliedAppendStore}'s default loop so each poll's read retries against {@link #retry}
     * only until this wait's own deadline, rather than continuing to block past it. Without this, a sustained store
     * outage keeps a single read retrying (and blocking) past the wait's deadline and the wait throws instead of
     * reaching the deadline check that is supposed to end it. The loop shape (read, check, sleep, grow the backoff)
     * otherwise matches the interface default and the blocking store's own override. Only the read is
     * store-specific.
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

    private static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private static void ensureIndexes(ReactiveMongoOperations mongoOperations, String collection, Duration retention) {
        ReactiveIndexOperations indexOps = mongoOperations.indexOps(collection);
        indexOps.ensureIndex(new Index().on(PROJECTION_ID, Direction.ASC).on(APPEND_ID, Direction.ASC).named(PROJECTION_ID_APPEND_ID_INDEX).unique()).block();
        try {
            indexOps.ensureIndex(new Index().on(RECORDED_AT, Direction.ASC).named(RECORDED_AT_TTL_INDEX).expire(retention)).block();
        } catch (RuntimeException e) {
            if (!isIndexOptionsConflict(e)) {
                throw e;
            }
            indexOps.alterIndex(RECORDED_AT_TTL_INDEX, IndexOptions.expireAfter(retention)).block();
        }
    }

    /**
     * MongoDB refuses to (re)create an index with the same key pattern but different options, error code 85,
     * {@code IndexOptionsConflict}, which is exactly what a changed {@code retention} across a restart does to the
     * TTL index. Caught here and handled with {@link ReactiveIndexOperations#alterIndex(String, IndexOptions)}
     * instead of failing startup.
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
