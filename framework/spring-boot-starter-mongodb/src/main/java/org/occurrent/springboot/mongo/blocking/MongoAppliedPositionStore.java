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

import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedPositionStore;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedPositionStore} the Mongo starter contributes as {@code @Projection(recordAppliedPosition = true)}'s
 * zero-config default. One document per projection id, {@code _id} the projection id and {@code position} the applied
 * position.
 * <p>
 * {@link #advance(String, long)} writes with MongoDB's {@code $max} update operator in one round trip, so the
 * never-moves-backwards guarantee {@link AppliedPositionStore#advance(String, long)} makes holds even under
 * concurrent advances for the same projection id, with no read-modify-write race.
 * <p>
 * Two different mechanisms pace two different things here, and they do not overlap. The {@link RetryStrategy} retries
 * a read or a write that failed, so a transient store error neither fails {@link #advance(String, long)} nor a plain
 * {@link #appliedPosition(String)} call. {@link #waitUntilApplied(String, long, Duration)} is the one exception. Its
 * own reads retry on the same {@link RetryStrategy}, but bounded to the wait's deadline, so a sustained store outage
 * still surfaces as a timeout rather than an unbounded block. The {@link Backoff} decides how long a wait sleeps
 * between polls that succeeded and simply found the projection still behind.
 */
@NullMarked
class MongoAppliedPositionStore implements AppliedPositionStore {

    private static final String ID = "_id";
    private static final String POSITION = "position";

    private final MongoOperations mongoOperations;
    private final String collection;
    private final RetryStrategy retryStrategy;
    private final Backoff pollBackoff;

    private volatile boolean shutdown = false;

    /**
     * Retries a failing read or write with exponential backoff from 100 ms up to 2 seconds, the same default
     * {@code NativeMongoCheckpointStorage} uses, and polls a wait at {@link AppliedPositionStore#DEFAULT_POLL_BACKOFF}.
     */
    MongoAppliedPositionStore(MongoOperations mongoOperations, String collection) {
        this(mongoOperations, collection, defaultRetryStrategy(), DEFAULT_POLL_BACKOFF);
    }

    MongoAppliedPositionStore(MongoOperations mongoOperations, String collection, RetryStrategy retryStrategy, Backoff pollBackoff) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
        this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.pollBackoff = requireNonNull(pollBackoff, "pollBackoff cannot be null");
    }

    @Override
    public OptionalLong appliedPosition(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Supplier<OptionalLong> read = () -> readOnce(projectionId);
        return requireNonNull(executeWithRetry(read, __ -> !shutdown, retryStrategy).get());
    }

    private OptionalLong readOnce(String projectionId) {
        Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection);
        if (document == null) {
            return OptionalLong.empty();
        }
        Number position = document.get(POSITION, Number.class);
        return position == null ? OptionalLong.empty() : OptionalLong.of(position.longValue());
    }

    /**
     * A read for {@link #waitUntilApplied(String, long, Duration, Backoff)} whose retries stop once {@code deadlineNanos}
     * ({@link System#nanoTime()} scale) passes, rather than continuing on {@link #retryStrategy}'s own unbounded
     * schedule. A store that is still failing once the deadline arrives answers empty instead of retrying past it,
     * so the wait's own deadline check is what ends the wait.
     */
    private OptionalLong readOnceBoundedBy(String projectionId, long deadlineNanos) {
        Supplier<OptionalLong> read = () -> readOnce(projectionId);
        Predicate<Throwable> notShutdownAndBeforeDeadline = __ -> !shutdown && System.nanoTime() < deadlineNanos;
        try {
            return requireNonNull(executeWithRetry(read, notShutdownAndBeforeDeadline, retryStrategy).get());
        } catch (RuntimeException e) {
            return OptionalLong.empty();
        }
    }

    @Override
    public void advance(String projectionId, long position) {
        requireNonNull(projectionId, "projectionId cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("position must be positive but was " + position);
        }
        Runnable write = () -> mongoOperations.upsert(query(where(ID).is(projectionId)), new Update().max(POSITION, position), collection);
        executeWithRetry(write, __ -> !shutdown, retryStrategy).run();
    }

    @Override
    public boolean waitUntilApplied(String projectionId, long position, Duration timeout) {
        return waitUntilApplied(projectionId, position, timeout, pollBackoff);
    }

    /**
     * Overrides {@link AppliedPositionStore}'s default loop so each poll's read retries against {@link #retryStrategy}
     * bounded to this wait's own deadline, rather than {@link #retryStrategy}'s unbounded schedule. Without this, a
     * sustained store outage keeps a single read retrying forever and the wait never reaches the deadline check that
     * is supposed to end it. The loop shape (read, check, sleep, grow the backoff) otherwise matches the interface
     * default. Only the read is store-specific.
     */
    @Override
    public boolean waitUntilApplied(String projectionId, long position, Duration timeout, Backoff backoff) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        requireNonNull(backoff, "backoff cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("position must be positive but was " + position);
        }
        if (backoff instanceof Backoff.None) {
            throw new IllegalArgumentException("backoff cannot be Backoff.none(), a wait polls the store and needs a delay between polls. Use Backoff.fixed(..) or Backoff.exponential(..).");
        }
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long intervalNanos = switch (backoff) {
            case Backoff.Fixed fixed -> Duration.ofMillis(fixed.millis).toNanos();
            case Backoff.Exponential exponential -> exponential.initial.toNanos();
            case Backoff.None ignored -> throw new IllegalStateException("unreachable, rejected above");
        };
        while (true) {
            OptionalLong applied = readOnceBoundedBy(projectionId, deadlineNanos);
            if (applied.isPresent() && applied.getAsLong() >= position) {
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
}
