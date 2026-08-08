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
 * a read or a write that failed, so a transient store error neither fails the projection's delivery on the fold path
 * nor ends a caller's wait on the read path. The {@link Backoff} decides how long
 * {@link #waitUntilApplied(String, long, Duration)} sleeps between polls that succeeded and simply found the
 * projection still behind.
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
        Supplier<OptionalLong> read = () -> {
            Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection);
            if (document == null) {
                return OptionalLong.empty();
            }
            Number position = document.get(POSITION, Number.class);
            return position == null ? OptionalLong.empty() : OptionalLong.of(position.longValue());
        };
        return requireNonNull(executeWithRetry(read, __ -> !shutdown, retryStrategy).get());
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

    @PreDestroy
    void shutdown() {
        shutdown = true;
    }

    private static RetryStrategy defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f);
    }
}
