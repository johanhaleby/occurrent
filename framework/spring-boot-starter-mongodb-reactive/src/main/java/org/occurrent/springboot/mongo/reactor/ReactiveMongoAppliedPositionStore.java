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

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedPositionStore;
import org.occurrent.retry.Backoff;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedPositionStore} the reactive Mongo starter contributes as
 * {@code @Projection(recordAppliedPosition = true)}'s zero-config default. {@link AppliedPositionStore} is a
 * blocking-shaped interface on both stacks, called from the reactor recorder's {@code doOnSuccess} callback, which
 * already runs on {@code boundedElastic}, so blocking on the underlying reactive Mongo call here is the same bridge
 * the rest of this reactor stack makes in the other direction.
 * <p>
 * One document per projection id, {@code _id} the projection id and {@code position} the applied position.
 * {@link #advance(String, long)} writes with MongoDB's {@code $max} update operator in one round trip, so the
 * never-moves-backwards guarantee holds under concurrent advances for the same projection id, with no
 * read-modify-write race.
 * <p>
 * Two different mechanisms pace two different things here, and they do not overlap. The {@link Retry} retries a read
 * or a write that failed, so a transient store error neither fails the projection's delivery on the fold path nor
 * ends a caller's wait on the read path. The {@link Backoff} decides how long
 * {@link #waitUntilApplied(String, long, Duration)} sleeps between polls that succeeded and simply found the
 * projection still behind. {@link Retry} rather than the blocking {@code RetryStrategy} because this is the reactive
 * stack, matching how the reactive starter retries elsewhere.
 * <p>
 * {@link #defaultRetry()} gives up after a fixed number of attempts rather than retrying forever, a deliberate
 * divergence from the blocking store's default. This store has no {@code waitUntilApplied} override with a
 * wait-local deadline to fall back on, so bounding the retry itself is what keeps a sustained outage from blocking
 * {@link #appliedPosition(String)} and {@link #advance(String, long)} indefinitely. The two stores do not behave
 * the same way under a sustained outage, and an application that needs parity should supply its own {@link Retry}.
 */
@NullMarked
class ReactiveMongoAppliedPositionStore implements AppliedPositionStore {

    private static final String ID = "_id";
    private static final String POSITION = "position";

    private final ReactiveMongoOperations mongoOperations;
    private final String collection;
    private final Retry retry;
    private final Backoff pollBackoff;

    /**
     * Retries a failing read or write with backoff from 100 ms up to 2 seconds, giving up after 5 retries (6
     * attempts total) and surfacing the last failure. The blocking store does not give up this way.
     * {@code MongoAppliedPositionStore} retries the same backoff forever, since it keeps {@code advance(..)} durable
     * under an outage and bounds only {@code waitUntilApplied(..)}'s own reads to the wait's deadline instead. This
     * store has no such wait-local bound, so its default retries a fixed number of times rather than forever, and
     * polls a wait at {@link AppliedPositionStore#DEFAULT_POLL_BACKOFF}.
     */
    ReactiveMongoAppliedPositionStore(ReactiveMongoOperations mongoOperations, String collection) {
        this(mongoOperations, collection, defaultRetry(), DEFAULT_POLL_BACKOFF);
    }

    ReactiveMongoAppliedPositionStore(ReactiveMongoOperations mongoOperations, String collection, Retry retry, Backoff pollBackoff) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
        this.retry = requireNonNull(retry, "retry cannot be null");
        this.pollBackoff = requireNonNull(pollBackoff, "pollBackoff cannot be null");
    }

    @Override
    public OptionalLong appliedPosition(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection)
                .retryWhen(retry)
                .block();
        if (document == null) {
            return OptionalLong.empty();
        }
        Number position = document.get(POSITION, Number.class);
        return position == null ? OptionalLong.empty() : OptionalLong.of(position.longValue());
    }

    @Override
    public void advance(String projectionId, long position) {
        requireNonNull(projectionId, "projectionId cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("position must be positive but was " + position);
        }
        mongoOperations.upsert(query(where(ID).is(projectionId)), new Update().max(POSITION, position), collection)
                .retryWhen(retry)
                .block();
    }

    @Override
    public boolean waitUntilApplied(String projectionId, long position, Duration timeout) {
        return waitUntilApplied(projectionId, position, timeout, pollBackoff);
    }

    private static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
