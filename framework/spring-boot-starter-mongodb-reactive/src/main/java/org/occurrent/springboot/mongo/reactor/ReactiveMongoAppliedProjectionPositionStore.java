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
import org.occurrent.dsl.projection.AppliedProjectionPositionStore;
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
 * The {@link AppliedProjectionPositionStore} the reactive Mongo starter contributes as
 * {@code @Projection(recordAppliedPosition = true)}'s zero-config default. {@link AppliedProjectionPositionStore} is a
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
 * or a write that failed, so a transient store error neither fails the projection's delivery on the fold path nor a
 * plain {@link #appliedPosition(String)} call. {@link #waitUntilApplied(String, long, Duration)} is the one
 * exception. Its own reads retry on the same {@link Retry}, but bounded to the wait's deadline, and a read that is
 * still failing once the deadline arrives, or once {@link #retry} itself gives up, answers empty rather than
 * throwing, so a sustained store outage always surfaces as a timeout, never as an exception. The {@link Backoff}
 * decides how long a wait sleeps between polls that succeeded and simply found the projection still behind.
 * {@link Retry} rather than the blocking {@code RetryStrategy} because this is the reactive stack, matching how the
 * reactive starter retries elsewhere.
 * <p>
 * {@link #defaultRetry()} gives up after a fixed number of attempts rather than retrying forever, a deliberate
 * divergence from the blocking store's default, which keeps {@link #appliedPosition(String)} and
 * {@link #advance(String, long)} from blocking a direct caller indefinitely under a sustained outage. An application
 * that needs parity with the blocking store's unbounded retry should supply its own {@link Retry}.
 */
@NullMarked
class ReactiveMongoAppliedProjectionPositionStore implements AppliedProjectionPositionStore {

    private static final String ID = "_id";
    private static final String POSITION = "position";

    private final ReactiveMongoOperations mongoOperations;
    private final String collection;
    private final Retry retry;
    private final Backoff pollBackoff;

    /**
     * Retries a failing read or write with backoff from 100 ms up to 2 seconds, giving up after 5 retries (6
     * attempts total) and surfacing the last failure. The blocking store does not give up this way, it retries the
     * same backoff forever, since it keeps {@code advance(..)} durable under an outage. This store's default gives
     * up so a direct call to {@link #appliedPosition(String)} or {@link #advance(String, long)} does not block a
     * caller indefinitely. {@link #waitUntilApplied(String, long, Duration)} never inherits that exhaustion though,
     * its own reads answer empty rather than surfacing the failure, so a wait against this default still resolves
     * by its own deadline. Polls a wait at {@link AppliedProjectionPositionStore#DEFAULT_POLL_BACKOFF}.
     */
    ReactiveMongoAppliedProjectionPositionStore(ReactiveMongoOperations mongoOperations, String collection) {
        this(mongoOperations, collection, defaultRetry(), DEFAULT_POLL_BACKOFF);
    }

    ReactiveMongoAppliedProjectionPositionStore(ReactiveMongoOperations mongoOperations, String collection, Retry retry, Backoff pollBackoff) {
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
        return toPosition(document);
    }

    /**
     * A read for {@link #waitUntilApplied(String, long, Duration, Backoff)} whose retries stop once
     * {@code deadlineNanos} ({@link System#nanoTime()} scale) passes, rather than continuing on {@link #retry}'s own
     * schedule. Blocking on the retried {@link Document} read with the remaining duration as the block timeout is
     * what bounds the retries themselves, not just the wait loop around them. {@link #retry}'s default is finite, so
     * a sustained outage can also exhaust the retry itself well before the deadline. Either way the read answers
     * empty rather than throwing, since a wait polls for "not caught up yet" and leaves ending the wait to its own
     * deadline check, never to the read's failure.
     */
    private OptionalLong readOnceBoundedBy(String projectionId, long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return OptionalLong.empty();
        }
        try {
            Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection)
                    .retryWhen(retry)
                    .block(Duration.ofNanos(remainingNanos));
            return toPosition(document);
        } catch (RuntimeException ignored) {
            return OptionalLong.empty();
        }
    }

    private static OptionalLong toPosition(Document document) {
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

    /**
     * Overrides {@link AppliedProjectionPositionStore}'s default loop so each poll's read retries against {@link #retry}
     * bounded to this wait's own deadline, rather than continuing to block past it. Without this, a sustained store
     * outage keeps a single read retrying (and blocking) past the wait's deadline and the wait throws instead of
     * reaching the deadline check that is supposed to end it. The loop shape (read, check, sleep, grow the backoff)
     * otherwise matches the interface default and the blocking store's own override. Only the read is store-specific.
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

    private static Retry defaultRetry() {
        return Retry.backoff(5, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
