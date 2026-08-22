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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedAppendStore} the reactive Mongo starter auto-configures when the application declares none. A
 * projection records into it directly for now, through {@link AppliedAppendStore#recordApplied(String, AppendId)}.
 * A future {@code @Projection(recordAppliedAppends = true)} opt-in that records automatically, from a callback
 * already running on {@code boundedElastic}, is not part of this release. {@link AppliedAppendStore} is a
 * blocking-shaped interface on both stacks regardless, so this store bridges to the underlying reactive Mongo calls
 * with {@code block()}, the same direction that future callback would bridge in.
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
 * delivered event nor a plain {@link #hasApplied(String, AppendId)} call, until it runs out of attempts. {@link #waitUntilApplied(String, AppendId, Duration)}
 * is the one exception. Its own reads retry on the same {@link Retry}, but only until the wait's own deadline, and a
 * read that is still failing once the deadline arrives, or once {@link #retry} itself gives up, answers {@code false}
 * rather than throwing, so a sustained store outage always surfaces as a timeout, never as an exception. The
 * {@link Backoff} decides how long a wait sleeps between polls that succeeded and simply found the append not yet
 * applied. {@link Retry} rather than the blocking {@code RetryStrategy} because this is the reactive stack, matching
 * how the reactive starter retries elsewhere.
 * <p>
 * {@link #defaultRetry()} attempts a call {@link #DEFAULT_MAX_ATTEMPTS} times, the same number the blocking store
 * uses, so a store that stays unreachable stops a projection's delivery thread rather than holding it for as long
 * as the outage lasts, which is what lets a clear that keeps failing stop its recorder (ADR 132 decision 7). An
 * application that wants a different bound supplies its own {@link Retry}.
 * <p>
 * Every call outside a wait blocks for as long as the underlying {@code Mono} takes to answer. A MongoDB client
 * left with no timeout of its own does not answer at all while a connection it has accepted stops responding, so
 * configure one on the client if these calls have to return, for example through {@code spring.mongodb.uri}.
 * {@link #waitUntilApplied(String, AppendId, Duration)} bounds each poll that has time left on exactly that time,
 * so those polls need no such setting. The one read it must make gets no such limit, because a timeout of zero
 * or one that has already elapsed leaves nothing to bound it with, and that read depends on the client's timeout
 * exactly as the calls above it do.
 */
@NullMarked
public class ReactiveMongoAppliedAppendStore implements AppliedAppendStore {

    private static final String PROJECTION_ID = "projectionId";
    private static final String APPEND_ID = "appendId";
    private static final String RECORDED_AT = "recordedAt";
    private static final String PROJECTION_ID_APPEND_ID_INDEX = "projectionId_appendId";
    private static final String RECORDED_AT_TTL_INDEX = "recordedAt_ttl";
    private static final String INDEX_OPTIONS_CONFLICT = "IndexOptionsConflict";

    /**
     * How many times a read or a write calls MongoDB before it gives up, 10. This is a count of attempts and not a
     * length of time. A call that fails at once is retried on this store's 100 ms to 2 s backoff, so ten of those
     * take about 11 seconds, while a call to a server that is not answering spends the client's own server
     * selection timeout, 30 seconds by default, on each of the ten. Only a timeout on the client limits the time,
     * the same reason a wait needs one, so configure one there when the wall clock is what matters. The same number the blocking store uses.
     */
    public static final int DEFAULT_MAX_ATTEMPTS = 10;

    /**
     * The number of attempts after which this store stops a policy that has not stopped itself, 1000, whatever
     * {@link Retry} it was given. A {@link Retry} is an abstract class with no accessor reporting whether it stops on its own,
     * so a store handed one that never gives up cannot reject it at construction and would otherwise call MongoDB
     * for as long as an outage lasted. The store makes at most one attempt beyond this number, because a policy
     * that stops at or before it is left to stop on its own, including how it maps an exhausted retry. Two orders
     * of magnitude above {@link #DEFAULT_MAX_ATTEMPTS}, and {@code occurrent.projection.applied-append.max-attempts}
     * is rejected above it, so a configured policy is never shortened here. The same number the blocking store uses.
     */
    public static final int MAX_ATTEMPTS_CEILING = 1000;

    private final ReactiveMongoOperations mongoOperations;
    private final String collection;
    private final Duration retention;
    private final Retry retry;
    private final Backoff pollBackoff;

    private volatile boolean indexesEnsured = false;
    private volatile ConflictingIndexException indexConflict;

    /**
     * Retries a failing read or write with backoff from 100 ms up to 2 seconds, {@link #DEFAULT_MAX_ATTEMPTS}
     * times, so a transient outage of the store does not turn a direct call to {@link #hasApplied(String, AppendId)}
     * or {@link #recordApplied(String, AppendId)} into a failure, and one that keeps failing stops being called.
     * ADR 132 decision 5 asks for the retry so that a transient outage does not fail a wait, and decision 7 needs
     * it to end, which is why it counts attempts rather than going on. {@link #waitUntilApplied(String, AppendId, Duration)}
     * is unaffected either way, its own reads answer {@code false} rather than failing, so a wait resolves by its
     * own deadline regardless of what a caller supplies here. Polls a wait at
     * {@link AppliedAppendStore#DEFAULT_POLL_BACKOFF}.
     */
    public ReactiveMongoAppliedAppendStore(ReactiveMongoOperations mongoOperations, String collection, Duration retention) {
        this(mongoOperations, collection, retention, defaultRetry(), DEFAULT_POLL_BACKOFF);
    }

    /**
     * @param collection   rejected if blank, for the same reason a negative {@code retention} is. MongoDB rejects
     *                     the name, and a permanent configuration error belongs at startup rather than on every
     *                     read and write this store makes.
     * @param retry        wrapped so that it never repeats a {@link ConflictingIndexException}, whatever policy it
     *                     is. Error 85 never becomes anything else, so a caller cannot opt into retrying it and
     *                     cannot reintroduce the startup hang by supplying a policy of its own.
     * @param retention    rejected if negative, since MongoDB would then reject the TTL index this store creates
     *                      from it, and a {@code Retry} would otherwise retry that permanent
     *                      configuration error rather than fail once at startup.
     * @param pollBackoff  rejected the same way {@link #waitUntilApplied(String, AppendId, Duration, Backoff)}
     *                     rejects one, so a {@code pollBackoff} that would busy-loop the store fails here, when the
     *                     bean is built, rather than at the first wait a caller happens to make.
     */
    public ReactiveMongoAppliedAppendStore(ReactiveMongoOperations mongoOperations, String collection, Duration retention, Retry retry, Backoff pollBackoff) {
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
        this.retry = neverRetryingAConflictingIndex(requireNonNull(retry, "retry cannot be null"));
        AppliedAppendStore.rejectBusyLoopBackoff(requireNonNull(pollBackoff, "pollBackoff cannot be null"));
        this.pollBackoff = pollBackoff;
    }

    /**
     * Ensures the compound unique index and the TTL index exist, once, the first time this store is actually asked
     * to do anything. Composed entirely as {@code Mono} operations rather than a nested {@code block()}, as an
     * earlier version of this method had. {@code retryWhen}'s delayed resubscription runs on
     * {@code Schedulers.parallel()}, whose worker threads this project's dependencies do not instrument to reject a
     * blocking call, so a nested {@code block()} there does not throw, but it does hold one of that shared pool's
     * few threads for the length of the Mongo call on every retry, which is worth avoiding regardless of whether it
     * throws. Wrapped in {@link Mono#defer(java.util.function.Supplier)} so a retry re-checks
     * {@link #indexesEnsured} and rebuilds this {@code Mono} fresh, the same reason
     * {@link #recordApplied(String, AppendId)}'s own upsert is deferred. A race between two threads both finding
     * {@link #indexesEnsured} false is at worst wasted work and at best exactly the {@code IndexOptionsConflict}
     * path below already handles, so nothing here needs to serialize against it.
     */
    private Mono<Void> ensureIndexesOnce() {
        return Mono.defer(() -> {
            if (indexesEnsured) {
                return Mono.empty();
            }
            ConflictingIndexException conflict = indexConflict;
            if (conflict != null) {
                return Mono.error(conflict);
            }
            ReactiveIndexOperations indexOps = mongoOperations.indexOps(collection);
            // No collMod for this one, unlike the TTL index. collMod changes expireAfterSeconds and hidden, and it
            // cannot change a partial filter or a collation at all, so there is nothing to alter this index into.
            // An operator drops it, and the message says so rather than leaving error 85 to be retried.
            Mono<Void> uniqueIndex = indexOps.ensureIndex(new Index().on(PROJECTION_ID, Direction.ASC).on(APPEND_ID, Direction.ASC).named(PROJECTION_ID_APPEND_ID_INDEX).unique()).then()
                    .onErrorMap(ReactiveMongoAppliedAppendStore::isIndexOptionsConflict,
                            e -> remember(new ConflictingIndexException("Collection '" + collection + "' already has an index named '" + PROJECTION_ID_APPEND_ID_INDEX + "' whose options differ from the unique index on " + PROJECTION_ID + " and " + APPEND_ID + " this store needs. Drop that index and restart the application, since this store remembers the conflict rather than asking MongoDB again on every call.", e)));
            Mono<Void> ttlIndex = indexOps.ensureIndex(new Index().on(RECORDED_AT, Direction.ASC).named(RECORDED_AT_TTL_INDEX).expire(retention)).then()
                    .onErrorResume(e -> isIndexOptionsConflict(e)
                            ? indexOps.alterIndex(RECORDED_AT_TTL_INDEX, IndexOptions.expireAfter(retention))
                            : Mono.error(e));
            return uniqueIndex.then(ttlIndex).doOnSuccess(ignored -> indexesEnsured = true);
        });
    }

    /**
     * Remembers an index whose options MongoDB will never accept, the same way success is remembered, so the answer
     * costs one attempt per process rather than one per call. A wait polls, so without that memory a permanent
     * index conflict would be re-attempted on every poll and the number of calls this store makes would depend on
     * how long the caller waits. Dropping the conflicting index therefore needs a restart to take effect, which is
     * the same lifetime the successful case already had.
     */
    private ConflictingIndexException remember(ConflictingIndexException conflict) {
        indexConflict = conflict;
        return conflict;
    }

    @Override
    public void recordApplied(String projectionId, AppendId appendId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        // Mono.defer so a retried upsert builds a fresh Update, and with it a fresh new Date(), on every attempt.
        // Built once outside a defer, a retry would resubscribe to the same upsert Mono and reuse the first
        // attempt's timestamp, so a record that only succeeds after several retries would carry a recordedAt from
        // well before the insert, shortening its actual time in the TTL index.
        ensureIndexesOnce()
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
        return ensureIndexesOnce()
                .then(mongoOperations.exists(query(where(PROJECTION_ID).is(projectionId).and(APPEND_ID).is(appendId.value().toString())), collection));
    }

    /**
     * A read for {@link #waitUntilApplied(String, AppendId, Duration, Backoff)} whose retries stop once
     * {@code deadlineNanos} ({@link System#nanoTime()} scale) passes, rather than continuing on {@link #retry}'s own
     * schedule. Blocking on the retried read with the remaining duration as the block timeout is what stops the
     * retries themselves at the deadline, not just the wait loop around them. A sustained outage exhausts
     * {@link #retry}'s own attempts too, well before a long deadline. Either way the read answers {@code false}
     * rather than throwing. A wait polls for
     * "not applied yet", and its own deadline check, not the read's failure, is what ends it. This also covers a
     * fresh store's index setup, since
     * {@link #existsWithIndexesEnsured} runs it in the same chain as the read, retried and limited to the same
     * deadline.
     */
    private boolean readOnceBoundedBy(String projectionId, AppendId appendId, long deadlineNanos, AtomicBoolean anyReadStarted) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        boolean firstRead = anyReadStarted.compareAndSet(false, true);
        if (remainingNanos <= 0 && !firstRead) {
            return false;
        }
        try {
            // Retries stop at the deadline rather than running the policy's whole budget, which is what the blocking
            // store gets from folding the deadline into its retry predicate. Without it, an already-elapsed timeout
            // fell through to an unbounded block and a failing store cost every attempt the policy allowed.
            Mono<Boolean> read = existsWithIndexesEnsured(projectionId, appendId).retryWhen(retryUntil(deadlineNanos));
            // The first read of a wait runs whatever the deadline says, so a timeout that has already elapsed still
            // gets one answer rather than a false one. With no remaining time to bound it, that single read blocks
            // the way a plain hasApplied does, which is the one place a wait depends on the client's own timeout.
            Boolean applied = remainingNanos > 0 ? read.block(Duration.ofNanos(remainingNanos)) : read.block();
            return Boolean.TRUE.equals(applied);
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    @Override
    public void clear(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        ensureIndexesOnce()
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
        AtomicBoolean anyReadStarted = new AtomicBoolean();
        while (true) {
            if (readOnceBoundedBy(projectionId, appendId, deadlineNanos, anyReadStarted)) {
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

    /**
     * {@code Retry.backoff} counts retries rather than total calls, unlike the blocking {@code RetryStrategy}'s
     * {@code maxAttempts}, so {@link #DEFAULT_MAX_ATTEMPTS} minus one is what makes both stacks call the store the
     * same number of times.
     */
    static Retry defaultRetry() {
        return Retry.backoff(DEFAULT_MAX_ATTEMPTS - 1L, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    /**
     * Wraps this store's {@link #retry} so it stops once {@code deadlineNanos} ({@link System#nanoTime()} scale)
     * has passed, rather than spending attempts a wait has no time left for. Checked when an attempt fails and
     * before the policy would sleep, the same position the blocking store's retry predicate checks it.
     * <p>
     * Built on the delegate's own {@link Retry#retryContext()} rather than an empty one, so a caller's hooks and
     * policies still read back what they put there through {@code RetrySignal.retryContextView()}.
     */
    private Retry retryUntil(long deadlineNanos) {
        Retry delegate = retry;
        return new Retry(delegate.retryContext()) {
            @Override
            public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
                return delegate.generateCompanion(retrySignals.handle((signal, sink) -> {
                    if (System.nanoTime() >= deadlineNanos) {
                        sink.error(signal.failure());
                    } else {
                        sink.next(signal);
                    }
                }));
            }
        };
    }

    /**
     * Wraps a {@link Retry} so a {@link ConflictingIndexException} ends the call instead of being repeated, and so
     * no policy runs past {@link #MAX_ATTEMPTS_CEILING} attempts, whatever the wrapped policy would have done with
     * either. Reactor takes the retry policy as one object and gives a store no separate say in which failures it
     * repeats, unlike the blocking {@code executeWithRetry}, so the store takes that say back here rather than
     * asking every caller to remember it. Erroring the companion is what stops {@code retryWhen}, and every other
     * failure reaches the wrapped policy unchanged.
     * <p>
     * Built on the delegate's own {@link Retry#retryContext()}, for the same reason {@link #retryUntil(long)} is.
     */
    private static Retry neverRetryingAConflictingIndex(Retry delegate) {
        return new Retry(delegate.retryContext()) {
            @Override
            public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
                return delegate.generateCompanion(retrySignals.handle((signal, sink) -> {
                    if (signal.failure() instanceof ConflictingIndexException
                            || signal.totalRetries() >= MAX_ATTEMPTS_CEILING) {
                        sink.error(signal.failure());
                    } else {
                        sink.next(signal);
                    }
                }));
            }
        };
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
