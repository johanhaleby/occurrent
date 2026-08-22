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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Records which appends a projection has applied, and lets a caller wait until the projection has applied a
 * particular append, for example one returned by the write the caller just made.
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>)
 * <p>
 * The recorded state is membership in a set, not a position on a line. Answering "has this projection applied this
 * append" needs no assumption about the order events arrive in and no notion of a completed prefix, both of which
 * the withdrawn position-based design (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>)
 * depended on and ADR 122 refuted.
 * <p>
 * This is the store a projection records into and a caller reads from. A projection records into it by calling
 * {@link #recordApplied(String, AppendId)} itself, or through the {@code @Projection(recordAppliedAppends = true)}
 * opt-in and its recording wrapper in the blocking and reactor projection DSLs. Reading is a plain call to
 * {@link #hasApplied(String, AppendId)} or {@link #waitUntilApplied(String, AppendId, Duration)}.
 */
@NullMarked
public interface AppliedAppendStore {

    /**
     * The pace {@link #waitUntilApplied(String, AppendId, Duration)} polls at, 25 ms doubling up to 250 ms. The
     * first poll stays fast so a projection that has already applied the append answers immediately, and the
     * interval grows the longer the wait runs, which is exactly when the extra polling would cost the store the
     * most.
     */
    Backoff DEFAULT_POLL_BACKOFF = Backoff.exponential(Duration.ofMillis(25), Duration.ofMillis(250), 2.0);

    /**
     * Records that {@code projectionId} has applied {@code appendId}. Recording the same append twice for the same
     * projection is a no-op the second time, so a caller does not need to check {@link #hasApplied(String, AppendId)}
     * first.
     */
    void recordApplied(String projectionId, AppendId appendId);

    /**
     * Whether {@code projectionId} has recorded {@code appendId}, and that record has not since been cleared.
     */
    boolean hasApplied(String projectionId, AppendId appendId);

    /**
     * Deletes every append recorded for {@code projectionId}. A projection whose read model is rebuilt from
     * scratch must not answer for appends recorded by whatever it was before the rebuild, and this is what removes
     * them.
     * <p>
     * Calling this directly, rather than through a replay a recorder observed itself, leaves that recorder's own
     * one-append dedup memory unaware the store changed underneath it. The next delivery of whichever append it last
     * recorded is skipped as an assumed repeat, and a wait for that specific append then times out rather than
     * seeing it recorded again, until a delivery of a different append updates that memory. The safe direction to
     * get wrong, since the failure is a wait that gives up rather than a record that should not exist.
     */
    void clear(String projectionId);

    /**
     * Rejects a {@code backoff} a wait loop cannot use as documented. This rejects {@link Backoff#none()}, an
     * exponential backoff whose initial or max interval is zero or negative, or whose multiplier is below 1.0 (this
     * also catches {@code NaN}, since every comparison against {@code NaN} except {@code !=} is false) and so
     * shrinks the interval back to zero after enough polls, becoming a busy loop against the store. It also rejects
     * an exponential backoff whose initial interval exceeds its max, which is not a busy loop but breaks the "first
     * poll stays fast" contract {@link #DEFAULT_POLL_BACKOFF} documents, since the wait loop's first sleep is
     * {@code initial} unchanged and an initial larger than max would suppress re-checking well past what max is
     * supposed to cap it at. Shared by this interface's own wait loop and by every store's override, so the rule is
     * enforced once rather than repeated per implementation.
     */
    static void rejectBusyLoopBackoff(Backoff backoff) {
        if (backoff instanceof Backoff.None) {
            throw new IllegalArgumentException("backoff cannot be Backoff.none(), a wait polls the store and needs a delay between polls. Use Backoff.fixed(..) or Backoff.exponential(..).");
        }
        if (backoff instanceof Backoff.Exponential exponential
                && (exponential.initial.isZero() || exponential.initial.isNegative()
                || exponential.max.isZero() || exponential.max.isNegative()
                || !(exponential.multiplier >= 1.0))) {
            throw new IllegalArgumentException("backoff's initial and max intervals must be positive and its multiplier must be at least 1.0, an interval that never grows past zero is a busy loop on the store.");
        }
        if (backoff instanceof Backoff.Exponential exponential && exponential.initial.compareTo(exponential.max) > 0) {
            throw new IllegalArgumentException("backoff's initial interval cannot exceed its max interval, the first poll would then sleep longer than the max this backoff is supposed to cap every poll at.");
        }
    }

    /**
     * As {@link #waitUntilApplied(String, AppendId, Duration, Backoff)}, pacing the polls with
     * {@link #DEFAULT_POLL_BACKOFF}. An implementation the application has configured a different pace for
     * overrides this to supply its own {@link Backoff}, which is how the Spring properties under
     * {@code occurrent.projection.applied-append} reach a caller that never names one.
     */
    default boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout) {
        return waitUntilApplied(projectionId, appendId, timeout, DEFAULT_POLL_BACKOFF);
    }

    /**
     * Blocks until {@code projectionId} has applied {@code appendId}, or {@code timeout} elapses. Returns
     * {@code true} once applied, and {@code false} on timeout, on interrupt, or if {@link #hasApplied(String, AppendId)}
     * keeps throwing, never throwing itself for any of those, the same shape {@code Subscription.waitUntilStarted(Duration)}
     * uses for a blocking wait elsewhere in this library. An interrupt restores the thread's interrupt flag before
     * returning, so a caller that needs to tell the cases apart can check {@link Thread#isInterrupted()} itself.
     * <p>
     * This is a plain read-and-sleep loop, since the record lives in a store this method cannot subscribe to for a
     * push notification. An implementation backed by a store that can push a change is free to override this method.
     * <p>
     * An {@code appendId} the projection never handles, because none of its events match the projection's selector,
     * is never recorded, and the wait times out. That is the correct answer rather than a defect, since a
     * projection that never applies the append has no effect for the caller to read.
     * <p>
     * {@code backoff} paces the polls only, not a retry policy for the store. A poll whose
     * {@link #hasApplied(String, AppendId)} throws counts as not yet applied, so a store failure keeps the wait
     * polling toward its deadline rather than ending it, the same absorb-and-poll behavior the Mongo stores
     * establish with their own {@code RetryStrategy}.
     * <p>
     * A wait always reads at least once, so a {@code timeout} of zero, or one a caller computed from a budget that
     * has already run out, still answers whether the append is applied instead of reporting that it is not. Every
     * implementation owes that, since a store answering {@code false} about an append it holds is a wrong answer
     * rather than a fast one.
     * <p>
     * The deadline is checked between polls, never during one, so this method returns after {@code timeout} plus
     * however long the {@link #hasApplied(String, AppendId)} call already in flight takes to answer. For
     * {@link #inMemory()} that is a map lookup and the difference is nothing. For an implementation that calls a
     * remote store, it is whatever that store's client waits before it gives up on a connection that has stopped
     * responding, so an implementation that wants {@code timeout} to hold needs its client configured with a
     * timeout of its own.
     *
     * @param appendId the append to wait for, never {@code null}. An append that persisted no events has no
     *                 identity to wait for in the first place, see {@code WriteResult}/{@code DcbAppendResult}.
     * @param timeout  how long to wait before giving up.
     * @param backoff  how the interval between polls grows. {@link Backoff#none()} is rejected, since a wait with no
     *                 delay between polls is a busy loop on the store.
     */
    default boolean waitUntilApplied(String projectionId, AppendId appendId, Duration timeout, Backoff backoff) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(appendId, "appendId cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        requireNonNull(backoff, "backoff cannot be null");
        rejectBusyLoopBackoff(backoff);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long intervalNanos = switch (backoff) {
            case Backoff.Fixed fixed -> Duration.ofMillis(fixed.millis).toNanos();
            case Backoff.Exponential exponential -> exponential.initial.toNanos();
            case Backoff.None ignored -> throw new IllegalStateException("unreachable, rejected above");
        };
        while (true) {
            boolean applied;
            try {
                applied = hasApplied(projectionId, appendId);
            } catch (RuntimeException e) {
                applied = false;
            }
            if (applied) {
                return true;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            // Nanosecond precision rather than toMillis(), which would truncate a sub-millisecond interval or
            // remaining time to Thread.sleep(0), never actually sleeping and spinning the loop instead.
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
     * How many recorded appends {@link #inMemory()} keeps per projection, 10,000. Ten times the number of appends
     * the recording wrapper lets wait for a clear before it drops the oldest, and far more than a wait measured in
     * seconds can still be looking for.
     */
    int DEFAULT_IN_MEMORY_MAX_RECORDED_APPENDS_PER_PROJECTION = 10_000;

    /**
     * An {@code AppliedAppendStore} backed by a plain map, for tests and single-process applications with no store
     * of their own to persist the recorded appends in. Keeps
     * {@value #DEFAULT_IN_MEMORY_MAX_RECORDED_APPENDS_PER_PROJECTION} recorded appends per projection, see
     * {@link #inMemory(int)} to choose a different number. Recorded appends do not survive a restart.
     */
    static AppliedAppendStore inMemory() {
        return inMemory(DEFAULT_IN_MEMORY_MAX_RECORDED_APPENDS_PER_PROJECTION);
    }

    /**
     * As {@link #inMemory()}, keeping {@code maxRecordedAppendsPerProjection} recorded appends per projection.
     * Recording one more than that evicts the projection's oldest recorded append, and a wait for an evicted append
     * times out, the same answer the Mongo stores give for a record their TTL index has expired.
     * <p>
     * Nothing limits how many projections the store holds appends for, since a projection id comes from the
     * application's own configuration rather than from anything it records.
     *
     * @param maxRecordedAppendsPerProjection how many appends each projection keeps, at least 1.
     */
    static AppliedAppendStore inMemory(int maxRecordedAppendsPerProjection) {
        if (maxRecordedAppendsPerProjection < 1) {
            throw new IllegalArgumentException("maxRecordedAppendsPerProjection must be at least 1, a store that keeps no append at all answers false for one it was just told about.");
        }
        return new AppliedAppendStore() {
            private final Map<String, Set<AppendId>> applied = new ConcurrentHashMap<>();

            // Insertion-ordered rather than access-ordered, so a read never lets a newer append outlive an older
            // one. Evicting oldest first is what the Mongo stores' TTL index does.
            private Set<AppendId> boundedSet() {
                Map<AppendId, Boolean> bounded = new LinkedHashMap<>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<AppendId, Boolean> eldest) {
                        return size() > maxRecordedAppendsPerProjection;
                    }
                };
                return Collections.newSetFromMap(Collections.synchronizedMap(bounded));
            }

            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                requireNonNull(projectionId, "projectionId cannot be null");
                requireNonNull(appendId, "appendId cannot be null");
                applied.computeIfAbsent(projectionId, __ -> boundedSet()).add(appendId);
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                requireNonNull(projectionId, "projectionId cannot be null");
                requireNonNull(appendId, "appendId cannot be null");
                Set<AppendId> appendIds = applied.get(projectionId);
                return appendIds != null && appendIds.contains(appendId);
            }

            @Override
            public void clear(String projectionId) {
                requireNonNull(projectionId, "projectionId cannot be null");
                applied.remove(projectionId);
            }
        };
    }
}
