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
 * A projection built with {@code @Projection(recordAppliedAppends = true)}, or the corresponding recording wrapper
 * in the blocking and reactor projection DSLs, records into this store automatically while it is delivering live
 * events, and clears its own records on a reset (a rebuild, or any replay). Reading it back is a plain call to
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
     */
    void clear(String projectionId);

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
     * {@code true} once applied, {@code false} on timeout or if the waiting thread is interrupted, and never throws
     * for either, the same shape {@code Subscription.waitUntilStarted(Duration)} uses for a blocking wait elsewhere
     * in this library. An interrupt restores the thread's interrupt flag before returning, so a caller that needs to
     * tell the two apart can check {@link Thread#isInterrupted()} itself.
     * <p>
     * This is a plain read-and-sleep loop, since the record lives in a store this method cannot subscribe to for a
     * push notification. An implementation backed by a store that can push a change is free to override this method.
     * <p>
     * An {@code appendId} the projection never handles, because none of its events match the projection's selector,
     * is never recorded, and the wait times out. That is the correct answer rather than a defect, since a
     * projection that never applies the append has no effect for the caller to read.
     * <p>
     * {@code backoff} paces the polls only. It is not a retry policy for the store, which is an implementation's own
     * concern. The Mongo stores wrap their reads and writes in a {@code RetryStrategy}, so a transient store error
     * during a wait is absorbed there rather than ending the wait.
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
            if (hasApplied(projectionId, appendId)) {
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
     * An {@code AppliedAppendStore} backed by a plain map, for tests and single-process applications with no store
     * of their own to persist the recorded appends in. Recorded appends do not survive a restart.
     */
    static AppliedAppendStore inMemory() {
        return new AppliedAppendStore() {
            private final Map<String, Set<AppendId>> applied = new ConcurrentHashMap<>();

            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                requireNonNull(projectionId, "projectionId cannot be null");
                requireNonNull(appendId, "appendId cannot be null");
                applied.computeIfAbsent(projectionId, __ -> ConcurrentHashMap.newKeySet()).add(appendId);
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
