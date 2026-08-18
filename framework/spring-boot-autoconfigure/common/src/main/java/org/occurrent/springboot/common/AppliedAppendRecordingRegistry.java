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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.ReplayPhase;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * The pacing behind the scheduled poll each {@code @Projection(recordAppliedAppends = true)} registrar runs, shared
 * by the blocking and reactor stacks so the schedule cannot drift apart between them
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 7). Pure state, no thread or scheduler of its own: a registrar owns the scheduler, asks
 * {@link #dueInNanos(String)} for how long to wait before the next tick, and calls {@link #tick(String)} when it
 * fires, then asks {@link #dueInNanos(String)} again to reschedule itself. This is what makes the poll
 * self-rescheduling per projection instead of a global tick that wakes up every registered projection whether or not
 * it is due.
 * <p>
 * Not thread-safe across concurrent calls for the <em>same</em> {@code projectionId}: a registrar that runs its
 * scheduler single-threaded (blocking) or on a single-worker {@code Scheduler} (reactor) never calls
 * {@link #tick(String)} for one id from two threads at once, which is the only guarantee this class relies on.
 */
@NullMarked
public final class AppliedAppendRecordingRegistry {

    private final long initialNanos;
    private final long maxNanos;
    private final double multiplier;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    private record Entry(ReplayPhase phase, AppliedAppendRecorder recorder, long[] intervalNanos) {
    }

    public AppliedAppendRecordingRegistry(Duration initial, Duration max, double multiplier) {
        requireNonNull(initial, "initial cannot be null");
        requireNonNull(max, "max cannot be null");
        this.initialNanos = initial.toNanos();
        this.maxNanos = max.toNanos();
        this.multiplier = multiplier;
    }

    /**
     * Registers {@code projectionId} for the poll, due for its first {@link #tick(String)} after
     * {@link #dueInNanos(String)} nanoseconds, which starts at the configured {@code initial} interval.
     */
    public void register(String projectionId, ReplayPhase phase, AppliedAppendRecorder recorder) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(phase, "phase cannot be null");
        requireNonNull(recorder, "recorder cannot be null");
        entries.put(projectionId, new Entry(phase, recorder, new long[]{initialNanos}));
    }

    /**
     * How long, in nanoseconds, until {@code projectionId} is next due for {@link #tick(String)}, from its current
     * interval. Read this again after every {@link #tick(String)} to reschedule, since a tick can change the
     * interval.
     *
     * @throws IllegalArgumentException if {@code projectionId} was never registered
     */
    public long dueInNanos(String projectionId) {
        return entryFor(projectionId).intervalNanos()[0];
    }

    /**
     * Asks {@code projectionId}'s {@link ReplayPhase}. Replaying calls {@link AppliedAppendRecorder#replayObserved()}
     * and resets the interval to {@code initial}, so a projection just seen replaying, or one that has just
     * registered, is polled at the fast end. Live retries a clear the recorder already owes from an earlier
     * replay, through {@link AppliedAppendRecorder#retryPendingClear()}, since the phase no longer reporting a
     * replay is not the same as a clear that replay caused having succeeded, then grows the interval by
     * {@code multiplier}, capped at {@code max}.
     *
     * @throws IllegalArgumentException if {@code projectionId} was never registered
     */
    public void tick(String projectionId) {
        Entry entry = entryFor(projectionId);
        if (entry.phase().isReplaying()) {
            entry.recorder().replayObserved();
            entry.intervalNanos()[0] = initialNanos;
        } else {
            entry.recorder().retryPendingClear();
            entry.intervalNanos()[0] = Math.min((long) (entry.intervalNanos()[0] * multiplier), maxNanos);
        }
    }

    private Entry entryFor(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Entry entry = entries.get(projectionId);
        if (entry == null) {
            throw new IllegalArgumentException("'%s' is not registered on this poll.".formatted(projectionId));
        }
        return entry;
    }

    /**
     * The refusal a {@code recordAppliedAppends = true} registrar throws at registration when no
     * {@code AppliedAppendStore} bean exists, worded identically on both stacks so the two never drift apart.
     */
    public static IllegalStateException noAppliedAppendStoreConfigured(String projectionId) {
        return new IllegalStateException(("@Projection '%s' sets recordAppliedAppends = true, but no AppliedAppendStore bean exists. " +
                "Declare one, or use one of the Spring Boot Mongo starters, which auto-configure a zero-config default.").formatted(projectionId));
    }

    /**
     * The refusal a {@code recordAppliedAppends = true} registrar throws at registration when combined with
     * {@code mode = SYNCHRONOUS}, worded identically on both stacks so the two never drift apart.
     */
    public static IllegalArgumentException recordAppliedAppendsWithSynchronousMode(String projectionId) {
        return new IllegalArgumentException(("@Projection '%s' cannot combine recordAppliedAppends = true with mode = SYNCHRONOUS: " +
                "a synchronous projection already updates inside the write and answers read-your-writes without it.").formatted(projectionId));
    }
}
