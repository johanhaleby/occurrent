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
 * Not thread-safe across concurrent calls for the <em>same</em> {@code projectionId}. A registrar's self-rescheduling
 * only asks {@link #dueInNanos(String)} again and reschedules once {@link #tick(String)} for that id has already
 * returned, never before, so the same id's ticks stay serialized regardless of how many worker threads the
 * scheduler runs overall (a virtual thread per fired tick on the blocking stack, a {@code boundedElastic} worker on
 * the reactor stack). Two different ids can, and are meant to, tick concurrently.
 */
@NullMarked
public final class AppliedAppendRecordingRegistry {

    private final long initialNanos;
    private final long maxNanos;
    private final double multiplier;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    private record Entry(AppliedAppendRecorder recorder, long[] intervalNanos) {
    }

    /**
     * @param initial    the interval a newly registered projection, or one just seen replaying, is next due at.
     * @param max        the interval growth is capped at once a projection has stayed live for a while.
     * @param multiplier what the interval is multiplied by after each tick that found the projection live.
     * @throws IllegalArgumentException if {@code initial} or {@code max} is zero or negative, if {@code initial}
     *                                   exceeds {@code max}, or if {@code multiplier} is below 1.0 (this also
     *                                   catches {@code NaN}, since every comparison against it except {@code !=}
     *                                   is false). Each of those would converge the poll to a zero-delay busy loop
     *                                   rather than the paced schedule this class exists to keep it on.
     */
    public AppliedAppendRecordingRegistry(Duration initial, Duration max, double multiplier) {
        requireNonNull(initial, "initial cannot be null");
        requireNonNull(max, "max cannot be null");
        if (initial.isZero() || initial.isNegative()) {
            throw new IllegalArgumentException("initial must be positive, a poll ticking at a zero or negative interval is a busy loop.");
        }
        if (max.isZero() || max.isNegative()) {
            throw new IllegalArgumentException("max must be positive, a poll capped at a zero or negative interval is a busy loop.");
        }
        if (initial.compareTo(max) > 0) {
            throw new IllegalArgumentException("initial cannot exceed max, the first tick would then wait longer than max is supposed to cap it at.");
        }
        if (!(multiplier >= 1.0)) {
            throw new IllegalArgumentException("multiplier must be at least 1.0, a smaller value shrinks the interval back toward zero and becomes a busy loop.");
        }
        this.initialNanos = initial.toNanos();
        this.maxNanos = max.toNanos();
        this.multiplier = multiplier;
    }

    /**
     * Registers {@code projectionId} for the poll, due for its first {@link #tick(String)} after
     * {@link #dueInNanos(String)} nanoseconds, which starts at the configured {@code initial} interval.
     */
    public void register(String projectionId, AppliedAppendRecorder recorder) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(recorder, "recorder cannot be null");
        entries.put(projectionId, new Entry(recorder, new long[]{initialNanos}));
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
     * Calls {@link AppliedAppendRecorder#pollReplayPhase()}, which checks the phase and reacts to it atomically
     * rather than this class checking it first and dispatching from a reading that could go stale before the
     * recorder acts on it. Replaying resets the interval to {@code initial}, so a projection just seen replaying, or
     * one that has just registered, is polled at the fast end. Live grows the interval by {@code multiplier}, capped
     * at {@code max}.
     *
     * @throws IllegalArgumentException if {@code projectionId} was never registered
     */
    public void tick(String projectionId) {
        Entry entry = entryFor(projectionId);
        boolean replaying = entry.recorder().pollReplayPhase();
        if (replaying) {
            entry.intervalNanos()[0] = initialNanos;
        } else {
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

    /**
     * The startup WARN a {@code recordAppliedAppends = true} registrar logs when the resolved start position or
     * composition never replays (ADR 132 decision 9's third case), worded identically on both stacks so the two
     * never drift apart.
     */
    public static String recordAppliedAppendsNeverResetsAutomatically(String projectionId) {
        return ("@Projection '%s' sets recordAppliedAppends = true, but its resolved start position or composition never replays, " +
                "so its recorded memberships survive a read-model rebuild until the TTL evicts them or an operator clears them.").formatted(projectionId);
    }
}
