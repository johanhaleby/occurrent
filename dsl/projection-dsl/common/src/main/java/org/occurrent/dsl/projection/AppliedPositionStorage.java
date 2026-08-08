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

import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Records the global position a projection has applied, and lets a caller wait until the projection has caught up to
 * a position it already knows about, for example one returned by the command that a read is meant to see the effect
 * of.
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>)
 * <p>
 * The recorded value is one per projection, not one per view instance, and it means "this projection has applied
 * every event it was given, up to and including this position". That only holds when the projection is fed its
 * events in position order, which a subscription does. {@link #waitUntilApplied(String, long, Duration)} therefore
 * waits for "applied at least this position", never "applied exactly this position", since a position is not dense
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0084-what-a-position-guarantees.md">ADR 84</a>)
 * and a caller-held position may belong to an event this projection never handles, in which case the wait times out
 * rather than throwing.
 * <p>
 * A projection built by {@code Projections.recordingAppliedPosition(..)} in the blocking and reactor projection DSLs
 * advances the position it wraps automatically. Reading it back is a plain call to {@link #appliedPosition(String)}
 * or {@link #waitUntilApplied(String, long, Duration)}.
 */
@NullMarked
public interface AppliedPositionStorage {

    /**
     * The default interval {@link #waitUntilApplied(String, long, Duration)} polls at.
     */
    Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(25);

    /**
     * The position {@code projectionId} has applied, or empty when the projection has not advanced yet (it has not
     * started, or has not applied a single event with a position).
     */
    OptionalLong appliedPosition(String projectionId);

    /**
     * Advances the recorded position for {@code projectionId} to {@code position}, unless the recorded position is
     * already at or beyond it. A recorded position never moves backwards, which is what keeps a restarted projection
     * from appearing to run backwards while it replays history it has already applied once.
     *
     * @param position must be positive, following <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0084-what-a-position-guarantees.md">ADR 84</a>.
     */
    void advance(String projectionId, long position);

    /**
     * As {@link #waitUntilApplied(String, long, Duration, Duration)}, polling at {@link #DEFAULT_POLL_INTERVAL}.
     */
    default boolean waitUntilApplied(String projectionId, long position, Duration timeout) {
        return waitUntilApplied(projectionId, position, timeout, DEFAULT_POLL_INTERVAL);
    }

    /**
     * Blocks until {@code projectionId} has applied a position at or beyond {@code position}, or {@code timeout}
     * elapses. Returns {@code true} once caught up, {@code false} on timeout, and never throws for a timeout, the
     * same shape {@code Subscription.waitUntilStarted(Duration)} uses for a blocking wait elsewhere in this library.
     * <p>
     * This is a plain read-and-sleep loop, since the position lives in a store this method cannot subscribe to for a
     * push notification. An implementation backed by a store that can push a change is free to override this method.
     * <p>
     * A {@code position} that belongs to an event this projection never handles, because the event's type does not
     * match the projection's selector, is never reached, and the wait times out. That is the correct answer rather
     * than a defect, since a projection that never sees the event has no effect for the caller to read.
     *
     * @param position     the position to wait for, following <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0084-what-a-position-guarantees.md">ADR 84</a>.
     * @param timeout      how long to wait before giving up.
     * @param pollInterval how often to re-check the stored position.
     */
    default boolean waitUntilApplied(String projectionId, long position, Duration timeout, Duration pollInterval) {
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        requireNonNull(pollInterval, "pollInterval cannot be null");
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            OptionalLong applied = appliedPosition(projectionId);
            if (applied.isPresent() && applied.getAsLong() >= position) {
                return true;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            try {
                Thread.sleep(Math.min(pollInterval.toMillis(), Duration.ofNanos(remaining).toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * An {@code AppliedPositionStorage} backed by a plain map, for tests and single-process applications with no
     * store of their own to persist the position in. The recorded position does not survive a restart.
     */
    static AppliedPositionStorage inMemory() {
        return new AppliedPositionStorage() {
            private final Map<String, Long> positions = new ConcurrentHashMap<>();

            @Override
            public OptionalLong appliedPosition(String projectionId) {
                requireNonNull(projectionId, "projectionId cannot be null");
                Long position = positions.get(projectionId);
                return position == null ? OptionalLong.empty() : OptionalLong.of(position);
            }

            @Override
            public void advance(String projectionId, long position) {
                requireNonNull(projectionId, "projectionId cannot be null");
                if (position <= 0) {
                    throw new IllegalArgumentException("position must be positive but was " + position);
                }
                positions.merge(projectionId, position, Math::max);
            }
        };
    }
}
