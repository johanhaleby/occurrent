/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.view;

/**
 * A capability a {@link MaterializedView} may implement to learn where a catch-up replay begins and ends, so it can
 * buffer replayed updates and write them coalesced instead of one store round trip per event
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * <p>
 * A view that does not implement this interface is never told anything and keeps writing through per event, exactly as
 * before this capability existed. Whoever drives a replay (a catch-up handover, for example) probes for it with an
 * {@code instanceof} check at the point of need, the same idiom {@code SagaInstances} uses for
 * {@code SagaStateStoreQueries}. There is deliberately no {@code static Optional<ReplayAware> findIn(...)}
 * helper. That shape exists elsewhere to unwrap a delegating view. The one delegating {@link MaterializedView} this
 * library builds, the position recorder from
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>,
 * answers the {@code instanceof} probe itself and stays the outermost view, so it needs no unwrapping helper either.
 * <p>
 * {@link #replayCompleted()} runs before the replay's driver records the catch-up as complete, so an implementation
 * that buffers must have written every buffered update by the time this method returns. A write that fails here fails
 * the whole catch-up, exactly as a failed write inside the per-event fold does today.
 */
public interface ReplayAware {

    /** A catch-up replay is about to start delivering events to this view. */
    void replayStarted();

    /**
     * The replay finished delivering every event; anything buffered since {@link #replayStarted()} must be written
     * before this method returns, because the driver records the catch-up complete immediately afterwards.
     */
    void replayCompleted();

    /**
     * The replay was stopped before it finished. Anything buffered since {@link #replayStarted()} must be discarded
     * rather than written: the replay that would have produced a complete batch never finished, and the next catch-up
     * replays the whole history again, so a partial write here would only store state the next replay recomputes
     * anyway. Must not throw: a failure here must not mask whatever the replay was already unwinding from.
     */
    void replayAbandoned();
}
