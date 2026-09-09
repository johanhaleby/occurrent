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

import org.occurrent.cloudevents.EventMetadata;

/**
 * A capability a {@link MaterializedView} may implement to learn where a catch-up replay begins and ends, so it can
 * buffer replayed updates and write them coalesced instead of one store round trip per event
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * <p>
 * A view that does not implement this interface is never told anything and keeps writing through per event, exactly as
 * before this capability existed. Whoever drives a replay (a catch-up handover, for example) probes for it with an
 * {@code instanceof} check at the point of need, the same idiom {@code SagaInstances} uses for
 * {@code SagaStateStoreQueries}. There is deliberately no {@code static Optional<ReplayAware> findIn(...)}
 * helper. That shape exists elsewhere to unwrap a delegating view. The one wrapper this library builds around a
 * {@link MaterializedView}, the projection DSL's recording wrapper, forwards this capability to its delegate rather
 * than needing to be unwrapped itself, so there is still nothing here that needs a {@code findIn}.
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

    /**
     * A live copy arrived of an event the replay already delivered to this view, so the feed did not deliver it a
     * second time. The view has applied the event and is meant to apply it exactly once, so nothing here should apply
     * it again. What it is, is the only chance the view gets to do the work it does per delivery rather than per
     * application, which for a recording view is writing down the append the event came from
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
     * <p>
     * Sent only after {@link #replayCompleted()}, and only for an event the replay itself delivered. An event an
     * earlier live delivery already handled is not sent here, because that delivery did all of it. Sent again for
     * every further copy the feed is offered, so an implementation records rather than counts.
     * <p>
     * The default does nothing, which is what a view that only batches its replay wants.
     *
     * @param metadata What the feed knows about the event that was not delivered a second time. Empty when the live
     *                 copy arrived through {@code accept(E)}, which carries no metadata, and there is then no append
     *                 to write down.
     */
    default void alreadyDeliveredByReplay(EventMetadata metadata) {
    }
}
