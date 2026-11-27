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
 * The callbacks run only for a replay from the beginning, which a catch-up does when no catch-up marker is stored. A
 * blocking catch-up that finds a stored marker skips its replay. A reactor catch-up that reads one resumes after it
 * and delivers those events the way live events are delivered, with none of these callbacks, so a view that rebuilds
 * itself between {@link #replayStarted()} and {@link #replayCompleted()} sees a rebuild only when it gets one.
 * <p>
 * {@link #replayCompleted()} runs before the replay's driver stores its catch-up marker, so an implementation that
 * buffers must have written every buffered update by the time this method returns. A write that fails here fails
 * the whole catch-up, exactly as a failed write inside the per-event fold does today.
 */
public interface ReplayAware {

    /** A catch-up replay is about to start delivering events to this view. */
    void replayStarted();

    /**
     * The replay finished delivering every event; anything buffered since {@link #replayStarted()} must be written
     * before this method returns, because the driver can store the catch-up marker afterwards.
     */
    void replayCompleted();

    /**
     * The replay was stopped before it finished. Anything buffered since {@link #replayStarted()} must be discarded
     * rather than written, since the replay that would have produced a complete batch never finished and stored no
     * catch-up marker. Must not throw, since a failure here must not mask whatever the replay was already
     * unwinding from.
     */
    void replayAbandoned();

    /**
     * A live copy arrived of an event the replay already delivered to this view, so the feed did not deliver it a
     * second time, and nothing here should apply it again. This is the only chance a recording view gets to write
     * down the append the event came from
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
     * The replay delivering the event does not mean the view applied it, since a projection can skip an event, so a
     * recording view writes the append down only when its replay applied an event of it.
     * <p>
     * Sent only after {@link #replayCompleted()}, and only for an event the replay itself delivered. An event an
     * earlier live delivery already handled is not sent here, because that delivery did all of it. It can be sent
     * again for a further copy the feed is offered, so an implementation records rather than counts.
     * <p>
     * A throw for an event the feed held during its catch-up refuses that event alone. Its {@code acceptAndWait(..)}
     * throws, so the listener has the broker deliver it again, and that delivery calls this again while the replay's
     * de-dup cache, sized by {@code dedupCacheSize}, still holds the event. The catch-up goes
     * on, and the feed stays usable. A throw for an event fed after the feed went live reaches the call that fed it.
     * <p>
     * The default does nothing, which is what a view that only batches its replay wants.
     *
     * @param metadata What the feed knows about the event that was not delivered a second time. Empty when the live
     *                 copy arrived through {@code acceptAndWait(E)} or {@code acceptOrDefer(E)}, which take no
     *                 metadata, and there is then no append to write down.
     */
    default void alreadyDeliveredByReplay(EventMetadata metadata) {
    }
}
