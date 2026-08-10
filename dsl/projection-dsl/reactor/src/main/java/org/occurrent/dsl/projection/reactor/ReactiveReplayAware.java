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

package org.occurrent.dsl.projection.reactor;

import reactor.core.publisher.Mono;

/**
 * The reactive twin of the blocking view DSL's {@code ReplayAware}
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * It cannot live beside the blocking capability in {@code dsl/view-dsl}, because that module carries no reactor
 * dependency.
 * <p>
 * A view that does not implement this interface is never told anything and keeps writing through per event. Whoever
 * drives a replay probes for it with an {@code instanceof} check at the point of need.
 * <p>
 * Only {@link #replayCompleted()} returns a {@link Mono}: it is the one call the driving engine actually awaits,
 * chained before the catch-up marker is recorded, so an implementation that buffers can make that write asynchronous.
 * {@link #replayStarted()} and {@link #replayAbandoned()} are plain signals the engine calls inline on its own worker
 * thread and never waits on, the same shape the blocking twin uses for all three methods.
 * <p>
 * {@link #replayCompleted()} must complete before the replay's driver records the catch-up as complete, so an
 * implementation that buffers must have written every buffered update by the time the returned {@link Mono} completes.
 * A {@link Mono} that errors here fails the whole catch-up, exactly as a failed write inside the per-event fold does
 * today.
 */
public interface ReactiveReplayAware {

    /** A catch-up replay is about to start delivering events to this view. */
    void replayStarted();

    /**
     * The replay finished delivering every event; anything buffered since {@link #replayStarted()} must be written
     * before the returned {@link Mono} completes, because the driver records the catch-up complete immediately
     * afterwards.
     */
    Mono<Void> replayCompleted();

    /**
     * The replay was stopped before it finished. Anything buffered since {@link #replayStarted()} must be discarded
     * rather than written: the replay that would have produced a complete batch never finished, and the next catch-up
     * replays the whole history again, so a partial write here would only store state the next replay recomputes
     * anyway. Must not throw: a failure here must not mask whatever the replay was already unwinding from.
     */
    void replayAbandoned();
}
