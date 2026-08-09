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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedPositionStore;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.BiFunction;

/**
 * The reactive update {@link Projections#recordingAppliedPosition(BiFunction, AppliedPositionStore, String)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>).
 * Applies the wrapped update and then advances {@code store}, so the recorded position is written only after the
 * state it describes.
 * <p>
 * Implements {@link ReactiveReplayAwareMaterializedView} and forwards every lifecycle call to the delegate when it implements
 * the capability too. During a replay the delegate may be buffering (a coalescing update), so the position seen so
 * far is kept in memory and only written in {@link #replayCompleted()}, after the delegate's own {@link Mono}
 * completes. {@link #replayAbandoned()} discards it instead, since the next replay recomputes everything anyway.
 * {@code lock} guards the mutable state because the reactor catch-up handover can hop between worker threads even
 * though its calls never run concurrently.
 * <p>
 * {@link AppliedPositionStore} is a blocking-shaped interface, so both store advances below hop to
 * {@link Schedulers#boundedElastic()} first. The delegate this class wraps is caller-supplied
 * ({@code Projections.recordingAppliedPosition(..)} accepts any {@code BiFunction}), and nothing guarantees it
 * completes off a non-blocking thread the way the framework's own coalescing update does. A delegate that finishes
 * on a Reactor Netty event loop would otherwise make a blocking store's advance throw.
 */
@NullMarked
final class RecordingReactiveUpdate<E> implements BiFunction<EventMetadata, E, Mono<Void>>, ReactiveReplayAwareMaterializedView {

    private final BiFunction<EventMetadata, E, Mono<Void>> delegate;
    private final AppliedPositionStore store;
    private final String projectionId;

    private final Object lock = new Object();
    private boolean replaying = false;
    private long highestPositionSeenDuringReplay = 0;

    RecordingReactiveUpdate(BiFunction<EventMetadata, E, Mono<Void>> delegate, AppliedPositionStore store, String projectionId) {
        this.delegate = delegate;
        this.store = store;
        this.projectionId = projectionId;
    }

    @Override
    public Mono<Void> apply(EventMetadata metadata, E event) {
        @Nullable Long position = metadata.getPosition();
        if (position == null) {
            return Mono.error(new IllegalStateException(("Projection '%s' is configured to record its applied position, but received an event with no position. " +
                    "Either the event store has position writing turned off, or the event arrived on a path that carries no metadata " +
                    "(a live domain-event feed the application did not pass metadata into, or the metadata-less query/replay path).").formatted(projectionId)));
        }
        return delegate.apply(metadata, event).then(Mono.defer(() -> recordApplied(position)));
    }

    private Mono<Void> recordApplied(long position) {
        boolean stillReplaying;
        synchronized (lock) {
            stillReplaying = replaying;
            if (stillReplaying && position > highestPositionSeenDuringReplay) {
                highestPositionSeenDuringReplay = position;
            }
        }
        if (stillReplaying) {
            return Mono.empty();
        }
        return Mono.<Void>fromRunnable(() -> store.advance(projectionId, position)).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void replayStarted() {
        if (delegate instanceof ReactiveReplayAwareMaterializedView replayAware) {
            replayAware.replayStarted();
        }
        synchronized (lock) {
            replaying = true;
            highestPositionSeenDuringReplay = 0;
        }
    }

    @Override
    public Mono<Void> replayCompleted() {
        Mono<Void> delegateCompletion = delegate instanceof ReactiveReplayAwareMaterializedView replayAware ? replayAware.replayCompleted() : Mono.empty();
        return delegateCompletion.then(Mono.defer(() -> {
            long highest;
            synchronized (lock) {
                highest = highestPositionSeenDuringReplay;
                replaying = false;
            }
            if (highest == 0) {
                return Mono.empty();
            }
            return Mono.<Void>fromRunnable(() -> store.advance(projectionId, highest)).subscribeOn(Schedulers.boundedElastic());
        }));
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReactiveReplayAwareMaterializedView replayAware) {
            replayAware.replayAbandoned();
        }
        synchronized (lock) {
            highestPositionSeenDuringReplay = 0;
            replaying = false;
        }
    }
}
