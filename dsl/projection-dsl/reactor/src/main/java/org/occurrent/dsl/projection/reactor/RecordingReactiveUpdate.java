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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.dsl.projection.internal.AppliedAppendRecording;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * The reactive update {@link Projections#recordingAppliedAppends(BiFunction, String, AppliedAppendStore, ReplayPhase)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
 * Applies the wrapped update and then, once it completes, records the delivered event's append id, so the recorded
 * membership never claims state the delegate has not actually written yet.
 * <p>
 * {@link AppliedAppendStore} is a blocking-shaped interface, so both the readiness check and the record itself hop to
 * {@link Schedulers#boundedElastic()} together in one subscription, the same precedent
 * {@code RecordingReactiveUpdate} used for the withdrawn position-based design (commit {@code 0f3980c20^}). The
 * delegate this class wraps is caller-supplied and nothing guarantees it completes off a non-blocking thread, so a
 * delegate finishing on a Reactor Netty event loop would otherwise make the blocking store call throw.
 * <p>
 * Implements {@link ReactiveReplayAware} and forwards every lifecycle call to the delegate when it is one too. None
 * of the three lifecycle calls touch {@code store}: a replay records nothing by design (ADR 132 decision 6), so
 * there is nothing to write here, only bookkeeping to update, which needs no thread hop of its own.
 */
@NullMarked
public final class RecordingReactiveUpdate<E> implements BiFunction<EventMetadata, E, Mono<Void>>, ReactiveReplayAware, AppliedAppendRecorder {

    private final BiFunction<EventMetadata, E, Mono<Void>> delegate;
    private final AppliedAppendRecording recording;

    RecordingReactiveUpdate(BiFunction<EventMetadata, E, Mono<Void>> delegate, String projectionId, AppliedAppendStore store, ReplayPhase phase) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
        this.recording = new AppliedAppendRecording(projectionId, store, phase);
    }

    @Override
    public Mono<Void> apply(EventMetadata metadata, E event) {
        return delegate.apply(metadata, event).then(Mono.defer(() -> recordOnBoundedElastic(metadata)));
    }

    private Mono<Void> recordOnBoundedElastic(EventMetadata metadata) {
        return Mono.<Void>fromRunnable(() -> {
            if (recording.readyToRecord()) {
                recording.record(metadata);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void replayObserved() {
        recording.replayObserved();
    }

    @Override
    public void replayStarted() {
        if (delegate instanceof ReactiveReplayAware replayAware) {
            replayAware.replayStarted();
        }
        recording.replayStarted();
    }

    @Override
    public Mono<Void> replayCompleted() {
        Mono<Void> delegateCompletion = delegate instanceof ReactiveReplayAware replayAware ? replayAware.replayCompleted() : Mono.empty();
        // A plain field write, no store call, so this needs no boundedElastic hop of its own.
        return delegateCompletion.doOnSuccess(ignored -> recording.replayEnded());
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReactiveReplayAware replayAware) {
            replayAware.replayAbandoned();
        }
        recording.replayEnded();
    }
}
