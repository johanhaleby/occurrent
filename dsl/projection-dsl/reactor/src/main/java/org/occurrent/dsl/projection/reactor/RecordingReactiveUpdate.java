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
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.internal.AppliedAppendRecording;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * The reactive update {@link Projections#recordingAppliedAppends(BiFunction, String, AppliedAppendStore)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
 * Applies the wrapped update and then, once it completes, records the delivered event's append id, so the recorded
 * membership never claims state the delegate has not actually written yet. Nothing is recorded for a delegate that
 * reports skipping the event, {@link CoalescingMaterializedUpdate} when its id mapper resolves to no key, since such
 * an event never changed the read model this recording claims to describe.
 * <p>
 * {@link AppliedAppendStore} is a blocking-shaped interface, so both the readiness check and the record itself hop to
 * {@link Schedulers#boundedElastic()} together in one subscription, the same precedent
 * {@code RecordingReactiveUpdate} used for the withdrawn position-based design (commit {@code 0f3980c20^}). The
 * delegate this class wraps is caller-supplied and nothing guarantees it completes off a non-blocking thread, so a
 * delegate finishing on a Reactor Netty event loop would otherwise make the blocking store call throw.
 * <p>
 * Implements {@link ReactiveReplayAware} and forwards every lifecycle call to the delegate when it is one too.
 * {@link #replayStarted()} and {@link #replayAbandoned()} are plain bookkeeping, void signals the driving engine
 * never awaits, so neither touches {@code store} and neither needs a thread hop. {@link #replayCompleted()} does
 * touch {@code store}, retrying a clear the replay may have left owed, hopped to {@link Schedulers#boundedElastic()}
 * after the delegate's own completion, since it is the one lifecycle {@code Mono} this class's driving engine
 * actually awaits.
 */
@NullMarked
public final class RecordingReactiveUpdate<E> implements BiFunction<EventMetadata, E, Mono<Void>>, ReactiveReplayAware, AppliedAppendRecorder {

    private final BiFunction<EventMetadata, E, Mono<Void>> delegate;
    private final AppliedAppendRecording recording;
    // The episode minted for the replay a pull feed is currently driving, so its completion names the same one.
    private volatile @Nullable Object feedEpisode = null;

    RecordingReactiveUpdate(BiFunction<EventMetadata, E, Mono<Void>> delegate, String projectionId, AppliedAppendStore store) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
        this.recording = new AppliedAppendRecording(projectionId, store);
    }

    @Override
    public Mono<Void> apply(EventMetadata metadata, E event) {
        return applyDelegate(metadata, event).flatMap(wasApplied -> wasApplied ? recordOnBoundedElastic(metadata) : Mono.empty());
    }

    // The unchecked cast is safe: only a CoalescingMaterializedUpdate<?, E, ?> for this same E ever implements
    // SkippableUpdate<E>, since it is package-private and delegate's own generic parameter already fixes E.
    @SuppressWarnings("unchecked")
    private Mono<Boolean> applyDelegate(EventMetadata metadata, E event) {
        if (delegate instanceof SkippableUpdate<?> skippable) {
            return ((SkippableUpdate<E>) skippable).applyReportingWhetherApplied(metadata, event);
        }
        return delegate.apply(metadata, event).thenReturn(true);
    }

    private Mono<Void> recordOnBoundedElastic(EventMetadata metadata) {
        return Mono.<Void>fromRunnable(() -> recording.recordIfReady(metadata)).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void catchupStarted(Object episode) {
        recording.catchupStarted(episode);
    }

    @Override
    public void historyRead(Object episode) {
        recording.historyRead(episode);
    }

    @Override
    public void retryPendingClear() {
        recording.retryPendingClear();
    }

    @Override
    public boolean pollForClear() {
        return recording.pollForClear();
    }

    // The replay lifecycle a pull feed drives, mapped onto the two catch-up signals. The feed does not mint an
    // episode, so one is minted here, which is the same thing once per replay it starts. Both signals are I/O-free,
    // so neither needs a scheduler hop.
    @Override
    public void replayStarted() {
        if (delegate instanceof ReactiveReplayAware replayAware) {
            replayAware.replayStarted();
        }
        Object started = new Object();
        feedEpisode = started;
        recording.catchupStarted(started);
    }

    @Override
    public Mono<Void> replayCompleted() {
        Mono<Void> delegateCompletion = delegate instanceof ReactiveReplayAware replayAware ? replayAware.replayCompleted() : Mono.empty();
        return delegateCompletion.then(Mono.<Void>fromRunnable(() -> {
            Object started = feedEpisode;
            if (started != null) {
                recording.historyRead(started);
            }
            // A feed is not polled, so nothing else would retry a clear its replay left owed. This one call does
            // reach the store, which is why it keeps the hop the signals themselves no longer need.
            recording.retryPendingClear();
        }).subscribeOn(Schedulers.boundedElastic()));
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReactiveReplayAware replayAware) {
            replayAware.replayAbandoned();
        }
        // The history read is over even though it was cut short, and a pull feed goes on delivering live events to
        // this same fold afterwards, which are applied and are recorded. The clear the replay owed stays owed.
        // A subscription model sends nothing here instead, since a stopped catch-up delivers nothing more until a
        // new one announces itself.
        Object started = feedEpisode;
        if (started != null) {
            recording.historyRead(started);
        }
    }
}
