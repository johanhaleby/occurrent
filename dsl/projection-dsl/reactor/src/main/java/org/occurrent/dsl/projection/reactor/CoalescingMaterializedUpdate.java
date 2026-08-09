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
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The reactive update {@link Projections#reactiveUpdateWithMetadata(Projection, ViewStateRepository)} and its
 * single-instance twin build
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * Bridges the blocking {@code repository} onto {@link Schedulers#boundedElastic()}, the same as before this class
 * existed. Outside a replay it writes through per event. Once {@link #replayStarted()} has run,
 * {@link #apply(EventMetadata, Object)} buffers instead: events are grouped by the id {@code resolveId} resolves them
 * to, in arrival order, until {@code batchSize} events have buffered across every key, at which point the batch
 * flushes before buffering resumes. {@link #replayCompleted()} flushes whatever is left; {@link #replayAbandoned()}
 * discards it instead.
 * <p>
 * The reactor catch-up handover folds one replayed payload at a time through a serialized {@code concatMap}, and the
 * lifecycle calls are woven into that same serialized pipeline, itself scheduled once onto
 * {@link Schedulers#boundedElastic()}. {@link #apply(EventMetadata, Object)} and {@link #replayCompleted()} hop there
 * explicitly because they do real work (a repository round trip); {@link #replayStarted()} and
 * {@link #replayAbandoned()} do not, since the engine only ever calls them from inside that same pipeline and never
 * awaits them (see {@link ReactiveReplayAwareMaterializedView}). {@code lock} still guards the mutable state throughout,
 * because a plain field write on one worker thread is not guaranteed visible to whichever worker thread runs the next
 * call, even though the calls themselves never run concurrently.
 */
@NullMarked
final class CoalescingMaterializedUpdate<S extends @Nullable Object, E, ID> implements BiFunction<EventMetadata, E, Mono<Void>>, ReactiveReplayAwareMaterializedView {

    private final View<S, E> view;
    private final ViewStateRepository<S, ID> repository;
    private final int batchSize;
    private final BiFunction<EventMetadata, E, @Nullable ID> resolveId;

    private final Object lock = new Object();
    private boolean replaying = false;
    private Map<ID, List<Buffered<E>>> buffered = new LinkedHashMap<>();
    private int bufferedCount = 0;

    CoalescingMaterializedUpdate(View<S, E> view, ViewStateRepository<S, ID> repository, int batchSize,
                                  BiFunction<EventMetadata, E, @Nullable ID> resolveId) {
        this.view = view;
        this.repository = repository;
        this.batchSize = batchSize;
        this.resolveId = resolveId;
    }

    @Override
    public Mono<Void> apply(EventMetadata metadata, E event) {
        return Mono.<Void>fromRunnable(() -> foldOnCallingThread(metadata, event)).subscribeOn(Schedulers.boundedElastic());
    }

    private void foldOnCallingThread(EventMetadata metadata, E event) {
        @Nullable ID id = resolveId.apply(metadata, event);
        if (id == null) {
            return;
        }
        Map<ID, List<Buffered<E>>> toFlush = null;
        boolean writeThrough = false;
        synchronized (lock) {
            if (replaying) {
                buffered.computeIfAbsent(id, ignored -> new ArrayList<>()).add(new Buffered<>(metadata, event));
                bufferedCount++;
                if (bufferedCount >= batchSize) {
                    toFlush = drainLocked();
                }
            } else {
                writeThrough = true;
            }
        }
        if (toFlush != null) {
            writeBatch(toFlush);
        } else if (writeThrough) {
            S currentState = repository.findById(id).orElse(view.initialState());
            repository.save(id, view.evolve(currentState, metadata, event));
        }
    }

    // replayStarted() and replayAbandoned() are plain signals ReactiveHandover calls inline, never Mono-wrapped or
    // awaited, so they need no scheduler hop of their own: the engine only ever calls them from inside its own
    // boundedElastic-scheduled pipeline (ReactiveHandover.catchUp()'s single subscribeOn covers the whole chain),
    // which is also this class's own invariant for every other call (see the class javadoc).
    @Override
    public void replayStarted() {
        synchronized (lock) {
            replaying = true;
        }
    }

    @Override
    public Mono<Void> replayCompleted() {
        return Mono.<Void>fromRunnable(() -> {
            Map<ID, List<Buffered<E>>> toFlush;
            synchronized (lock) {
                toFlush = drainLocked();
                replaying = false;
            }
            writeBatch(toFlush);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void replayAbandoned() {
        synchronized (lock) {
            buffered.clear();
            bufferedCount = 0;
            replaying = false;
        }
    }

    // Must be called holding lock.
    private Map<ID, List<Buffered<E>>> drainLocked() {
        Map<ID, List<Buffered<E>>> batch = buffered;
        buffered = new LinkedHashMap<>();
        bufferedCount = 0;
        return batch;
    }

    // Runs on the calling (boundedElastic) thread. Reads each buffered key's state once, folds that key's buffered
    // events onto it in arrival order, and writes the result once.
    private void writeBatch(Map<ID, List<Buffered<E>>> batch) {
        if (batch.isEmpty()) {
            return;
        }
        Map<ID, S> currentStates = repository.findAllById(batch.keySet());
        Map<ID, S> updatedStates = new LinkedHashMap<>();
        for (Map.Entry<ID, List<Buffered<E>>> entry : batch.entrySet()) {
            ID id = entry.getKey();
            S state = currentStates.getOrDefault(id, view.initialState());
            for (Buffered<E> buffered : entry.getValue()) {
                state = view.evolve(state, buffered.metadata(), buffered.event());
            }
            updatedStates.put(id, state);
        }
        repository.saveAll(updatedStates);
    }

    private record Buffered<E>(EventMetadata metadata, E event) {
    }
}
