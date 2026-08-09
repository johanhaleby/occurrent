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

package org.occurrent.dsl.projection.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAwareMaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.retry.RetryStrategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The coalescing view {@link Projections#materializedView} builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
 * Outside a replay it writes through per event, exactly as before this class existed. Once
 * {@link #replayStarted()} has been called, {@link #update(EventMetadata, Object)} buffers instead: events are grouped
 * by the id {@code resolveId} resolves them to, in arrival order, until {@code batchSize} events have buffered across
 * every key, at which point the batch flushes on the calling thread before buffering resumes. {@link #replayCompleted()}
 * flushes whatever is left; {@link #replayAbandoned()} discards it instead.
 * <p>
 * A flush reads each buffered key's current state once, folds that key's buffered events onto it in arrival order, and
 * writes the result once: {@code K} reads and {@code K} writes for a batch touching {@code K} keys, against one read
 * and one write per event outside a replay. With no {@code retryStrategy} the flush reads through
 * {@link ViewStateRepository#findAllById(java.util.Collection)} and writes through
 * {@link ViewStateRepository#saveAll(Map)} in one call each, so a repository that overrides either for a real bulk
 * round trip is used to full effect. A configured {@code retryStrategy} instead flushes one key at a time, re-reading
 * through {@link ViewStateRepository#findById(Object)} and writing through {@link ViewStateRepository#save(Object, Object)}
 * on every attempt, the same per-key optimistic-locking recovery {@link MaterializedView#updateFromRepository} gives the
 * live path: a lost update is only recoverable when the store reports the conflict on an individual write, which an
 * overridden {@code saveAll} does not, so a configured retry strategy trades the bulk round trip for keeping that
 * recovery.
 */
@NullMarked
final class CoalescingMaterializedView<S extends @Nullable Object, E, ID> implements MaterializedView<E>, ReplayAwareMaterializedView {

    private final View<S, E> view;
    private final ViewStateRepository<S, ID> repository;
    private final RetryStrategy retryStrategy;
    private final int batchSize;
    private final BiFunction<EventMetadata, E, @Nullable ID> resolveId;

    // Every access happens on the thread driving the catch-up replay: BlockingHandover.catchUp folds one payload at a
    // time on the calling thread, so this class does no synchronization of its own.
    private boolean replaying = false;
    private Map<ID, List<Buffered<E>>> buffered = new LinkedHashMap<>();
    private int bufferedCount = 0;

    CoalescingMaterializedView(View<S, E> view, ViewStateRepository<S, ID> repository, RetryStrategy retryStrategy,
                                int batchSize, BiFunction<EventMetadata, E, @Nullable ID> resolveId) {
        this.view = view;
        this.repository = repository;
        this.retryStrategy = retryStrategy;
        this.batchSize = batchSize;
        this.resolveId = resolveId;
    }

    @Override
    public void update(E event) {
        update(EventMetadata.empty(), event);
    }

    @Override
    public void update(EventMetadata metadata, E event) {
        @Nullable ID id = resolveId.apply(metadata, event);
        if (id == null) {
            return;
        }
        if (replaying) {
            buffered.computeIfAbsent(id, ignored -> new ArrayList<>()).add(new Buffered<>(metadata, event));
            bufferedCount++;
            if (bufferedCount >= batchSize) {
                flush();
            }
        } else {
            updateFromRepository(id, metadata, event, view, repository, retryStrategy);
        }
    }

    @Override
    public void replayStarted() {
        replaying = true;
    }

    @Override
    public void replayCompleted() {
        flush();
        replaying = false;
    }

    @Override
    public void replayAbandoned() {
        buffered.clear();
        bufferedCount = 0;
        replaying = false;
    }

    private void flush() {
        if (buffered.isEmpty()) {
            return;
        }
        Map<ID, List<Buffered<E>>> batch = buffered;
        buffered = new LinkedHashMap<>();
        bufferedCount = 0;

        if (retryStrategy instanceof RetryStrategy.DontRetry) {
            Map<ID, S> currentStates = repository.findAllById(batch.keySet());
            Map<ID, S> updatedStates = new LinkedHashMap<>();
            for (Map.Entry<ID, List<Buffered<E>>> entry : batch.entrySet()) {
                ID id = entry.getKey();
                updatedStates.put(id, fold(currentStates.getOrDefault(id, view.initialState()), entry.getValue()));
            }
            repository.saveAll(updatedStates);
        } else {
            for (Map.Entry<ID, List<Buffered<E>>> entry : batch.entrySet()) {
                ID id = entry.getKey();
                List<Buffered<E>> events = entry.getValue();
                retryStrategy.execute(() -> {
                    S state = fold(repository.findById(id).orElse(view.initialState()), events);
                    repository.save(id, state);
                });
            }
        }
    }

    private S fold(S state, List<Buffered<E>> events) {
        for (Buffered<E> event : events) {
            state = view.evolve(state, event.metadata(), event.event());
        }
        return state;
    }

    private record Buffered<E>(EventMetadata metadata, E event) {
    }
}
