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
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAwareMaterializedView;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.BiFunction;

/**
 * Built by {@link Projections#reactiveUpdateWithMetadata(MaterializedView)} and its single-instance twin. Calls the
 * blocking {@code materializedView.update(metadata, event)} on {@link Schedulers#boundedElastic()}. Implements
 * {@link ReactiveReplayAwareMaterializedView} and forwards every lifecycle call to {@code materializedView} when it
 * implements the blocking {@link ReplayAwareMaterializedView} capability, so a batching view built with the blocking
 * view DSL (for example {@code org.occurrent.dsl.projection.blocking.Projections.materializedView(..)}) keeps
 * batching instead of silently falling back to a write-through per event.
 */
@NullMarked
final class BlockingMaterializedViewUpdate<E> implements BiFunction<EventMetadata, E, Mono<Void>>, ReactiveReplayAwareMaterializedView {

    private final MaterializedView<E> materializedView;

    BlockingMaterializedViewUpdate(MaterializedView<E> materializedView) {
        this.materializedView = materializedView;
    }

    @Override
    public Mono<Void> apply(EventMetadata metadata, E event) {
        return Mono.<Void>fromRunnable(() -> materializedView.update(metadata, event)).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void replayStarted() {
        if (materializedView instanceof ReplayAwareMaterializedView replayAware) {
            replayAware.replayStarted();
        }
    }

    @Override
    public Mono<Void> replayCompleted() {
        if (materializedView instanceof ReplayAwareMaterializedView replayAware) {
            return Mono.<Void>fromRunnable(replayAware::replayCompleted).subscribeOn(Schedulers.boundedElastic());
        }
        return Mono.empty();
    }

    @Override
    public void replayAbandoned() {
        if (materializedView instanceof ReplayAwareMaterializedView replayAware) {
            replayAware.replayAbandoned();
        }
    }
}
