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
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Internal helper shared by the blocking projection runners (the Kotlin {@code project} extensions and the Java
 * {@code *ProjectionRunner} facades): turning a {@link Projection} plus a {@link ViewStateRepository} into a
 * {@link MaterializedView} that skips events whose id resolves to {@code null}. The plain-{@code Filter} derivation
 * lives in {@code org.occurrent.dsl.projection.ProjectionFilters}, shared with the reactor stack.
 */
@NullMarked
public final class Projections {

    private Projections() {
    }

    /**
     * A {@link MaterializedView} that loads, evolves, and saves a projection's view state through {@code repository},
     * keyed by the projection's {@link Projection#id() id}. An event whose id resolves to {@code null} is skipped (no
     * load or save), so a projection can safely see events that map to no keyed instance. No retry is applied here; a
     * store that needs one (for example optimistic-locking retries against MongoDB) should supply its own
     * {@link MaterializedView}, such as the one the view DSL's {@code materialized(...)} builds.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        View<S, E> view = projection.view();
        Function<E, @Nullable ID> id = projection.id();
        return event -> {
            ID instanceId = id.apply(event);
            if (instanceId == null) {
                return;
            }
            S currentState = repository.findById(instanceId).orElse(view.initialState());
            repository.save(instanceId, view.evolve(currentState, event));
        };
    }
}
