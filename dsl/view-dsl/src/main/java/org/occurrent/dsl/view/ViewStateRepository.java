/*
 *
 *  Copyright 2023 Johan Haleby
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

import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An interface that finds and saves the view state. If you're using Kotlin, see the <code>org.occurrent.dsl.view.fetch</code>
 * function in the <code>ViewStateRepositoryExtensions.kt</code> file to avoid the <code>Optional</code> returned by <code>findById</code>.
 *
 * @param <S>  The state to store
 * @param <ID> The id that uniquely identifies the state
 */
public interface ViewStateRepository<S, ID> {
    Optional<@NotNull S> findById(@NotNull ID id);

    void save(@NotNull ID id, @NotNull S state);

    default S findByIdOrElse(@NotNull ID id, View<S, ?> view) {
        return findByIdOrElse(id, view.initialState());
    }

    default S findByIdOrElse(@NotNull ID id, S initialState) {
        return findById(id).orElse(initialState);
    }

    static <S, ID> ViewStateRepository<S, ID> create(Function<@NotNull ID, S> findById, BiConsumer<@NotNull ID, @NotNull S> save) {
        return new ViewStateRepository<>() {
            @Override
            public Optional<S> findById(@NotNull ID id) {
                return Optional.ofNullable(findById.apply(id));
            }

            @Override
            public void save(@NotNull ID id, @NotNull S state) {
                save.accept(id, state);
            }
        };
    }
}