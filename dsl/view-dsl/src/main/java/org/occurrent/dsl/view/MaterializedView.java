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

import java.util.function.Function;

/**
 * A materialized view combines a {@link View} and a {@link ViewStateRepository} to make updates from events in a convenient manner.
 */
public interface MaterializedView<E> {
    void update(E event);

    default <S, ID> void updateFromRepository(ID id, E event, View<S, E> view, ViewStateRepository<S, ID> repository) {
        S currentState = repository.findById(id).orElse(view.initialState());
        S updatedState = view.evolve(currentState, event);
        repository.save(id, updatedState);
    }

    static <S, E, ID> MaterializedView<E> create(Function<E, ID> idMapper, View<S, E> view, ViewStateRepository<S, ID> repository) {
        return new MaterializedView<>() {
            @Override
            public void update(E event) {
                ID id = idMapper.apply(event);
                updateFromRepository(id, event, view, repository);
            }
        };
    }
}