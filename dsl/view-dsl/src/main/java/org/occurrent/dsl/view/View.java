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

import java.util.function.BiFunction;

/**
 * A structure for representing and updating views based on state and an event
 *
 * @param <S> The type of the state that this view produces
 * @param <E> The type of the event that is used to create the state
 */
public interface View<S, E> {
    S initialState();

    S evolve(S state, @NotNull E event);

    static <S, E> View<S, E> create(S initialState, @NotNull BiFunction<S, E, S> evolve) {
        return new View<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @Override
            public S evolve(S state, @NotNull E event) {
                return evolve.apply(state, event);
            }
        };
    }
}