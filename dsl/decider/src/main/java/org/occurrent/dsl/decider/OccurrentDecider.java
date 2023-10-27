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

package org.occurrent.dsl.decider;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class OccurrentDecider<C, S, E> implements Decider<C, S, E> {
    @Nullable
    final S initialState;
    final BiFunction<C, S, List<E>> decide;
    final BiFunction<S, E, S> evolve;
    final Predicate<S> isTerminal;

    public OccurrentDecider(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve) {
        this(initialState, decide, evolve, __ -> false);
    }

    public OccurrentDecider(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve, @NotNull Predicate<@Nullable S> isTerminal) {
        this.initialState = initialState;
        this.decide = decide;
        this.evolve = evolve;
        this.isTerminal = isTerminal;
    }

    @Override
    public S initialState() {
        return initialState;
    }

    @NotNull
    public List<E> decide(@NotNull C command, @Nullable S state) {
        return decide.apply(command, state);
    }

    @NotNull
    public S evolve(@Nullable S state, @NotNull E event) {
        return evolve.apply(state, event);
    }

    public boolean isTerminal(@Nullable S state) {
        return isTerminal.test(state);
    }
}