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

public interface Decider<C, S, E> {
    S initialState();

    @NotNull
    List<E> decide(@NotNull C command, @Nullable S state);

    @NotNull S evolve(@Nullable S state, @NotNull E event);

    default boolean isTerminal(@Nullable S state) {
        return false;
    }

    default Result<S, E> decide(List<E> events, C command) {
        S currentState = fold(initialState(), events);
        List<E> newEvents = decide(command, currentState);
        S newState = fold(currentState, newEvents);
        return new Result<>(newState, newEvents);
    }

    default List<E> decideAndReturnEvents(List<E> events, C command) {
        S state = fold(initialState(), events);
        return decide(command, state);
    }

    default S decideAndReturnState(List<E> events, C command) {
        return decide(events, command).state;
    }

    private S fold(S state, List<E> events) {
        for (E event : events) {
            state = evolve(state, event);
            if (isTerminal(state)) {
                break;
            }
        }
        return state;
    }

    record Result<S, E>(S state, List<E> events) {
    }
}
