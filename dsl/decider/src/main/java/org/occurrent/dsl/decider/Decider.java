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

public interface Decider<C, S, E> {
    S initialState();

    @NotNull
    List<E> decide(@NotNull C command, @Nullable S state);

    @Nullable S evolve(@Nullable S state, @NotNull E event);

    default boolean isTerminal(@Nullable S state) {
        return false;
    }

    default Decision<S, E> decideOnEvents(List<E> events, C command) {
        @Nullable S currentState = fold(initialState(), events);
        List<E> newEvents = decide(command, currentState);
        S newState = fold(currentState, newEvents);
        return new Decision<>(newState, newEvents);
    }

    default List<E> decideOnEventAndReturnEvents(List<E> events, C command) {
        return decideOnEvents(events, command).events;
    }

    default @Nullable S decideOnEventsAndReturnState(List<E> events, C command) {
        return decideOnEvents(events, command).state;
    }

    default Decision<S, E> decideOnState(S state, C command) {
        List<E> newEvents = decide(command, state);
        @Nullable S newState = fold(state, newEvents);
        return new Decision<>(newState, newEvents);
    }

    default List<E> decideOnStateAndReturnEvents(S state, C command) {
        return decideOnState(state, command).events;
    }

    default @Nullable S decideOnStateAndReturnState(S state, C command) {
        return decideOnState(state, command).state;
    }

    @Nullable
    private S fold(@Nullable S state, List<E> events) {
        for (E event : events) {
            state = evolve(state, event);
            if (isTerminal(state)) {
                break;
            }
        }
        return state;
    }

    record Decision<S, E>(@Nullable S state, List<E> events) {
    }

    static <C, S, E> Decider<C, S, E> create(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve) {
        return create(initialState, decide, evolve, __ -> false);
    }

    static <C, S, E> Decider<C, S, E> create(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve,
                                             @NotNull Predicate<@Nullable S> isTerminal) {

        return new Decider<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @NotNull
            @Override
            public List<E> decide(@NotNull C command, @Nullable S state) {
                return decide.apply(command, state);
            }

            @NotNull
            @Override
            public S evolve(@Nullable S state, @NotNull E event) {
                return evolve.apply(state, event);
            }

            @Override
            public boolean isTerminal(@Nullable S state) {
                return isTerminal.test(state);
            }
        };
    }
}