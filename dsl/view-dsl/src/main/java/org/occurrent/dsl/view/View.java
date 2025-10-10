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
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A structure for representing and updating views based on state and an event
 *
 * @param <S> The type of the state that this view produces
 * @param <E> The type of the event that is used to create the state
 */
public interface View<S, E> {
    /**
     * @return The initial state
     */
    S initialState();

    /**
     * Evolve state by applying the event
     *
     * @param state The current state
     * @param event The event
     * @return The evolved state
     */
    S evolve(@Nullable S state, @NotNull E event);

    /**
     * Evolve initial state from events
     *
     * @return The evolved state
     */
    @SuppressWarnings("unchecked")
    default S evolve(@NotNull E event, @NotNull E event2, @NotNull E... moreEvents) {
        return evolve(initialState(), event, event2, moreEvents);
    }

    /**
     * Evolve from events
     *
     * @return The state
     */
    @SuppressWarnings("unchecked")
    default S evolve(S state, @NotNull E event, @NotNull E event2, @NotNull E... moreEvents) {
        return evolve(state, Stream.concat(Stream.of(event, event2), Arrays.stream(moreEvents)));
    }

    default S evolve(S state, @NotNull List<E> events) {
        return evolve(state, events.stream());
    }

    /**
     * Evolve initial state from events
     *
     * @return The evolved state
     */
    default S evolve(@NotNull List<E> events) {
        return evolve(initialState(), events.stream());
    }

    default S evolve(S state, @NotNull Stream<E> events) {
        return events.sequential().reduce(state, this::evolve, (left, right) -> right);
    }

    /**
     * Evolve initial state from events
     *
     * @return The evolved state
     */
    default S evolve(@NotNull Stream<E> events) {
        return evolve(initialState(), events);
    }

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