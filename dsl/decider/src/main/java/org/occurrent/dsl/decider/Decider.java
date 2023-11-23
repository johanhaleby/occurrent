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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * A decider is a model that can be implemented to get a structured way to implement decision logic for a business capability.
 *
 * @param <C> The type of commands that the decider can handle
 * @param <S> The state that the decider work
 * @param <E> The type of events that the decider returns
 */
public interface Decider<C, S, E> {
    S initialState();

    @NotNull
    List<E> decide(@NotNull C command, @Nullable S state);

    @Nullable S evolve(@Nullable S state, @NotNull E event);

    default boolean isTerminal(@Nullable S state) {
        return false;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, toList(command, additionalCommands));
    }

    @NotNull
    default Decision<S, E> decideOnEvents(List<E> events, List<C> commands) {
        Decision<S, E> decision = new Decision<>(initialState(), events);
        for (C command : commands) {
            Decision<S, E> thisDecision = decideOnEventsWithSingleCommand(decision.events, command);
            List<E> accumulatedEvents = new ArrayList<>(decision.events);
            accumulatedEvents.addAll(thisDecision.events);
            decision = new Decision<>(thisDecision.state, accumulatedEvents);
        }
        // The decision now has all events, including the ones we passed in. But we're only interested
        // in the new ones, thus we remove the events that we passed in.
        List<E> newEvents = decision.events.subList(events.size(), decision.events.size());
        return new Decision<>(decision.state, newEvents);
    }

    @NotNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnEventsAndReturnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).events;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    default S decideOnEventsAndReturnState(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).state;
    }

    @NotNull
    default List<E> decideOnEventsAndReturnEvents(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).events;
    }

    @Nullable
    default S decideOnEventsAndReturnState(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).state;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnState(S state, C command, C... additionalCommands) {
        return decideOnState(state, toList(command, additionalCommands));
    }

    @NotNull
    default Decision<S, E> decideOnState(S state, List<C> commands) {
        Decision<S, E> decision = new Decision<>(state, List.of());
        for (C command : commands) {
            Decision<S, E> thisDecision = decideOnStateWithSingleCommand(decision.state, command);
            List<E> accumulatedEvents = new ArrayList<>(decision.events);
            accumulatedEvents.addAll(thisDecision.events);
            decision = new Decision<>(thisDecision.state, accumulatedEvents);
        }
        return decision;
    }

    @NotNull
    default List<E> decideOnStateAndReturnEvents(S state, List<C> commands) {
        return decideOnState(state, commands).events;
    }

    @Nullable
    default S decideOnStateAndReturnState(S state, List<C> commands) {
        return decideOnState(state, commands).state;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnStateAndReturnEvents(S state, C command, C... additionalCommands) {
        return decideOnState(state, command, additionalCommands).events;
    }

    @Nullable
    default S decideOnStateAndReturnState(S state, C command, C... additionalCommands) {
        return decideOnState(state, command, additionalCommands).state;
    }

    @NotNull
    private Decision<S, E> decideOnEventsWithSingleCommand(List<E> events, C command) {
        @Nullable S currentState = fold(initialState(), events);
        List<E> newEvents = decide(command, currentState);
        S newState = fold(currentState, newEvents);
        return new Decision<>(newState, newEvents);
    }

    @NotNull
    default Decision<S, E> decideOnStateWithSingleCommand(S state, C command) {
        List<E> newEvents = decide(command, state);
        @Nullable S newState = fold(state, newEvents);
        return new Decision<>(newState, newEvents);
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

    @NotNull
    private static <C> List<C> toList(C command, C[] additionalCommands) {
        List<C> commands = new ArrayList<>();
        commands.add(command);
        if (additionalCommands != null && additionalCommands.length != 0) {
            Collections.addAll(commands, additionalCommands);
        }
        return commands;
    }

    record Decision<S, E>(@Nullable S state, List<E> events) {
    }

    static <C, S, E> Decider<C, S, E> create(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve) {
        return create(initialState, decide, evolve, __ -> false);
    }

    static <C, S, E> Decider<C, S, E> create(@Nullable S initialState, @NotNull BiFunction<C, S, List<E>> decide, @NotNull BiFunction<S, E, S> evolve,
                                             @NotNull Predicate<S> isTerminal) {

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