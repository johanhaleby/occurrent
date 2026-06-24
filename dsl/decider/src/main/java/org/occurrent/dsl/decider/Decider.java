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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * A decider is a model that can be implemented to get a structured way to implement decision logic for a business entity (typically aggregate) or use case.
 *
 * @param <C> The type of commands that the decider can handle
 * @param <S> The state that the decider work
 * @param <E> The type of events that the decider returns
 */
public interface Decider<C, S, E> {
    S initialState();

    @NonNull
    List<E> decide(@NonNull C command, S state);

    S evolve(S state, @NonNull E event);

    /**
     * Whether {@code state} is terminal, meaning no further events should be folded into it. This is expected to be
     * absorbing: once it is {@code true} for a state it stays {@code true} for every state reachable from it by
     * {@link #evolve}. Both {@link #fold} and {@link #compose} rely on that, {@code fold} by breaking at the first
     * terminal state and {@code compose} by freezing a terminal slice, so a non-absorbing predicate gives undefined
     * results.
     */
    default boolean isTerminal(S state) {
        return false;
    }

    @NonNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, toList(command, additionalCommands));
    }

    @NonNull
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

    @NonNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnEventsAndReturnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).events;
    }

    @SuppressWarnings("unchecked")
    default @Nullable S decideOnEventsAndReturnState(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).state;
    }

    @NonNull
    default List<E> decideOnEventsAndReturnEvents(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).events;
    }

    default @Nullable S decideOnEventsAndReturnState(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).state;
    }

    @NonNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnState(S state, C command, C... additionalCommands) {
        return decideOnState(state, toList(command, additionalCommands));
    }

    @NonNull
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

    @NonNull
    default List<E> decideOnStateAndReturnEvents(S state, List<C> commands) {
        return decideOnState(state, commands).events;
    }

    default @Nullable S decideOnStateAndReturnState(S state, List<C> commands) {
        return decideOnState(state, commands).state;
    }

    @NonNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnStateAndReturnEvents(S state, C command, C... additionalCommands) {
        return decideOnState(state, command, additionalCommands).events;
    }

    default @Nullable S decideOnStateAndReturnState(S state, C command, C... additionalCommands) {
        return decideOnState(state, command, additionalCommands).state;
    }

    @NonNull
    private Decision<S, E> decideOnEventsWithSingleCommand(List<E> events, C command) {
        S currentState = fold(initialState(), events);
        List<E> newEvents = decide(command, currentState);
        S newState = fold(currentState, newEvents);
        return new Decision<>(newState, newEvents);
    }

    @NonNull
    private Decision<S, E> decideOnStateWithSingleCommand(S state, C command) {
        List<E> newEvents = decide(command, state);
        S newState = fold(state, newEvents);
        return new Decision<>(newState, newEvents);
    }

    private @Nullable S fold(S state, List<E> events) {
        for (E event : events) {
            state = evolve(state, event);
            if (isTerminal(state)) {
                break;
            }
        }
        return state;
    }

    @NonNull
    private static <C> List<C> toList(C command, C[] additionalCommands) {
        List<C> commands = new ArrayList<>();
        commands.add(command);
        if (additionalCommands != null && additionalCommands.length != 0) {
            Collections.addAll(commands, additionalCommands);
        }
        return commands;
    }

    record Decision<S, E>(S state, List<E> events) {
    }

    static <C, S, E> Decider<C, S, E> create(S initialState, @NonNull BiFunction<C, S, List<E>> decide, @NonNull BiFunction<S, E, S> evolve) {
        return create(initialState, decide, evolve, __ -> false);
    }

    static <C, S, E> Decider<C, S, E> create(S initialState, @NonNull BiFunction<C, S, List<E>> decide, @NonNull BiFunction<S, E, S> evolve,
                                             @NonNull Predicate<S> isTerminal) {

        return new Decider<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @NonNull
            @Override
            public List<E> decide(@NonNull C command, S state) {
                return decide.apply(command, state);
            }

            @NonNull
            @Override
            public S evolve(S state, @NonNull E event) {
                return evolve.apply(state, event);
            }

            @Override
            public boolean isTerminal(S state) {
                return isTerminal.test(state);
            }
        };
    }

    /**
     * Adapt a decider defined over a subtype of commands and events so it can run as a decider over the supertypes.
     * Commands that are not instances of {@code commandType} produce no events, and events that are not instances of
     * {@code eventType} leave the state unchanged. This is what lets a feature decider, for example
     * {@code Decider<CourseCommand, CourseState, CourseEvent>}, run against a service over a common {@code DomainEvent},
     * and it is the building block that {@link #compose} relies on.
     *
     * @param decider     the decider to adapt
     * @param commandType the concrete command type the adapted decider understands
     * @param eventType   the concrete event type the adapted decider understands
     * @param <C>         the wider command type of the adapted decider
     * @param <S>         the state type, unchanged by adapting
     * @param <E>         the wider event type of the adapted decider
     * @param <SubC>      the command type the wrapped decider understands
     * @param <SubE>      the event type the wrapped decider understands
     */
    static <C, S, E, SubC extends C, SubE extends E> Decider<C, S, E> adapt(@NonNull Decider<SubC, S, SubE> decider,
                                                                            @NonNull Class<SubC> commandType,
                                                                            @NonNull Class<SubE> eventType) {
        return create(
                decider.initialState(),
                (command, state) -> commandType.isInstance(command)
                        ? new ArrayList<E>(decider.decide(commandType.cast(command), state))
                        : List.of(),
                (state, event) -> eventType.isInstance(event)
                        ? decider.evolve(state, eventType.cast(event))
                        : state,
                decider::isTerminal);
    }

    /**
     * Compose several deciders that share the same command and event types (typically reached with {@link #adapt}) into a
     * single decider whose state is the product of the individual states, held positionally in a {@link CompositeState}.
     * Each command is offered to every sub-decider, but only the one that recognizes it produces events. Each event
     * evolves every sub-decider's own state slice independently, skipping a slice that is already terminal so each slice
     * settles at the same state it would reach folding its own events alone (matching {@link #fold} and assuming
     * {@link #isTerminal} is absorbing). The composed decider is terminal once every sub-decider is.
     */
    @SafeVarargs
    static <C, E> Decider<C, CompositeState, E> compose(@NonNull Decider<C, ?, E>... deciders) {
        return compose(Arrays.asList(deciders));
    }

    /**
     * List-taking variant of {@link #compose(Decider[])}, for composing a number of deciders not known at compile time.
     */
    static <C, E> Decider<C, CompositeState, E> compose(@NonNull List<? extends Decider<C, ?, E>> deciders) {
        @SuppressWarnings("unchecked")
        List<Decider<C, Object, E>> slices = (List<Decider<C, Object, E>>) (List<?>) new ArrayList<>(deciders);

        List<Object> initialStates = new ArrayList<>();
        for (Decider<C, Object, E> decider : slices) {
            initialStates.add(decider.initialState());
        }

        return create(
                new CompositeState(initialStates),
                (command, composite) -> {
                    List<E> events = new ArrayList<>();
                    for (int i = 0; i < slices.size(); i++) {
                        events.addAll(slices.get(i).decide(command, composite.slice(i)));
                    }
                    return events;
                },
                (composite, event) -> {
                    List<Object> next = new ArrayList<>(composite.states());
                    for (int i = 0; i < slices.size(); i++) {
                        Decider<C, Object, E> decider = slices.get(i);
                        Object slice = next.get(i);
                        if (!decider.isTerminal(slice)) {
                            next.set(i, decider.evolve(slice, event));
                        }
                    }
                    return new CompositeState(next);
                },
                composite -> {
                    for (int i = 0; i < slices.size(); i++) {
                        if (!slices.get(i).isTerminal(composite.slice(i))) {
                            return false;
                        }
                    }
                    return true;
                });
    }
}