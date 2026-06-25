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
    /**
     * The state a decider starts from, before any events have been applied.
     */
    S initialState();

    /**
     * Decide what should happen for {@code command} given the current {@code state}. Returns the events to append, or an
     * empty list if the command does nothing or is rejected. This is where the business rules live and should be a pure
     * function (no side effects).
     */
    @NonNull
    List<E> decide(@NonNull C command, S state);

    /**
     * Apply {@code event} to {@code state} and return the new state. This is how past events are folded back into the
     * current state before a decision is made. Should be a pure function.
     */
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

    /**
     * Fold {@code events} to rebuild the current state, then run the command(s) against it. Returns the resulting state
     * and the new events that were produced. Use this when you have the entity's past events at hand.
     */
    @NonNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, toList(command, additionalCommands));
    }

    /**
     * Like {@link #decideOnEvents(List, Object, Object[])} but takes the commands as a list.
     */
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

    /**
     * Convenience for {@link #decideOnEvents} that returns only the new events.
     */
    @NonNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnEventsAndReturnEvents(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).events;
    }

    /**
     * Convenience for {@link #decideOnEvents} that returns only the resulting state.
     */
    @SuppressWarnings("unchecked")
    default @Nullable S decideOnEventsAndReturnState(List<E> events, C command, C... additionalCommands) {
        return decideOnEvents(events, command, additionalCommands).state;
    }

    /**
     * Convenience for {@link #decideOnEvents} that returns only the new events.
     */
    @NonNull
    default List<E> decideOnEventsAndReturnEvents(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).events;
    }

    /**
     * Convenience for {@link #decideOnEvents} that returns only the resulting state.
     */
    default @Nullable S decideOnEventsAndReturnState(List<E> events, List<C> commands) {
        return decideOnEvents(events, commands).state;
    }

    /**
     * Run the command(s) against an already known {@code state}, without folding any events first. Returns the new state
     * and the events produced. Use this when you already hold the current state, for example from a snapshot or a read
     * model.
     */
    @NonNull
    @SuppressWarnings("unchecked")
    default Decision<S, E> decideOnState(S state, C command, C... additionalCommands) {
        return decideOnState(state, toList(command, additionalCommands));
    }

    /**
     * Like {@link #decideOnState(Object, Object, Object[])} but takes the commands as a list.
     */
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

    /**
     * Convenience for {@link #decideOnState} that returns only the new events.
     */
    @NonNull
    default List<E> decideOnStateAndReturnEvents(S state, List<C> commands) {
        return decideOnState(state, commands).events;
    }

    /**
     * Convenience for {@link #decideOnState} that returns only the resulting state.
     */
    default @Nullable S decideOnStateAndReturnState(S state, List<C> commands) {
        return decideOnState(state, commands).state;
    }

    /**
     * Convenience for {@link #decideOnState} that returns only the new events.
     */
    @NonNull
    @SuppressWarnings("unchecked")
    default List<E> decideOnStateAndReturnEvents(S state, C command, C... additionalCommands) {
        return decideOnState(state, command, additionalCommands).events;
    }

    /**
     * Convenience for {@link #decideOnState} that returns only the resulting state.
     */
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

    /**
     * The outcome of a decision: the resulting {@code state} and the new {@code events} that were produced.
     */
    record Decision<S, E>(S state, List<E> events) {
    }

    /**
     * Create a decider from functions instead of implementing the interface. This overload is never terminal. See
     * {@link #create(Object, BiFunction, BiFunction, Predicate)} to also supply an {@code isTerminal} predicate.
     */
    static <C, S, E> Decider<C, S, E> create(S initialState, @NonNull BiFunction<C, S, List<E>> decide, @NonNull BiFunction<S, E, S> evolve) {
        return create(initialState, decide, evolve, __ -> false);
    }

    /**
     * Create a decider from functions instead of implementing the interface, including an {@code isTerminal} predicate.
     */
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
     * Widen a decider so it can be used where a decider over broader command and event types is expected, for example a
     * decider for one feature running against an application service that handles a whole domain.
     * <p>
     * A decider for one feature only knows that feature's commands and events. {@code adapt} wraps it so it accepts the
     * broader types and quietly ignores anything that is not its own: a command that is not a {@code commandType} produces
     * no events, and an event that is not an {@code eventType} leaves the state unchanged. It is also the building block
     * for {@link #compose}.
     *
     * <pre>
     * // courseDecider is a Decider&lt;CourseCommand, CourseState, CourseEvent&gt;,
     * // but the application service works with the whole domain's commands and events.
     * Decider&lt;DomainCommand, CourseState, DomainEvent&gt; widened =
     *         Decider.adapt(courseDecider, CourseCommand.class, CourseEvent.class);
     * </pre>
     *
     * @param decider     the feature decider to widen
     * @param commandType the command type the decider understands
     * @param eventType   the event type the decider understands
     */
    static <C, S, E, SubC extends C, SubE extends E> Decider<C, S, E> adapt(@NonNull Decider<SubC, S, SubE> decider,
                                                                            @NonNull Class<SubC> commandType,
                                                                            @NonNull Class<SubE> eventType) {
        return create(
                decider.initialState(),
                (command, state) -> commandType.isInstance(command)
                        ? new ArrayList<>(decider.decide(commandType.cast(command), state))
                        : List.of(),
                (state, event) -> eventType.isInstance(event)
                        ? decider.evolve(state, eventType.cast(event))
                        : state,
                decider::isTerminal);
    }

    /**
     * Combine several deciders that work over the same command and event types into one. The combined decider keeps each
     * decider's own state side by side in a {@link CompositeState}. A command is handed to whichever decider recognizes
     * it, and each event updates only the decider that understands it, so the deciders stay independent. The combined
     * decider is terminal once all of them are.
     * <p>
     * Use {@link #adapt} first so the deciders share the same command and event types. Read each decider's state back
     * from the result with {@link CompositeState#slice(int)}, by the order the deciders were passed in.
     *
     * <pre>
     * Decider&lt;DomainCommand, CompositeState, DomainEvent&gt; combined = Decider.compose(
     *         Decider.adapt(courseDecider, CourseCommand.class, CourseEvent.class),
     *         Decider.adapt(studentDecider, StudentCommand.class, StudentEvent.class));
     *
     * CompositeState state = combined.initialState();
     * CourseState course = state.slice(0);   // first decider's state
     * StudentState student = state.slice(1);  // second decider's state
     * </pre>
     */
    @SafeVarargs
    static <C, E> Decider<C, CompositeState, E> compose(@NonNull Decider<C, ?, E>... deciders) {
        return compose(Arrays.asList(deciders));
    }

    /**
     * Like {@link #compose(Decider[])} but takes the deciders as a list, for when the number is not known at compile time.
     */
    static <C, E> Decider<C, CompositeState, E> compose(@NonNull List<? extends Decider<C, ?, E>> deciders) {
        @SuppressWarnings("unchecked")
        List<Decider<C, Object, E>> slices = (List<Decider<C, Object, E>>) new ArrayList<>(deciders);

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