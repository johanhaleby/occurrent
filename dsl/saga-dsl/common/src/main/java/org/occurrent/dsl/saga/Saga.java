/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.internal.TypeDispatch;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * A saga (more precisely an event-driven <em>process manager</em>) described as pure data and pure functions: the
 * command-issuing mirror of {@code org.occurrent.dsl.decider.Decider}. A decider turns commands into events. A saga turns
 * events, and its own timeouts, into commands. Nothing here performs I/O. An executor feeds a saga inputs, folds and
 * persists its state, and interprets the {@link SagaEffect}s it returns.
 * <p>
 * The duality is exact in the type parameters, mirrored (not just described) end to end:
 * <pre>{@code
 * Decider<C, S, E>   // C command  -> S state -> E event(s)
 * Saga   <E, S, C>   // E event    -> S state -> C command(s)
 * }</pre>
 * A decider consumes a command and its own state and produces events; a saga consumes an event (or one of its own
 * timeouts) and its own state and produces commands. Same three type variables, same roles, the arrows reversed.
 * <p>
 * A saga is <em>not</em> a substitute for a Dynamic Consistency Boundary: when two rules must hold atomically in one
 * append, use DCB. A saga is for genuinely cross-boundary, time-involving, eventually-consistent processes, such as
 * "cancel the order if payment is not reserved within 30 minutes".
 *
 * <h2>Semantics an executor must honour</h2>
 * <ul>
 *   <li>An event with a {@code null} {@link #sagaId(Object)} belongs to no instance and is skipped.</li>
 *   <li>A correlated event whose type is not in {@link #startEventTypes()} and for which no instance exists is skipped.</li>
 *   <li>For each input the executor computes {@code s' = evolve(state, input)}, then {@code react(s', input)}: react sees
 *       the state <em>after</em> the fold. Replay only ever folds {@link #evolve}, so it produces no effects.</li>
 *   <li>{@link #onStart(Object, Object)} runs exactly once, when a start event creates the instance, after the first
 *       {@code evolve} and before that event's {@code react}, and the effects are concatenated in that order.</li>
 *   <li>{@link #isTerminal(Object)} is absorbing: a terminal instance ignores further inputs, and the executor cancels
 *       all of the instance's outstanding timers when a state first becomes terminal.</li>
 *   <li>Timeouts never start an instance and are ignored by a terminal instance.</li>
 * </ul>
 *
 * @param <E> the domain event type the saga reacts to
 * @param <S> the saga instance state
 * @param <C> the command type the saga issues
 */
// State returns below use the plain type variable S (parametric nullness), never @Nullable S. See the same guard comment
// in Decider.java: annotating them @Nullable forces the return nullable even for a non-null S and breaks state chaining.
public interface Saga<E, S extends @Nullable Object, C> {

    /** The state a new saga instance starts from, before its start event has been applied. */
    S initialState();

    /**
     * Fold {@code input} (a domain event or a fired timer) onto {@code state} and return the new state. Pure. This is the
     * only function used when rehydrating an instance from history, so replay never produces effects.
     */
    S evolve(S state, SagaInput<E> input);

    /**
     * Decide what should happen now that {@code input} has been applied. {@code state} is the state <em>after</em>
     * {@link #evolve}. Returns the effects to run (commands to issue, timers to start or cancel), or an empty list. Pure:
     * never called during replay.
     */
    List<SagaEffect<C>> react(S state, SagaInput<E> input);

    /**
     * Effects to run exactly once when a start event creates a new instance, called after the first {@link #evolve} and
     * before that event's {@link #react}. The canonical use is arming the first timeout for the process. Default: none.
     */
    default List<SagaEffect<C>> onStart(S state, E startEvent) {
        return List.of();
    }

    /**
     * Whether {@code state} is terminal. Absorbing (same contract as {@code Decider#isTerminal}): a terminal instance
     * ignores all further inputs, and the executor cancels the instance's outstanding timers when a state first becomes
     * terminal. Default: never terminal.
     */
    default boolean isTerminal(S state) {
        return false;
    }

    /**
     * Which saga instance {@code event} belongs to, or {@code null} if it belongs to none (and should be skipped). The id
     * is a {@code String} so it round-trips losslessly through whatever the executor persists.
     */
    @Nullable String sagaId(E event);

    /** The event types that create a new instance when none exists for the correlation id. */
    Set<Class<? extends E>> startEventTypes();

    /**
     * All event types the saga reacts to, the default subscription selector (mirrors {@code Projection#eventTypes()}).
     * Empty means "no type narrowing". Feeding a saga a broader stream is safe: an input it does not handle folds to the
     * same state and produces no effects.
     */
    default Set<Class<? extends E>> eventTypes() {
        return Set.of();
    }

    /** One live transition: {@link #evolve} then {@link #react}. What an executor runs per input, and what tests assert on. */
    default Step<S, C> step(S state, SagaInput<E> input) {
        S evolved = evolve(state, input);
        return new Step<>(evolved, react(evolved, input));
    }

    /** The outcome of one transition: the new {@code state} and the {@code effects} it produced. Mirror of {@code Decider.Decision}. */
    record Step<S extends @Nullable Object, C>(S state, List<SagaEffect<C>> effects) {
    }

    /**
     * Starts building a saga whose fold begins from {@code initialState}. Register correlation, start types, and the
     * per-event-type and per-timer folds and reactions on the returned {@link Builder}.
     */
    static <E, S extends @Nullable Object, C> Builder<E, S, C> builder(S initialState) {
        return new Builder<>(initialState);
    }

    /**
     * Create a saga from functions instead of the {@link Builder}, the escape hatch mirroring {@code Decider#create}. The
     * supplied {@code evolve}/{@code react} handle the whole {@link SagaInput} union themselves. This saga is never
     * terminal and has no {@code onStart}. Implement the interface directly for those. {@code startEventTypes} must be
     * non-empty, since a saga with no start type can never create an instance, the same guarantee {@link Builder#build()}
     * gives.
     */
    static <E, S extends @Nullable Object, C> Saga<E, S, C> create(S initialState,
                                                                   Function<E, @Nullable String> sagaId,
                                                                   Set<Class<? extends E>> startEventTypes,
                                                                   Set<Class<? extends E>> eventTypes,
                                                                   BiFunction<S, SagaInput<E>, S> evolve,
                                                                   BiFunction<S, SagaInput<E>, List<SagaEffect<C>>> react) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(startEventTypes, "startEventTypes cannot be null");
        requireNonNull(eventTypes, "eventTypes cannot be null");
        requireNonNull(evolve, "evolve cannot be null");
        requireNonNull(react, "react cannot be null");
        Set<Class<? extends E>> starts = Set.copyOf(startEventTypes);
        if (starts.isEmpty()) {
            throw new IllegalArgumentException("a saga needs at least one start event type, startEventTypes cannot be empty");
        }
        // Union the start types into the subscription selector, exactly as Builder.build() does. eventTypes is the default
        // subscription filter: a start type left out of a non-empty eventTypes would be filtered off the subscription, so a
        // start event could never reach the saga and no instance could ever be created. An empty eventTypes still means
        // "no type narrowing" (subscribe to everything), so only widen a set the caller has already narrowed.
        Set<Class<? extends E>> types;
        if (eventTypes.isEmpty()) {
            types = Set.of();
        } else {
            Set<Class<? extends E>> union = new LinkedHashSet<>(eventTypes);
            union.addAll(starts);
            types = Set.copyOf(union);
        }
        return new Saga<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @Override
            public S evolve(S state, SagaInput<E> input) {
                return evolve.apply(state, input);
            }

            @Override
            public List<SagaEffect<C>> react(S state, SagaInput<E> input) {
                return react.apply(state, input);
            }

            @Override
            public @Nullable String sagaId(E event) {
                return sagaId.apply(event);
            }

            @Override
            public Set<Class<? extends E>> startEventTypes() {
                return starts;
            }

            @Override
            public Set<Class<? extends E>> eventTypes() {
                return types;
            }
        };
    }

    /**
     * Widen a saga so it can run against broader event and command types, mirroring {@code Decider#adapt}. Events that
     * are not {@code eventType} are ignored (the fold leaves the state unchanged, react produces no effects, correlation
     * returns {@code null}, and they never start an instance). Timeouts always belong to this saga and pass through.
     * Commands widen by covariance ({@code SubC extends C}).
     *
     * @param saga      the feature saga to widen
     * @param eventType the event type the saga understands
     */
    static <E, S extends @Nullable Object, C, SubE extends E, SubC extends C> Saga<E, S, C> adapt(Saga<SubE, S, SubC> saga, Class<SubE> eventType) {
        requireNonNull(saga, "saga cannot be null");
        requireNonNull(eventType, "eventType cannot be null");
        return new Saga<>() {
            @Override
            public S initialState() {
                return saga.initialState();
            }

            @Override
            public S evolve(S state, SagaInput<E> input) {
                return switch (input) {
                    case SagaInput.Event<E> ev -> eventType.isInstance(ev.event())
                            ? saga.evolve(state, SagaInput.event(eventType.cast(ev.event())))
                            : state;
                    case SagaInput.Timeout<E> to -> saga.evolve(state, SagaInput.timeout(to.timeout()));
                };
            }

            @Override
            public List<SagaEffect<C>> react(S state, SagaInput<E> input) {
                return switch (input) {
                    case SagaInput.Event<E> ev -> eventType.isInstance(ev.event())
                            ? widen(saga.react(state, SagaInput.event(eventType.cast(ev.event()))))
                            : List.of();
                    case SagaInput.Timeout<E> to -> widen(saga.react(state, SagaInput.timeout(to.timeout())));
                };
            }

            @Override
            public List<SagaEffect<C>> onStart(S state, E startEvent) {
                return eventType.isInstance(startEvent) ? widen(saga.onStart(state, eventType.cast(startEvent))) : List.of();
            }

            @Override
            public boolean isTerminal(S state) {
                return saga.isTerminal(state);
            }

            @Override
            public @Nullable String sagaId(E event) {
                return eventType.isInstance(event) ? saga.sagaId(eventType.cast(event)) : null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Set<Class<? extends E>> startEventTypes() {
                return (Set<Class<? extends E>>) (Set<?>) saga.startEventTypes();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Set<Class<? extends E>> eventTypes() {
                return (Set<Class<? extends E>>) (Set<?>) saga.eventTypes();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <C> List<SagaEffect<C>> widen(List<? extends SagaEffect<? extends C>> effects) {
        // Safe: IssueCommand's command is a C by covariance, and the timer effects carry no C at all.
        return (List<SagaEffect<C>>) (List<?>) effects;
    }

    /**
     * A type-safe builder assembling a saga from per-event-type and per-timer folds and reactions. Not thread-safe. Build
     * one, configure it, and call {@link #build()} once. Single-assignment methods throw {@link IllegalStateException} on
     * a second call, and registering the same event type or timer name twice throws, because that is a bug rather than an
     * intended override.
     */
    final class Builder<E, S extends @Nullable Object, C> {
        private final S initialState;
        private final Map<Class<?>, BiFunction<S, E, S>> eventEvolvers = new LinkedHashMap<>();
        private final Map<Class<?>, BiFunction<S, E, List<SagaEffect<C>>>> eventReactors = new LinkedHashMap<>();
        private final Map<String, BiFunction<S, SagaTimeout, S>> timeoutEvolvers = new LinkedHashMap<>();
        private final Map<String, BiFunction<S, SagaTimeout, List<SagaEffect<C>>>> timeoutReactors = new LinkedHashMap<>();
        private final Map<Class<?>, Function<E, @Nullable String>> correlators = new LinkedHashMap<>();
        private final Set<Class<? extends E>> startTypes = new LinkedHashSet<>();
        private @Nullable Function<E, @Nullable String> correlateAll;
        private @Nullable BiFunction<S, E, List<SagaEffect<C>>> onStart;
        private @Nullable Predicate<S> isTerminal;

        private Builder(S initialState) {
            this.initialState = initialState;
        }

        /**
         * Registers how to derive the correlation id from an event of type {@code T}. Return {@code null} to skip an
         * event that maps to no instance. Registering the same type twice throws.
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, S, C> correlate(Class<T> type, Function<T, @Nullable String> correlator) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(correlator, "correlator cannot be null");
            if (correlators.containsKey(type)) {
                throw new IllegalStateException("correlate(...) has already been registered for " + type.getName());
            }
            correlators.put(type, (Function<E, @Nullable String>) correlator);
            return this;
        }

        /**
         * Registers a fallback correlation function used for any event type without its own {@link #correlate}. The
         * common case is a sealed event hierarchy exposing a shared id. Can be set only once.
         */
        public Builder<E, S, C> correlateAll(Function<E, @Nullable String> correlator) {
            if (this.correlateAll != null) {
                throw new IllegalStateException("correlateAll(...) has already been set and can only be set once");
            }
            this.correlateAll = requireNonNull(correlator, "correlator cannot be null");
            return this;
        }

        /** Marks {@code type} as instance-creating. At least one is required. Registering the same type twice throws. */
        public <T extends E> Builder<E, S, C> startsOn(Class<T> type) {
            requireNonNull(type, "type cannot be null");
            if (!startTypes.add(type)) {
                throw new IllegalStateException("startsOn(...) has already been registered for " + type.getName());
            }
            return this;
        }

        /** Registers the fold for one event type. Registering the same type twice throws. */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, S, C> evolve(Class<T> type, BiFunction<S, ? super T, S> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (eventEvolvers.containsKey(type)) {
                throw new IllegalStateException("evolve(...) has already been registered for " + type.getName());
            }
            eventEvolvers.put(type, (BiFunction<S, E, S>) handler);
            return this;
        }

        /** Registers the reaction for one event type, given the post-evolve state. Registering the same type twice throws. */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, S, C> react(Class<T> type, BiFunction<S, ? super T, List<SagaEffect<C>>> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (eventReactors.containsKey(type)) {
                throw new IllegalStateException("react(...) has already been registered for " + type.getName());
            }
            eventReactors.put(type, (BiFunction<S, E, List<SagaEffect<C>>>) handler);
            return this;
        }

        /** Registers the fold for one named timer firing. Registering the same name twice throws. */
        public Builder<E, S, C> evolveOnTimeout(String timerName, BiFunction<S, SagaTimeout, S> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (timeoutEvolvers.containsKey(timerName)) {
                throw new IllegalStateException("evolveOnTimeout(...) has already been registered for timer " + timerName);
            }
            timeoutEvolvers.put(timerName, handler);
            return this;
        }

        /**
         * Registers the reaction for one named timer firing, given the post-evolve state. Registering the same name twice
         * throws. A fired timer with no reaction registered here (and no {@link #evolveOnTimeout}) is consumed without
         * changing state or issuing a command, so every {@link SagaEffect#startTimeout} you arm needs a matching handler.
         */
        public Builder<E, S, C> reactOnTimeout(String timerName, BiFunction<S, SagaTimeout, List<SagaEffect<C>>> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (timeoutReactors.containsKey(timerName)) {
                throw new IllegalStateException("reactOnTimeout(...) has already been registered for timer " + timerName);
            }
            timeoutReactors.put(timerName, handler);
            return this;
        }

        /** Effects to run once when a start event creates the instance. Optional, can be set only once. */
        public Builder<E, S, C> onStart(BiFunction<S, ? super E, List<SagaEffect<C>>> onStart) {
            if (this.onStart != null) {
                throw new IllegalStateException("onStart(...) has already been set and can only be set once");
            }
            @SuppressWarnings("unchecked")
            BiFunction<S, E, List<SagaEffect<C>>> widened = (BiFunction<S, E, List<SagaEffect<C>>>) onStart;
            this.onStart = requireNonNull(widened, "onStart cannot be null");
            return this;
        }

        /** The terminal predicate. Optional (default never terminal), can be set only once. */
        public Builder<E, S, C> isTerminal(Predicate<S> isTerminal) {
            if (this.isTerminal != null) {
                throw new IllegalStateException("isTerminal(...) has already been set and can only be set once");
            }
            this.isTerminal = requireNonNull(isTerminal, "isTerminal cannot be null");
            return this;
        }

        /**
         * Builds the saga. Fails loud if no {@link #startsOn} type was registered, or if any handled event type has no
         * correlation (its own {@link #correlate} or a {@link #correlateAll} fallback), the same coverage guarantee that
         * keeps "event arrived, no idea which instance" impossible at run time.
         */
        @SuppressWarnings("unchecked")
        public Saga<E, S, C> build() {
            if (startTypes.isEmpty()) {
                throw new IllegalStateException("a saga needs at least one startsOn(...) event type, call startsOn(...) before build()");
            }

            Set<Class<?>> handledTypes = new LinkedHashSet<>();
            handledTypes.addAll(eventEvolvers.keySet());
            handledTypes.addAll(eventReactors.keySet());
            handledTypes.addAll(startTypes);
            if (correlateAll == null) {
                TypeDispatch<Function<E, @Nullable String>> coverage = new TypeDispatch<>(correlators);
                for (Class<?> type : handledTypes) {
                    if (coverage.resolve(type) == null) {
                        throw new IllegalStateException("event type " + type.getName() + " has no correlation; register correlate("
                                + type.getSimpleName() + ".class, ...) or a correlateAll(...) fallback before build()");
                    }
                }
            }

            Set<Class<? extends E>> allTypes = new LinkedHashSet<>(startTypes);
            for (Class<?> type : eventEvolvers.keySet()) {
                allTypes.add((Class<? extends E>) type);
            }
            for (Class<?> type : eventReactors.keySet()) {
                allTypes.add((Class<? extends E>) type);
            }

            S initial = this.initialState;
            TypeDispatch<BiFunction<S, E, S>> evolveDispatch = new TypeDispatch<>(eventEvolvers);
            TypeDispatch<BiFunction<S, E, List<SagaEffect<C>>>> reactDispatch = new TypeDispatch<>(eventReactors);
            TypeDispatch<Function<E, @Nullable String>> correlateDispatch = new TypeDispatch<>(correlators);
            Map<String, BiFunction<S, SagaTimeout, S>> timeoutEvolveByName = new LinkedHashMap<>(timeoutEvolvers);
            Map<String, BiFunction<S, SagaTimeout, List<SagaEffect<C>>>> timeoutReactByName = new LinkedHashMap<>(timeoutReactors);
            Function<E, @Nullable String> allCorrelator = this.correlateAll;
            BiFunction<S, E, List<SagaEffect<C>>> onStartFn = this.onStart;
            Predicate<S> terminalFn = this.isTerminal;
            Set<Class<? extends E>> starts = Set.copyOf(startTypes);
            Set<Class<? extends E>> types = Set.copyOf(allTypes);

            return new Saga<>() {
                @Override
                public S initialState() {
                    return initial;
                }

                @Override
                public S evolve(S state, SagaInput<E> input) {
                    return switch (input) {
                        case SagaInput.Event<E> ev -> {
                            BiFunction<S, E, S> handler = evolveDispatch.resolve(ev.event().getClass());
                            yield handler == null ? state : handler.apply(state, ev.event());
                        }
                        case SagaInput.Timeout<E> to -> {
                            BiFunction<S, SagaTimeout, S> handler = timeoutEvolveByName.get(to.timeout().timerName());
                            yield handler == null ? state : handler.apply(state, to.timeout());
                        }
                    };
                }

                @Override
                public List<SagaEffect<C>> react(S state, SagaInput<E> input) {
                    return switch (input) {
                        case SagaInput.Event<E> ev -> {
                            BiFunction<S, E, List<SagaEffect<C>>> handler = reactDispatch.resolve(ev.event().getClass());
                            yield handler == null ? List.of() : handler.apply(state, ev.event());
                        }
                        case SagaInput.Timeout<E> to -> {
                            BiFunction<S, SagaTimeout, List<SagaEffect<C>>> handler = timeoutReactByName.get(to.timeout().timerName());
                            yield handler == null ? List.of() : handler.apply(state, to.timeout());
                        }
                    };
                }

                @Override
                public List<SagaEffect<C>> onStart(S state, E startEvent) {
                    return onStartFn == null ? List.of() : onStartFn.apply(state, startEvent);
                }

                @Override
                public boolean isTerminal(S state) {
                    return terminalFn != null && terminalFn.test(state);
                }

                @Override
                public @Nullable String sagaId(E event) {
                    Function<E, @Nullable String> correlator = correlateDispatch.resolve(event.getClass());
                    if (correlator != null) {
                        return correlator.apply(event);
                    }
                    return allCorrelator == null ? null : allCorrelator.apply(event);
                }

                @Override
                public Set<Class<? extends E>> startEventTypes() {
                    return starts;
                }

                @Override
                public Set<Class<? extends E>> eventTypes() {
                    return types;
                }
            };
        }
    }
}
