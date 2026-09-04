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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.saga.internal.TypeDispatch;
import org.occurrent.filter.Filter;
import org.occurrent.filter.internal.EventTypeExpansion;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * A saga, more precisely an event-driven <em>process manager</em>, is pure data and pure functions: it reacts to domain
 * events, and to its own timeouts, by issuing commands while holding per-instance state. Nothing here performs I/O. An
 * executor feeds the saga its inputs, folds and persists its state, and interprets the {@link SagaEffect}s it returns.
 * <p>
 * A saga is <em>not</em> a substitute for a Dynamic Consistency Boundary. When two rules must hold atomically in one
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
 * <h4>If you already know Decider</h4>
 * <p>
 * A saga is the command-issuing mirror of {@code org.occurrent.dsl.decider.Decider}, so skip this section if you have
 * not met that type. A decider turns commands into events. A saga turns events, and its own timeouts, into commands. The
 * duality is exact in the type parameters, not merely described:
 * <pre>{@code
 * Decider<C, S, E>   // C command  -> S state -> E event(s)
 * Saga   <E, S, C>   // E event    -> S state -> C command(s)
 * }</pre>
 * Same three type variables, same roles, the arrows reversed.
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
     * before that event's {@link #react}. The canonical use is arming the first timeout for the process. {@code metadata}
     * is the start event's delivery metadata (stream id and version, global position, CloudEvent extensions). This is the
     * canonical form the executor calls, so an implementation overrides this one. The event-only
     * {@link #onStart(Object, Object)} is a convenience for callers that have no metadata and delegates here.
     * Default: none.
     */
    default List<SagaEffect<C>> onStart(S state, EventMetadata metadata, E startEvent) {
        return List.of();
    }

    /**
     * Event-only convenience for a caller that has no metadata, delegating to {@link #onStart(Object, EventMetadata, Object)}
     * with empty metadata. An implementation overrides the metadata-carrying form rather than this one, since that is the
     * form the executor calls.
     */
    default List<SagaEffect<C>> onStart(S state, E startEvent) {
        return onStart(state, EventMetadata.empty(), startEvent);
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
     * The refusal a saga reports for an event type its subscription cannot be built from. Shared by {@link Builder},
     * {@link #create} and {@code FlowSaga.Builder} so all three say the same thing.
     */
    static IllegalArgumentException cannotSubscribeOn(Class<?> eventType) {
        if (eventType.isArray()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a declared event type, since this expansion does not support an array. Declare the concrete event types instead.");
        }
        if (eventType.isPrimitive()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a declared event type, since no event is ever an instance of a primitive type. Declare the concrete event types instead.");
        }
        return new IllegalArgumentException("the concrete event types dispatch would accept for " + eventType.getName()
                + " cannot all be enumerated, so a filter derived from it would miss some of them. Declare the concrete "
                + "event types instead, make " + eventType.getSimpleName() + " and every level below it final or sealed, "
                + "or set a replacementFilter(...), which is used instead of deriving one and is the way out when a "
                + "CloudEventTypeMapper of your own maps the whole hierarchy onto a single CloudEvent type string.");
    }

    /**
     * All event types the saga reacts to, the default subscription selector (mirrors {@code Projection#eventTypes()}).
     * Empty means "no type narrowing". Feeding a saga a broader stream is safe: an input it does not handle folds to the
     * same state and produces no effects.
     * <p>
     * A saga built by {@link Builder} or {@code FlowSaga.Builder} reports more than it was given whenever it declares a
     * sealed type, because every concrete type that type permits is in here too. Handler lookup already accepts a
     * supertype, so those concrete types are the ones a subscription has to ask for.
     * <p>
     * A declared type whose concrete types cannot all be found is refused when the saga is built, since a filter derived
     * from it would miss some of them. Under a {@link #replacementFilter()} no filter is derived, so that type is
     * accepted and reported here with whatever concrete types could be found.
     */
    default Set<Class<? extends E>> eventTypes() {
        return Set.of();
    }

    /**
     * An extra condition every selected event must also match, combined with the selector derived from
     * {@link #eventTypes()} instead of replacing it, so a saga can select on subject, source, data or time while still
     * asking for its own event types. {@code null}, the default, means no extra condition.
     * <p>
     * Two things are yours to get right rather than Occurrent's to check. The condition has to admit the
     * {@link #startEventTypes()}, because one that excludes them means no instance is ever created. It also has to
     * admit the events that move an instance on, because an instance whose later events are excluded never reaches
     * {@link #isTerminal(Object)} and stays alive with its timers running.
     * <p>
     * A flow saga adds a third, which {@link #replacementFilter()} has too whenever it is narrower than the declared
     * types. A guard reads what has arrived, so a selector that excludes an event type changes the answer
     * {@code received.none(Rejected.class)} gives, and a branch can fire that would not have fired otherwise.
     * <p>
     * A {@code Filter.data(..)} condition is refused when the saga subscribes if the subscription model cannot read
     * event payloads, which is what a push subscription model does by default.
     * <p>
     * On a saga built by {@link Builder}, {@code FlowSaga.Builder} or {@link #create}, the build-time check that refuses
     * a declared type whose concrete types cannot all be found still runs under a narrowing, since a selector is still
     * derived. {@link #replacementFilter()} is what switches it off.
     * <p>
     * That check belongs to those three, so a saga written by implementing this interface directly never runs it, at any
     * {@link #eventTypes()}. A filter is still derived for it, so a declared type whose concrete types cannot all be
     * found leaves that saga subscribing on a filter naming fewer types than its own dispatch accepts, which under the
     * type mappers Occurrent ships means missing events. Declaring the concrete types is the answer, the same one the
     * builders would have insisted on.
     * <p>
     * A saga declaring no event types and setting no replacement derives a selector matching everything, so its
     * narrowing is the whole selector. For that saga the conversion obligation on {@link #replacementFilter()} applies
     * to the narrowing as well.
     */
    default @Nullable Filter narrowingFilter() {
        return null;
    }

    /**
     * An explicit selector used instead of one derived from {@link #eventTypes()}, so a saga can subscribe on something
     * a derived selector cannot express. {@code null}, the default, means derive one.
     * <p>
     * It is also how a saga runs over an event hierarchy whose concrete types cannot all be found. Such a declared type
     * is refused when the saga is built, and setting this skips that check, because nothing is derived. The check is
     * skipped for <em>every</em> declared type on this saga rather than only the one you could not enumerate, so a
     * selector set for an unrelated reason also stops you being told about a sealed hierarchy that was reopened
     * somewhere else. An array or a primitive declared type is still refused. The case that needs this is a
     * {@code CloudEventTypeMapper} of your own that maps a whole hierarchy onto a single CloudEvent type string.
     * <p>
     * Both things {@link #narrowingFilter()} asks of you apply here too, and two more come with them.
     * <p>
     * Every CloudEvent this admits is converted to a domain event before the saga sees it, so keep it inside what the
     * converter can turn into an {@code E}. One it cannot fails that delivery rather than being ignored, and a
     * subscription that keeps redelivering a failing event holds up everything queued behind it.
     * <p>
     * A converted event the saga has no handler for costs the two builders differently. A saga from {@link Builder}
     * leaves its state untouched. A flow saga appends every correlated event it receives to the instance's retained
     * history before it looks at which branch handles it, so a selector broader than the types the flow names grows
     * that history. Such an event never counts against a {@code stepWindow} cap and never evicts one of the step's
     * own events to make room for itself, but nothing evicts it either as long as the step's own declared-type
     * events stay within their cap, so it can grow what a parked step stores without limit. See the flow saga's
     * {@code stepWindow} javadoc for what that leaves you responsible for under a selector this broad.
     * <p>
     * Set together with a {@link #narrowingFilter()}, this is what the narrowing is combined with.
     */
    default @Nullable Filter replacementFilter() {
        return null;
    }

    /** One live transition: {@link #evolve} then {@link #react}. What an executor runs per input, and what tests assert on. */
    default Step<S, C> step(S state, SagaInput<E> input) {
        S evolved = evolve(state, input);
        return new Step<>(evolved, react(evolved, input));
    }

    /** The outcome of one transition: the new {@code state} and the {@code effects} it produced. Mirror of {@code Decider.Decision}. */
    record Step<S extends @Nullable Object, C>(S state, List<SagaEffect<C>> effects) {
        public Step {
            effects = List.copyOf(effects);
        }

        /**
         * The commands this transition issued, in the order {@link #effects()} holds them, and empty when it issued none.
         * This is usually what a test wants, because {@link #effects()} is a mixed list and a transition that issues
         * nothing is not an empty one: leaving a step whose timeout was armed contributes a
         * {@link SagaEffect.CancelTimeout} that says nothing about what the reaction decided.
         * <p>
         * See {@link #timerEffects()} for the other half of the split.
         * <p>
         * This is the same reading an executor performs, so an assertion here is an assertion about what would actually
         * be dispatched. Computed on each call, and unmodifiable. Kotlin callers write {@code step.issuedCommands()},
         * with the parentheses, because this is a derived accessor rather than a record component.
         */
        public List<C> issuedCommands() {
            List<C> commands = new ArrayList<>(effects.size());
            for (SagaEffect<C> effect : effects) {
                // Only IssueCommand carries a C, so this is a checked narrowing rather than an unchecked cast. The
                // exhaustive interpreter is SagaExecutionSupport.applyEffects, which stops compiling when a new
                // SagaEffect variant lands, so this accessor gets revisited at the same time.
                if (effect instanceof SagaEffect.IssueCommand<C> issue) {
                    commands.add(issue.command());
                }
            }
            return List.copyOf(commands);
        }

        /**
         * The timers this transition started or cancelled, in the order {@link #effects()} holds them, and empty when it
         * touched none. The timer half of {@link #effects()}, as {@link #issuedCommands()} is the command half.
         * <p>
         * Use it when a reaction both issues a command and touches a timer, so you can check the timers on their own:
         * {@code assertThat(step.timerEffects()).containsExactly(SagaEffect.startTimeout("payment", ofMinutes(30)))}
         * checks the timers and ignores the commands.
         * <p>
         * Starting a timer, starting one at a given time, and cancelling one all arrive here together, because they
         * carry different values and it reads better to match on them than to have three methods. The element type is
         * {@link SagaEffect.TimerEffect}, so this list cannot statically hold a command any more than
         * {@link #issuedCommands()} can hold a timer. Computed on each call, and unmodifiable.
         */
        public List<SagaEffect.TimerEffect<C>> timerEffects() {
            List<SagaEffect.TimerEffect<C>> timers = new ArrayList<>(effects.size());
            for (SagaEffect<C> effect : effects) {
                // Matched exhaustively over the sealed partition rather than as "not a command", so a new SagaEffect
                // variant that is neither a command nor a timer stops compiling here and has to be classified
                // deliberately. A negation would silently report such a variant as a timer.
                switch (effect) {
                    case SagaEffect.IssueCommand<C> ignored -> {
                    }
                    case SagaEffect.TimerEffect<C> timer -> timers.add(timer);
                }
            }
            return List.copyOf(timers);
        }
    }

    /**
     * The metadata-carrying fold for one event type, the three-argument sibling of a {@code BiFunction<S, T, S>}: it also
     * receives the event's delivery {@link EventMetadata}. Java has no three-argument {@code BiFunction}, so the builder's
     * metadata {@link Builder#evolve(Class, EventEvolver)} overload takes this instead.
     */
    @FunctionalInterface
    interface EventEvolver<S extends @Nullable Object, T> {
        S evolve(S state, EventMetadata metadata, T event);
    }

    /**
     * The metadata-carrying reaction for one event type, the three-argument sibling of a
     * {@code BiFunction<S, T, List<SagaEffect<C>>>}: it also receives the event's delivery {@link EventMetadata}. Used by
     * the builder's metadata {@link Builder#react(Class, EventReactor)} and {@link Builder#onStart(EventReactor)} overloads.
     */
    @FunctionalInterface
    interface EventReactor<S extends @Nullable Object, T, C> {
        List<SagaEffect<C>> react(S state, EventMetadata metadata, T event);
    }

    /**
     * Starts building a saga whose fold begins from {@code initialState}. Register correlation, start types, and the
     * per-event-type and per-timer folds and reactions on the returned {@link Builder}.
     */
    static <E, S extends @Nullable Object, C> Builder<E, S, C> builder(S initialState) {
        return new Builder<>(initialState);
    }

    /**
     * Starts building a saga whose fold begins from no state. The state stays {@code null} until an {@code evolve}
     * handler replaces it. Register correlation, start types, and the per-event-type and per-timer folds and
     * reactions on the returned {@link Builder}.
     */
    static <E, S extends @Nullable Object, C> Builder<E, S, C> builder() {
        return builder(null);
    }

    /**
     * Create a saga from functions instead of the {@link Builder}, the escape hatch mirroring {@code Decider#create}. The
     * supplied {@code evolve}/{@code react} handle the whole {@link SagaInput} union themselves. This saga is never
     * terminal and has no {@code onStart}. Implement the interface directly for those. {@code startEventTypes} must be
     * non-empty, since a saga with no start type can never create an instance, the same guarantee {@link Builder#build()}
     * gives, and for the same reason a start type nothing can be an instance of, an array or a primitive, is refused
     * whether or not {@code eventTypes} narrows the subscription.
     */
    static <E, S extends @Nullable Object, C> Saga<E, S, C> create(S initialState,
                                                                   Function<E, @Nullable String> sagaId,
                                                                   Set<Class<? extends E>> startEventTypes,
                                                                   Set<Class<? extends E>> eventTypes,
                                                                   BiFunction<S, SagaInput<E>, S> evolve,
                                                                   BiFunction<S, SagaInput<E>, List<SagaEffect<C>>> react) {
        return create(initialState, sagaId, startEventTypes, eventTypes, evolve, react, null);
    }

    /**
     * As {@link #create(Object, Function, Set, Set, BiFunction, BiFunction)}, with a {@link #replacementFilter()} used
     * instead of the selector derived from {@code eventTypes}. Pass {@code null} to derive one, which is what the
     * six-argument form does.
     * <p>
     * It is what lets this factory build a saga over an event hierarchy whose concrete types cannot all be found, since
     * nothing is derived and so nothing is refused. {@code eventTypes} is still worth declaring, because it stays the
     * saga's answer to which event types it handles.
     * <p>
     * There is no {@link #narrowingFilter()} here, and the saga this returns cannot be given one afterwards, since it is
     * an anonymous implementation. Both builders have the method. Implement the interface directly for a narrowing, the
     * same as for {@link #onStart(Object, Object)} and {@link #isTerminal(Object)} above. A saga written that way runs
     * no build-time check on its declared types, so declare concrete ones, which is what {@link #narrowingFilter()}
     * says about that route.
     */
    static <E, S extends @Nullable Object, C> Saga<E, S, C> create(S initialState,
                                                                   Function<E, @Nullable String> sagaId,
                                                                   Set<Class<? extends E>> startEventTypes,
                                                                   Set<Class<? extends E>> eventTypes,
                                                                   BiFunction<S, SagaInput<E>, S> evolve,
                                                                   BiFunction<S, SagaInput<E>, List<SagaEffect<C>>> react,
                                                                   @Nullable Filter replacementFilter) {
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
        // subscription filter, so a start type left out of a non-empty eventTypes would be filtered off the subscription
        // and no instance could ever be created. An empty eventTypes still means "no type narrowing" (subscribe to
        // everything), so only widen a set the caller has already narrowed. Under an explicit filter the union no longer
        // decides what arrives, and it is kept so that eventTypes() answers the same question either way.
        Set<Class<? extends E>> types;
        if (eventTypes.isEmpty()) {
            // Nothing narrows the subscription, so no filter is derived and there is no hierarchy to walk. A start type
            // nothing can be an instance of is still refused here, because that saga would build and then never create
            // an instance, which is the failure the non-empty branch and both builders already catch.
            for (Class<? extends E> start : starts) {
                EventTypeExpansion.refuseArrayOrPrimitive(start, Saga::cannotSubscribeOn);
            }
            types = Set.of();
        } else {
            Set<Class<? extends E>> union = new LinkedHashSet<>(eventTypes);
            union.addAll(starts);
            // No filter is derived under a replacement, so the walk that refuses a type a derived filter would miss
            // has nothing to protect and only reports what it finds.
            types = replacementFilter == null
                    ? EventTypeExpansion.expand(union, Saga::cannotSubscribeOn)
                    : EventTypeExpansion.expandWhatCanBeFound(union, Saga::cannotSubscribeOn);
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

            @Override
            public @Nullable Filter replacementFilter() {
                return replacementFilter;
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
            public List<SagaEffect<C>> onStart(S state, EventMetadata metadata, E startEvent) {
                return eventType.isInstance(startEvent) ? widen(saga.onStart(state, metadata, eventType.cast(startEvent))) : List.of();
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

            @Override
            public @Nullable Filter narrowingFilter() {
                return saga.narrowingFilter();
            }

            @Override
            public @Nullable Filter replacementFilter() {
                // Both carried across for the same reason eventTypes() is. Widening changes the Java type the saga is
                // driven through, never which stored events it wants, so the wrapped saga's selector is still right.
                return saga.replacementFilter();
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
        private final Map<Class<?>, EventEvolver<S, E>> eventEvolvers = new LinkedHashMap<>();
        private final Map<Class<?>, EventReactor<S, E, C>> eventReactors = new LinkedHashMap<>();
        private final Map<TimerName, BiFunction<S, SagaTimeout, S>> timeoutEvolvers = new LinkedHashMap<>();
        private final Map<TimerName, BiFunction<S, SagaTimeout, List<SagaEffect<C>>>> timeoutReactors = new LinkedHashMap<>();
        private final Map<Class<?>, Function<E, @Nullable String>> correlators = new LinkedHashMap<>();
        private final Set<Class<? extends E>> startTypes = new LinkedHashSet<>();
        private @Nullable Function<E, @Nullable String> correlateAll;
        private @Nullable EventReactor<S, E, C> onStart;
        private @Nullable Predicate<S> isTerminal;
        private @Nullable Filter narrowingFilter;
        private @Nullable Filter replacementFilter;

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
        public <T extends E> Builder<E, S, C> evolve(Class<T> type, BiFunction<S, ? super T, S> handler) {
            requireNonNull(handler, "handler cannot be null");
            EventEvolver<S, T> wrapped = (state, metadata, event) -> handler.apply(state, event);
            return evolve(type, wrapped);
        }

        /**
         * Registers the metadata-carrying fold for one event type: the handler also receives the event's delivery
         * {@link EventMetadata}. The metadata-first sibling of {@link #evolve(Class, BiFunction)}, which delegates here.
         * Registering the same type twice throws.
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, S, C> evolve(Class<T> type, EventEvolver<S, ? super T> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (eventEvolvers.containsKey(type)) {
                throw new IllegalStateException("evolve(...) has already been registered for " + type.getName());
            }
            eventEvolvers.put(type, (EventEvolver<S, E>) handler);
            return this;
        }

        /** Registers the reaction for one event type, given the post-evolve state. Registering the same type twice throws. */
        public <T extends E> Builder<E, S, C> react(Class<T> type, BiFunction<S, ? super T, List<SagaEffect<C>>> handler) {
            requireNonNull(handler, "handler cannot be null");
            EventReactor<S, T, C> wrapped = (state, metadata, event) -> handler.apply(state, event);
            return react(type, wrapped);
        }

        /**
         * Registers the metadata-carrying reaction for one event type, given the post-evolve state: the handler also
         * receives the event's delivery {@link EventMetadata}. The metadata-first sibling of {@link #react(Class, BiFunction)},
         * which delegates here. Registering the same type twice throws.
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, S, C> react(Class<T> type, EventReactor<S, ? super T, C> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (eventReactors.containsKey(type)) {
                throw new IllegalStateException("react(...) has already been registered for " + type.getName());
            }
            eventReactors.put(type, (EventReactor<S, E, C>) handler);
            return this;
        }

        /** Registers the fold for one named timer firing. Registering the same name twice throws. */
        public Builder<E, S, C> evolveOnTimeout(TimerName timerName, BiFunction<S, SagaTimeout, S> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (timeoutEvolvers.containsKey(timerName)) {
                throw new IllegalStateException("evolveOnTimeout(...) has already been registered for timer " + timerName);
            }
            timeoutEvolvers.put(timerName, handler);
            return this;
        }

        /**
         * Registers the fold for one named timer firing, naming it with a string that {@link TimerName#parse(String)}
         * reads. Registering the same name twice throws, and {@code "a:b"} is the same name as
         * {@code TimerName.of("a", "b")}.
         */
        public Builder<E, S, C> evolveOnTimeout(String timerName, BiFunction<S, SagaTimeout, S> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            return evolveOnTimeout(TimerName.parse(timerName), handler);
        }

        /**
         * Registers the reaction for one named timer firing, given the post-evolve state. Registering the same name twice
         * throws. A fired timer with no reaction registered here (and no {@link #evolveOnTimeout}) is consumed without
         * changing state or issuing a command, so every {@link SagaEffect#startTimeout} you arm needs a matching handler.
         */
        public Builder<E, S, C> reactOnTimeout(TimerName timerName, BiFunction<S, SagaTimeout, List<SagaEffect<C>>> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            requireNonNull(handler, "handler cannot be null");
            if (timeoutReactors.containsKey(timerName)) {
                throw new IllegalStateException("reactOnTimeout(...) has already been registered for timer " + timerName);
            }
            timeoutReactors.put(timerName, handler);
            return this;
        }

        /**
         * Registers the reaction for one named timer firing, naming it with a string that
         * {@link TimerName#parse(String)} reads. Registering the same name twice throws, and {@code "a:b"} is the same
         * name as {@code TimerName.of("a", "b")}.
         */
        public Builder<E, S, C> reactOnTimeout(String timerName, BiFunction<S, SagaTimeout, List<SagaEffect<C>>> handler) {
            requireNonNull(timerName, "timerName cannot be null");
            return reactOnTimeout(TimerName.parse(timerName), handler);
        }

        /** Effects to run once when a start event creates the instance. Optional, can be set only once. */
        public Builder<E, S, C> onStart(BiFunction<S, ? super E, List<SagaEffect<C>>> onStart) {
            requireNonNull(onStart, "onStart cannot be null");
            EventReactor<S, E, C> wrapped = (state, metadata, event) -> onStart.apply(state, event);
            return onStart(wrapped);
        }

        /**
         * Effects to run once when a start event creates the instance, with the start event's delivery {@link EventMetadata}.
         * The metadata-first sibling of {@link #onStart(BiFunction)}, which delegates here. Optional, can be set only once.
         */
        public Builder<E, S, C> onStart(EventReactor<S, ? super E, C> onStart) {
            if (this.onStart != null) {
                throw new IllegalStateException("onStart(...) has already been set and can only be set once");
            }
            @SuppressWarnings("unchecked")
            EventReactor<S, E, C> widened = (EventReactor<S, E, C>) onStart;
            this.onStart = requireNonNull(widened, "onStart cannot be null");
            return this;
        }

        /**
         * Adds a condition every selected event must also match, on top of the selector derived from the registered
         * event types, so the saga can select on subject, source, data or time and still ask for its own event types.
         * See {@link Saga#narrowingFilter()} for what that leaves you responsible for. Optional, can be set only once.
         */
        public Builder<E, S, C> narrowingFilter(Filter narrowingFilter) {
            if (this.narrowingFilter != null) {
                throw new IllegalStateException("narrowingFilter(...) has already been set and can only be set once");
            }
            this.narrowingFilter = requireNonNull(narrowingFilter, "narrowingFilter cannot be null");
            return this;
        }

        /**
         * Sets an explicit selector used instead of deriving one from the registered event types. It also builds a saga
         * over a hierarchy whose concrete types cannot all be found, which is otherwise refused here, since nothing is
         * derived and so nothing is refused. See {@link Saga#replacementFilter()} for what that leaves you responsible
         * for. Optional, can be set only once, and can be combined with {@link #narrowingFilter(Filter)}.
         */
        public Builder<E, S, C> replacementFilter(Filter replacementFilter) {
            if (this.replacementFilter != null) {
                throw new IllegalStateException("replacementFilter(...) has already been set and can only be set once");
            }
            this.replacementFilter = requireNonNull(replacementFilter, "replacementFilter cannot be null");
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

            Set<Class<? extends E>> declaredTypes = new LinkedHashSet<>();
            for (Class<?> type : eventEvolvers.keySet()) {
                declaredTypes.add((Class<? extends E>) type);
            }
            for (Class<?> type : eventReactors.keySet()) {
                declaredTypes.add((Class<? extends E>) type);
            }
            declaredTypes.addAll(startTypes);
            // No filter is derived under a replacement, so the walk that refuses a type a derived filter would miss has
            // nothing to protect and only reports what it finds. A narrowing does not key this, because the selector is
            // still derived and so still has to name every type. Coverage below runs over the same shape either way.
            Set<Class<? extends E>> allTypes = replacementFilter == null
                    ? EventTypeExpansion.expand(declaredTypes, Saga::cannotSubscribeOn)
                    : EventTypeExpansion.expandWhatCanBeFound(declaredTypes, Saga::cannotSubscribeOn);
            if (correlateAll == null) {
                TypeDispatch<Function<E, @Nullable String>> coverage = new TypeDispatch<>(correlators);
                for (Class<?> type : allTypes) {
                    if (coverage.resolve(type) == null) {
                        throw new IllegalStateException("event type " + type.getName() + " has no correlation; register correlate("
                                + type.getSimpleName() + ".class, ...) or a correlateAll(...) fallback before build()");
                    }
                }
            }

            S initial = this.initialState;
            TypeDispatch<EventEvolver<S, E>> evolveDispatch = new TypeDispatch<>(eventEvolvers);
            TypeDispatch<EventReactor<S, E, C>> reactDispatch = new TypeDispatch<>(eventReactors);
            TypeDispatch<Function<E, @Nullable String>> correlateDispatch = new TypeDispatch<>(correlators);
            Map<TimerName, BiFunction<S, SagaTimeout, S>> timeoutEvolveByName = new LinkedHashMap<>(timeoutEvolvers);
            Map<TimerName, BiFunction<S, SagaTimeout, List<SagaEffect<C>>>> timeoutReactByName = new LinkedHashMap<>(timeoutReactors);
            Function<E, @Nullable String> allCorrelator = this.correlateAll;
            EventReactor<S, E, C> onStartFn = this.onStart;
            Predicate<S> terminalFn = this.isTerminal;
            Set<Class<? extends E>> starts = Set.copyOf(startTypes);
            Set<Class<? extends E>> types = allTypes;
            Filter narrowing = this.narrowingFilter;
            Filter replacement = this.replacementFilter;

            return new Saga<>() {
                @Override
                public S initialState() {
                    return initial;
                }

                @Override
                public S evolve(S state, SagaInput<E> input) {
                    return switch (input) {
                        case SagaInput.Event<E> ev -> {
                            EventEvolver<S, E> handler = evolveDispatch.resolve(ev.event().getClass());
                            yield handler == null ? state : handler.evolve(state, ev.metadata(), ev.event());
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
                            EventReactor<S, E, C> handler = reactDispatch.resolve(ev.event().getClass());
                            yield handler == null ? List.of() : handler.react(state, ev.metadata(), ev.event());
                        }
                        case SagaInput.Timeout<E> to -> {
                            BiFunction<S, SagaTimeout, List<SagaEffect<C>>> handler = timeoutReactByName.get(to.timeout().timerName());
                            yield handler == null ? List.of() : handler.apply(state, to.timeout());
                        }
                    };
                }

                @Override
                public List<SagaEffect<C>> onStart(S state, EventMetadata metadata, E startEvent) {
                    return onStartFn == null ? List.of() : onStartFn.react(state, metadata, startEvent);
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

                @Override
                public @Nullable Filter narrowingFilter() {
                    return narrowing;
                }

                @Override
                public @Nullable Filter replacementFilter() {
                    return replacement;
                }
            };
        }
    }
}
