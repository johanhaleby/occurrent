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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.view.View;
import org.occurrent.filter.Filter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.addAll;
import static java.util.Objects.requireNonNull;

/**
 * A self-describing, capability-agnostic read model.
 * <p>
 * A plain {@link View} only knows how to fold events into state. To keep that state up to date from an event stream, a
 * caller must also know which view instance an event updates (the {@code id}) and which events feed the view (the
 * {@code eventTypes} to subscribe to, or an explicit {@code filter}). {@code Projection} couples those pieces together so
 * a feature can describe its read model right next to its fold, instead of that knowledge living separately in whatever
 * subscription or query wires the view to the store. It is the read-side mirror of
 * {@code org.occurrent.dsl.dcb.DcbDecider} on the write side.
 * <p>
 * Build one with the type-safe {@link #builder(Object) handler builder}: register a fold per event type with
 * {@link Builder#on(Class, BiFunction)} and the builder both assembles the {@link View} and records the handled event
 * types, so the subscription filter is derived from exactly the events the fold recognizes. The fold no-ops (returns the
 * state unchanged) for an event type with no registered handler, so it is always safe to feed a {@code Projection} a
 * broader stream than it handles.
 *
 * @param <S> the state type
 * @param <E> the event type
 * @param <ID> the view-instance id type
 */
public final class Projection<S extends @Nullable Object, E, ID> {

    private final View<S, E> view;
    private final @Nullable Function<E, @Nullable ID> id;
    private final Set<Class<? extends E>> eventTypes;
    private final @Nullable Filter filter;

    // Private on purpose: the only way to build a Projection is the builder/singletonBuilder/adapt factories, which fix
    // a single-instance projection's id type to String, so a non-String singleton cannot be constructed.
    private Projection(View<S, E> view, @Nullable Function<E, @Nullable ID> id, Set<Class<? extends E>> eventTypes, @Nullable Filter filter) {
        this.view = requireNonNull(view, "view cannot be null");
        this.id = id;
        this.eventTypes = Set.copyOf(requireNonNull(eventTypes, "eventTypes cannot be null"));
        this.filter = filter;
    }

    /** The pure fold: initial state and how an event evolves it. */
    public View<S, E> view() {
        return view;
    }

    /**
     * The function deriving which view instance an event updates, or {@code null} for a single-instance projection (the
     * framework supplies the single key). When present, the function may return {@code null} to skip an event that maps
     * to no keyed instance.
     */
    public @Nullable Function<E, @Nullable ID> id() {
        return id;
    }

    /** The event types the fold handles, the default subscription selector. Empty means "all types" (no type narrowing). */
    public Set<Class<? extends E>> eventTypes() {
        return eventTypes;
    }

    /**
     * An optional explicit selector that overrides the type-derived one, so a projection can select on more than event
     * type (subject, source, data, time). {@code null} means "derive the selector from {@code eventTypes}".
     */
    public @Nullable Filter filter() {
        return filter;
    }

    /**
     * Starts building a {@code Projection} whose fold begins from {@code initialState}. Register a handler per event type
     * with {@link Builder#on(Class, BiFunction)}, an {@link Builder#id(Function) id} function, and optionally an explicit
     * {@link Builder#filter(Filter) filter}, then call {@link Builder#build()}.
     */
    public static <S extends @Nullable Object, E, ID> Builder<S, E, ID> builder(S initialState) {
        return new Builder<>(initialState);
    }

    /**
     * Starts building a single-instance {@code Projection}: one that folds into a single view state rather than one per
     * key, so it needs no {@code id} function. The single slot is keyed at run time by the projection's own identity
     * (the subscription id, or the {@code @Projection} id), which is a {@code String}, so a single-instance projection
     * is always a {@code Projection<S, E, String>}. Register handlers with {@link Builder#on(Class, BiFunction)} and call
     * {@link Builder#build()}. Use {@link #builder(Object)} with {@link Builder#id(Function)} for a keyed, multi-instance
     * projection instead. The Kotlin equivalents are {@code singletonProjection}/{@code dcbSingletonProjection}.
     */
    public static <S extends @Nullable Object, E> Builder<S, E, String> singletonBuilder(S initialState) {
        Builder<S, E, String> builder = new Builder<>(initialState);
        builder.singleton();
        return builder;
    }

    /**
     * Widen a {@code Projection} so it can run against a broader event type, mirroring
     * {@code org.occurrent.dsl.decider.Decider#adapt}. The wrapped fold and id function are widened to ignore events that
     * are not {@code eventType} (the fold returns the state unchanged, the id returns {@code null} to skip), so the
     * widened projection can consume a stream carrying other events without touching its state for them.
     *
     * @param projection the feature projection to widen
     * @param eventType  the event type the projection understands
     */
    public static <S extends @Nullable Object, E, ID, SubE extends E> Projection<S, E, ID> adapt(Projection<S, SubE, ID> projection, Class<SubE> eventType) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(eventType, "eventType cannot be null");
        View<S, SubE> subView = projection.view();
        View<S, E> widenedView = View.create(subView.initialState(), (state, event) ->
                eventType.isInstance(event) ? subView.evolve(state, eventType.cast(event)) : state);
        @Nullable Function<SubE, @Nullable ID> subId = projection.id();
        Function<E, @Nullable ID> widenedId = subId == null ? null
                : event -> eventType.isInstance(event) ? subId.apply(eventType.cast(event)) : null;
        Set<Class<? extends E>> widenedTypes = new LinkedHashSet<>(projection.eventTypes());
        return new Projection<>(widenedView, widenedId, widenedTypes, projection.filter());
    }

    /**
     * A type-safe builder that assembles a {@link View} from per-event-type handlers and records which event types were
     * registered. Not thread-safe; build one, configure it, and call {@link #build()} once.
     */
    public static final class Builder<S extends @Nullable Object, E, ID> {
        private final S initialState;
        private final Map<Class<?>, BiFunction<S, E, S>> handlers = new LinkedHashMap<>();
        private final Set<Class<? extends E>> eventTypes = new LinkedHashSet<>();
        private @Nullable Function<E, @Nullable ID> id;
        private boolean singleton;
        private @Nullable Filter filter;

        private Builder(S initialState) {
            this.initialState = initialState;
        }

        /**
         * Sets the function deriving the view-instance id from an event. Return {@code null} for an event that maps to no
         * instance and should be skipped. Mutually exclusive with {@link #singleton()}. Exactly one of the two is
         * required, and it can be set only once.
         */
        public Builder<S, E, ID> id(Function<E, @Nullable ID> id) {
            if (this.id != null) {
                throw new IllegalStateException("id(...) has already been set and can only be set once");
            }
            if (this.singleton) {
                throw new IllegalStateException("id(...) cannot be combined with singleton()");
            }
            this.id = requireNonNull(id, "id cannot be null");
            return this;
        }

        /**
         * Marks the builder single-instance: it holds one view state rather than one per key, so no {@code id} function
         * is needed. Package-private on purpose. The public entry points fix the id type to {@code String}, so a
         * single-instance projection cannot be built with a non-{@code String} id type: {@link Projection#singletonBuilder(Object)}
         * in Java, and {@code singletonProjection}/{@code dcbSingletonProjection} in Kotlin. Mutually exclusive with
         * {@link #id(Function)}.
         */
        Builder<S, E, ID> singleton() {
            if (this.singleton) {
                throw new IllegalStateException("singleton() has already been set and can only be set once");
            }
            if (this.id != null) {
                throw new IllegalStateException("singleton() cannot be combined with id(...)");
            }
            this.singleton = true;
            return this;
        }

        /**
         * Registers the fold for a single event type. The handler runs when the evolved event is an instance of
         * {@code type}; a concrete event with no exact handler falls back to a handler registered for a superclass or
         * implemented interface (nearest superclass first, then interfaces). Registering the same {@code type} twice
         * replaces the earlier handler.
         * <p>
         * The registered types also become the projection's {@link Projection#eventTypes()}, which is what a runner
         * derives the subscription or query filter from when no explicit {@link #filter(Filter) filter} is set. Events
         * are stored under their concrete runtime type, so register the concrete event types here. A handler keyed on an
         * abstract supertype or an interface still folds every matching event through the runtime-type dispatch above,
         * but a type filter derived from that supertype key matches no stored event, so set an explicit
         * {@link #filter(Filter) filter} whenever you fold by a supertype.
         *
         * @param type    the event type this handler folds
         * @param handler the fold: current state and the event, returning the new state
         * @param <T>     the event subtype the handler accepts
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<S, E, ID> on(Class<T> type, BiFunction<S, ? super T, S> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            handlers.put(type, (BiFunction<S, E, S>) handler);
            eventTypes.add(type);
            return this;
        }

        /**
         * Sets an explicit selector that overrides the event-type-derived one. Use it to select on more than event type
         * (subject, source, data, time). A filter broader than the registered handlers is safe (the fold no-ops on
         * events it does not handle); a filter narrower than the handlers deliberately starves those handlers. Can be
         * set only once.
         */
        public Builder<S, E, ID> filter(Filter filter) {
            if (this.filter != null) {
                throw new IllegalStateException("filter(...) has already been set and can only be set once");
            }
            this.filter = requireNonNull(filter, "filter cannot be null");
            return this;
        }

        /**
         * Builds the {@code Projection}. Requires exactly one of {@link #id(Function) id} or {@link #singleton()}.
         */
        public Projection<S, E, ID> build() {
            if (id == null && !singleton) {
                throw new IllegalStateException("a projection needs exactly one of id(...) or singleton(), call one before build()");
            }
            View<S, E> view = View.create(initialState, new HandlerDispatch<>(handlers));
            return new Projection<>(view, id, new LinkedHashSet<>(eventTypes), filter);
        }
    }

    /**
     * The {@link View} fold produced by the handler builder: dispatch each event to the handler registered for its type,
     * falling back through superclasses and interfaces, and return the state unchanged when none is registered. Resolved
     * lookups are cached per concrete event class.
     */
    private static final class HandlerDispatch<S extends @Nullable Object, E> implements BiFunction<S, E, S> {
        private final Map<Class<?>, BiFunction<S, E, S>> handlers;
        private final Map<Class<?>, BiFunction<S, E, S>> resolved = new ConcurrentHashMap<>();
        private final BiFunction<S, E, S> noOp = (state, event) -> state;

        private HandlerDispatch(Map<Class<?>, BiFunction<S, E, S>> handlers) {
            this.handlers = new LinkedHashMap<>(handlers);
        }

        @Override
        public S apply(S state, E event) {
            BiFunction<S, E, S> handler = resolved.computeIfAbsent(event.getClass(), this::resolve);
            return handler.apply(state, event);
        }

        private BiFunction<S, E, S> resolve(Class<?> eventClass) {
            for (Class<?> c = eventClass; c != null; c = c.getSuperclass()) {
                BiFunction<S, E, S> handler = handlers.get(c);
                if (handler != null) {
                    return handler;
                }
            }
            Deque<Class<?>> queue = new ArrayDeque<>();
            for (Class<?> c = eventClass; c != null; c = c.getSuperclass()) {
                addAll(queue, c.getInterfaces());
            }
            Set<Class<?>> visited = new HashSet<>();
            while (!queue.isEmpty()) {
                Class<?> anInterface = queue.poll();
                if (!visited.add(anInterface)) {
                    continue;
                }
                BiFunction<S, E, S> handler = handlers.get(anInterface);
                if (handler != null) {
                    return handler;
                }
                addAll(queue, anInterface.getInterfaces());
            }
            return noOp;
        }
    }
}
