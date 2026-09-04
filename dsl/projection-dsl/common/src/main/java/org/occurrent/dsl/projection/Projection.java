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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.View;
import org.occurrent.filter.Filter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.addAll;
import static java.util.Objects.requireNonNull;

/**
 * A self-describing, capability-agnostic read model: a {@link View} fold plus which view instance an event updates (the
 * {@code id}) and which events feed it (the handled {@code eventTypes}, or an explicit {@code filter}). It lets a feature
 * describe its read model next to its fold, and is the read-side mirror of {@code org.occurrent.dsl.dcb.DcbDecider}.
 * <p>
 * Build one with the type-safe {@link #builder(Object) handler builder}, registering a fold per event type with
 * {@link Builder#on(Class, BiFunction)}. The handled types become the subscription filter, and the fold leaves the state
 * unchanged for any event type it does not handle, so feeding it a broader stream is safe as long as every event the
 * filter admits is still one the {@code CloudEventConverter} can turn into an {@code E}. One it cannot fails that
 * delivery instead of being ignored, and a subscription that keeps redelivering a failing event holds up everything
 * queued behind it. See {@link Builder#filter(Filter) filter} for how this bears on an explicit filter.
 * <p>
 * One descriptor runs two ways. Feed it a subscription to keep a stored read model up to date, with a
 * {@code ProjectionRunner} or the Kotlin {@code project} extensions, or fold it over a query for a strongly consistent
 * read on demand, with {@code Projections.project(projection, queries)} in the blocking and reactor projection DSL
 * modules. Running one descriptor both ways and checking the two answers agree is the strongest test available for a
 * projection.
 *
 * @param <S> the state type
 * @param <E> the event type
 * @param <ID> the view-instance id type
 */
public final class Projection<S extends @Nullable Object, E, ID> {

    private final View<S, E> view;
    private final @Nullable BiFunction<EventMetadata, E, @Nullable ID> id;
    private final boolean metadataKeyed;
    private final Set<Class<? extends E>> eventTypes;
    private final @Nullable Filter filter;

    // Private on purpose: the only way to build a Projection is the builder/singletonBuilder/adapt factories, which fix
    // a single-instance projection's id type to String, so a non-String singleton cannot be constructed.
    private Projection(View<S, E> view, @Nullable BiFunction<EventMetadata, E, @Nullable ID> id, boolean metadataKeyed, Set<Class<? extends E>> eventTypes, @Nullable Filter filter) {
        this.view = requireNonNull(view, "view cannot be null");
        this.id = id;
        this.metadataKeyed = metadataKeyed;
        this.eventTypes = Set.copyOf(requireNonNull(eventTypes, "eventTypes cannot be null"));
        this.filter = filter;
    }

    /** The pure fold: initial state and how an event evolves it. */
    public View<S, E> view() {
        return view;
    }

    /**
     * The function deriving which view instance an event updates, or {@code null} for a single-instance projection. A
     * single-instance projection folds every event into one view regardless of subject, like a leaderboard built from
     * all players' events, so it has no per-event key and the framework supplies the single key. A keyed projection has
     * one instance per key, like a player profile keyed by player id, and its function may return {@code null} to skip
     * an event that maps to no instance.
     * <p>
     * This is the event-only view of the id function. A projection keyed on {@link EventMetadata} (for example the
     * stream id) via {@link Builder#id(BiFunction)} still exposes an {@code id()} here, but it applies the underlying
     * function with {@link EventMetadata#empty()}, so it is only meaningful for an event-only-keyed projection. Use
     * {@link #idWithMetadata()} to key with metadata.
     */
    public @Nullable Function<E, @Nullable ID> id() {
        BiFunction<EventMetadata, E, @Nullable ID> metadataId = this.id;
        if (metadataId == null) {
            return null;
        }
        return event -> {
            try {
                return metadataId.apply(EventMetadata.empty(), event);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Could not resolve the view-instance id from the event alone. If this projection is keyed by event metadata (id(BiFunction)), it cannot be keyed on a metadata-less path such as the on-demand query fold. Use idWithMetadata() on a metadata-carrying path (a subscription runner).", e);
            }
        };
    }

    /**
     * The function deriving which view instance an event updates, seeing the event's {@link EventMetadata} as well as the
     * event, or {@code null} for a single-instance projection. This is the metadata-aware form of {@link #id()}: a
     * runner keys with it so a projection can be keyed by metadata such as the stream id. On a metadata-less path (the
     * on-demand query/replay), the metadata is {@link EventMetadata#empty()}.
     */
    public @Nullable BiFunction<EventMetadata, E, @Nullable ID> idWithMetadata() {
        return id;
    }

    /**
     * Whether the id function was declared through {@link Builder#id(BiFunction)} rather than
     * {@link Builder#id(Function)}, meaning it was given the chance to key on {@link EventMetadata}.
     * <p>
     * This says how the key was <em>declared</em>, not whether it actually reads the metadata: a caller writing
     * {@code id((metadata, event) -> event.orderId())} is reported as metadata-keyed even though it ignores the
     * metadata. That is why this must not be used to reject a projection up front. It is safe as one input to a
     * failure that has already happened: a runner that folded with {@link EventMetadata#empty()} and got a
     * {@code null} id can use this to tell "the key needed metadata it never received" apart from "this event maps
     * to no instance", which is a legitimate skip. A projection that ignores the metadata still returns a real id,
     * so it never reaches that branch.
     */
    public boolean metadataKeyed() {
        return metadataKeyed;
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
     * Starts building a {@code Projection} whose fold begins from no state. The handler for the first event received
     * sees {@code null} rather than an initial value. Register a handler per event type with
     * {@link Builder#on(Class, BiFunction)}, an {@link Builder#id(Function) id} function, and optionally an explicit
     * {@link Builder#filter(Filter) filter}, then call {@link Builder#build()}.
     */
    public static <S extends @Nullable Object, E, ID> Builder<S, E, ID> builder() {
        return builder(null);
    }

    /**
     * Starts building a single-instance {@code Projection}: one view folded from every event, like a leaderboard, rather
     * than one instance per key, so it needs no {@code id}. The single slot is keyed at run time by the projection's own
     * identity (the subscription id, or the {@code @Projection} id), a {@code String}, so a single-instance projection is
     * always a {@code Projection<S, E, String>}. Use {@link #builder(Object)} with {@link Builder#id(Function)} for a
     * keyed, multi-instance projection. Kotlin: {@code singletonProjection}/{@code dcbSingletonProjection}.
     */
    public static <S extends @Nullable Object, E> Builder<S, E, String> singletonBuilder(S initialState) {
        Builder<S, E, String> builder = new Builder<>(initialState);
        builder.singleton();
        return builder;
    }

    /**
     * Starts building a single-instance {@code Projection} whose fold begins from no state. The handler for the first
     * event received sees {@code null} rather than an initial value. See {@link #singletonBuilder(Object)} for what
     * single-instance means.
     */
    public static <S extends @Nullable Object, E> Builder<S, E, String> singletonBuilder() {
        return singletonBuilder(null);
    }

    /**
     * Widens a {@code Projection} to a broader event type, mirroring {@code Decider#adapt}. The fold and id ignore events
     * that are not {@code eventType} (the fold leaves the state unchanged, the id returns {@code null}), so the widened
     * projection can consume a stream carrying other events.
     *
     * @param projection the feature projection to widen
     * @param eventType  the event type the projection understands
     */
    public static <S extends @Nullable Object, E, ID, SubE extends E> Projection<S, E, ID> adapt(Projection<S, SubE, ID> projection, Class<SubE> eventType) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(eventType, "eventType cannot be null");
        View<S, SubE> subView = projection.view();
        View<S, E> widenedView = View.create(subView.initialState(), (View.Fold<S, E>) (state, metadata, event) ->
                eventType.isInstance(event) ? subView.evolve(state, metadata, eventType.cast(event)) : state);
        @Nullable BiFunction<EventMetadata, SubE, @Nullable ID> subId = projection.idWithMetadata();
        BiFunction<EventMetadata, E, @Nullable ID> widenedId = subId == null ? null
                : (metadata, event) -> eventType.isInstance(event) ? subId.apply(metadata, eventType.cast(event)) : null;
        Set<Class<? extends E>> widenedTypes = new LinkedHashSet<>(projection.eventTypes());
        // Carry metadataKeyed across: widening rebuilds the id from idWithMetadata(), which loses how it was declared.
        return new Projection<>(widenedView, widenedId, projection.metadataKeyed(), widenedTypes, projection.filter());
    }

    /**
     * A type-safe builder that assembles a {@link View} from per-event-type handlers and records which event types were
     * registered. Not thread-safe. Build one, configure it, and call {@link #build()} once.
     */
    public static final class Builder<S extends @Nullable Object, E, ID> {
        private final S initialState;
        private final Map<Class<?>, View.Fold<S, E>> handlers = new LinkedHashMap<>();
        private final Set<Class<? extends E>> eventTypes = new LinkedHashSet<>();
        private @Nullable BiFunction<EventMetadata, E, @Nullable ID> id;
        private boolean metadataKeyed;
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
            requireNonNull(id, "id cannot be null");
            // Deliberately does not route through id(BiFunction): this key provably cannot read metadata, and
            // metadataKeyed must stay false so a metadata-less path is not blamed for a null id it did not cause.
            return setId((metadata, event) -> id.apply(event), false);
        }

        /**
         * Sets the function deriving the view-instance id from the event's {@link EventMetadata} and the event, so a
         * projection can be keyed by metadata such as the stream id ({@code (metadata, event) -> metadata.getStreamId()}).
         * Return {@code null} for an event that maps to no instance and should be skipped. Mutually exclusive with
         * {@link #singleton()}. Exactly one of the two is required, and it can be set only once. The metadata-less
         * on-demand query/replay path folds with {@link EventMetadata#empty()}, so a metadata-keyed projection cannot be
         * read that way.
         */
        public Builder<S, E, ID> id(BiFunction<EventMetadata, E, @Nullable ID> id) {
            return setId(id, true);
        }

        private Builder<S, E, ID> setId(@Nullable BiFunction<EventMetadata, E, @Nullable ID> id, boolean metadataKeyed) {
            if (this.id != null) {
                throw new IllegalStateException("id(...) has already been set and can only be set once");
            }
            if (this.singleton) {
                throw new IllegalStateException("id(...) cannot be combined with singleton()");
            }
            this.id = requireNonNull(id, "id cannot be null");
            this.metadataKeyed = metadataKeyed;
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
         * Registers the fold for one event type. A concrete event with no exact handler falls back to a handler on a
         * superclass or interface (nearest superclass first). Registering the same {@code type} twice replaces the
         * earlier handler.
         * <p>
         * The registered types also become {@link Projection#eventTypes()}, the selector a runner uses when no explicit
         * {@link #filter(Filter) filter} is set. A handler registered on a sealed supertype asks for every concrete type
         * it permits, the same expansion the saga DSL and the subscription annotations apply, so folding by a supertype
         * needs no explicit filter. A registered type whose concrete types cannot all be found is refused when the
         * filter is derived, naming the type and the remedy.
         *
         * @param type    the event type this handler folds
         * @param handler the fold: current state and the event, returning the new state
         * @param <T>     the event subtype the handler accepts
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<S, E, ID> on(Class<T> type, BiFunction<S, ? super T, S> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            BiFunction<S, ? super T, S> h = handler;
            // The dispatch only invokes this fold for events of type T, so the cast is safe.
            handlers.put(type, (state, metadata, event) -> h.apply(state, (T) event));
            eventTypes.add(type);
            return this;
        }

        /**
         * Registers a metadata-aware fold for one event type: the fold sees the event's {@link EventMetadata} (stream id
         * and version, global position, DCB tags, CloudEvent extensions) as well as the event. The metadata-less
         * counterpart to {@link #on(Class, BiFunction)}, the same type-resolution and replacement rules apply, and the
         * registered type joins {@link Projection#eventTypes()}. On the metadata-less query/replay path the fold sees
         * {@link EventMetadata#empty()}.
         *
         * @param type    the event type this handler folds
         * @param handler the fold: current state, the event's metadata, and the event, returning the new state
         * @param <T>     the event subtype the handler accepts
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<S, E, ID> on(Class<T> type, View.Fold<S, ? super T> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            handlers.put(type, (View.Fold<S, E>) handler);
            eventTypes.add(type);
            return this;
        }

        /**
         * Sets an explicit selector that overrides the event-type-derived one. Use it to select on more than event type
         * (subject, source, data, time). A filter broader than the registered handlers is safe for the fold, which
         * no-ops on events it does not handle, but every CloudEvent the filter admits is converted to a domain event
         * before the fold sees it. One the {@code CloudEventConverter} cannot turn into an {@code E} fails that
         * delivery instead of being ignored, and a subscription that keeps redelivering a failing event holds up
         * everything queued behind it, so keep the filter inside what the converter can convert. A filter narrower
         * than the handlers deliberately starves those handlers. Can be set only once.
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
            return new Projection<>(view, id, metadataKeyed, new LinkedHashSet<>(eventTypes), filter);
        }
    }

    /**
     * The {@link View} fold produced by the handler builder: dispatch each event to the handler registered for its type,
     * falling back through superclasses and interfaces, and return the state unchanged when none is registered. Resolved
     * lookups are cached per concrete event class.
     */
    private static final class HandlerDispatch<S extends @Nullable Object, E> implements View.Fold<S, E> {
        private final Map<Class<?>, View.Fold<S, E>> handlers;
        private final Map<Class<?>, View.Fold<S, E>> resolved = new ConcurrentHashMap<>();
        private final View.Fold<S, E> noOp = (state, metadata, event) -> state;

        private HandlerDispatch(Map<Class<?>, View.Fold<S, E>> handlers) {
            this.handlers = new LinkedHashMap<>(handlers);
        }

        @Override
        public S evolve(S state, EventMetadata metadata, E event) {
            View.Fold<S, E> handler = resolved.computeIfAbsent(event.getClass(), this::resolve);
            return handler.evolve(state, metadata, event);
        }

        private View.Fold<S, E> resolve(Class<?> eventClass) {
            for (Class<?> c = eventClass; c != null; c = c.getSuperclass()) {
                View.Fold<S, E> handler = handlers.get(c);
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
                View.Fold<S, E> handler = handlers.get(anInterface);
                if (handler != null) {
                    return handler;
                }
                addAll(queue, anInterface.getInterfaces());
            }
            return noOp;
        }
    }
}
