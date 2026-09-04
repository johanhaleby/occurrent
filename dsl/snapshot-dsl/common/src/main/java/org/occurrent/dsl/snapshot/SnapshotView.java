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

package org.occurrent.dsl.snapshot;

import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.View;
import org.occurrent.filter.Filter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static java.util.Collections.addAll;
import static java.util.Objects.requireNonNull;

/**
 * A snapshottable, deciders-free fold: a {@link View} plus the {@code schemaVersion} that tags the state it produces and
 * the event types it folds (or an explicit {@code filter}). This is the read-side descriptor for snapshotting state that
 * is not driven by a {@code Decider}, the snapshot mirror of {@code org.occurrent.dsl.projection.Projection}.
 * <p>
 * Build one with the type-safe {@link #builder(Object) handler builder}, registering a fold per event type with
 * {@link Builder#on(Class, BiFunction)}. The fold leaves the state unchanged for any event type it does not handle, so
 * folding a broader stream onto it is safe.
 *
 * @param <S> the state type
 * @param <E> the event type
 */
public final class SnapshotView<S extends @Nullable Object, E> {

    private final View<S, E> view;
    private final int schemaVersion;
    private final Set<Class<? extends E>> eventTypes;
    private final @Nullable Filter filter;

    private SnapshotView(View<S, E> view, int schemaVersion, Set<Class<? extends E>> eventTypes, @Nullable Filter filter) {
        this.view = requireNonNull(view, "view cannot be null");
        if (schemaVersion < 0) {
            throw new IllegalArgumentException("schemaVersion cannot be negative");
        }
        this.schemaVersion = schemaVersion;
        this.eventTypes = Set.copyOf(requireNonNull(eventTypes, "eventTypes cannot be null"));
        this.filter = filter;
    }

    /** The pure fold: initial state and how an event evolves it. */
    public View<S, E> view() {
        return view;
    }

    /** The schema version tagging the state this fold produces, bumped when the state shape changes so older snapshots invalidate. */
    public int schemaVersion() {
        return schemaVersion;
    }

    /** The event types the fold handles, the default selector. Empty means "all types" (no type narrowing). */
    public Set<Class<? extends E>> eventTypes() {
        return eventTypes;
    }

    /** An optional explicit selector that overrides the type-derived one. {@code null} means "derive it from {@code eventTypes}". */
    public @Nullable Filter filter() {
        return filter;
    }

    /**
     * Starts building a {@code SnapshotView} whose fold begins from {@code initialState}. Register a handler per event
     * type with {@link Builder#on(Class, BiFunction)}, set the {@link Builder#schemaVersion(int) schemaVersion}, and
     * optionally an explicit {@link Builder#filter(Filter) filter}, then call {@link Builder#build()}.
     */
    public static <S extends @Nullable Object, E> Builder<S, E> builder(S initialState) {
        return new Builder<>(initialState);
    }

    /**
     * Starts building a {@code SnapshotView} whose fold begins from no state. The handler for the first event
     * received sees {@code null} rather than an initial value. Register a handler per event type with
     * {@link Builder#on(Class, BiFunction)}, set the {@link Builder#schemaVersion(int) schemaVersion}, and
     * optionally an explicit {@link Builder#filter(Filter) filter}, then call {@link Builder#build()}.
     */
    public static <S extends @Nullable Object, E> Builder<S, E> builder() {
        return builder(null);
    }

    /**
     * Widens a {@code SnapshotView} to a broader event type, mirroring {@code Decider#adapt}. The fold ignores events
     * that are not {@code eventType} (it leaves the state unchanged), so the widened view can fold a stream carrying
     * other events.
     *
     * @param snapshotView the view to widen
     * @param eventType    the event type the view understands
     */
    public static <S extends @Nullable Object, E, SubE extends E> SnapshotView<S, E> adapt(SnapshotView<S, SubE> snapshotView, Class<SubE> eventType) {
        requireNonNull(snapshotView, "snapshotView cannot be null");
        requireNonNull(eventType, "eventType cannot be null");
        View<S, SubE> subView = snapshotView.view();
        View<S, E> widenedView = View.create(subView.initialState(), (View.Fold<S, E>) (state, metadata, event) ->
                eventType.isInstance(event) ? subView.evolve(state, metadata, eventType.cast(event)) : state);
        return new SnapshotView<>(widenedView, snapshotView.schemaVersion(), new LinkedHashSet<>(snapshotView.eventTypes()), snapshotView.filter());
    }

    /**
     * A type-safe builder that assembles a {@link View} from per-event-type handlers. Not thread-safe. Build one,
     * configure it, and call {@link #build()} once. {@code schemaVersion} defaults to {@code 1} until set.
     */
    public static final class Builder<S extends @Nullable Object, E> {
        private final S initialState;
        private final Map<Class<?>, View.Fold<S, E>> handlers = new LinkedHashMap<>();
        private final Set<Class<? extends E>> eventTypes = new LinkedHashSet<>();
        private int schemaVersion = 1;
        private @Nullable Filter filter;

        private Builder(S initialState) {
            this.initialState = initialState;
        }

        /**
         * Registers the fold for one event type. A concrete event with no exact handler falls back to a handler on a
         * superclass or interface (nearest superclass first). Registering the same {@code type} twice replaces the
         * earlier handler. The registered types also become {@link SnapshotView#eventTypes()}.
         *
         * @param type    the event type this handler folds
         * @param handler the fold: current state and the event, returning the new state
         * @param <T>     the event subtype the handler accepts
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<S, E> on(Class<T> type, BiFunction<S, ? super T, S> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            BiFunction<S, ? super T, S> h = handler;
            // The dispatch only invokes this fold for events of type T, so the cast is safe.
            handlers.put(type, (state, metadata, event) -> h.apply(state, (T) event));
            eventTypes.add(type);
            return this;
        }

        /**
         * Registers a metadata-aware fold for one event type: the fold sees the event's {@link EventMetadata} as well as
         * the event. The metadata-less counterpart to {@link #on(Class, BiFunction)}, the same type-resolution and
         * replacement rules apply, and the registered type joins {@link SnapshotView#eventTypes()}. Snapshot rebuilds
         * that fold from a query/replay see {@link EventMetadata#empty()}.
         *
         * @param type    the event type this handler folds
         * @param handler the fold: current state, the event's metadata, and the event, returning the new state
         * @param <T>     the event subtype the handler accepts
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<S, E> on(Class<T> type, View.Fold<S, ? super T> handler) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(handler, "handler cannot be null");
            handlers.put(type, (View.Fold<S, E>) handler);
            eventTypes.add(type);
            return this;
        }

        /**
         * Sets the schema version tagging the state this fold produces. Bump it whenever the state shape changes so that
         * snapshots written by an older shape invalidate instead of being read into the new one.
         */
        public Builder<S, E> schemaVersion(int schemaVersion) {
            if (schemaVersion < 0) {
                throw new IllegalArgumentException("schemaVersion cannot be negative");
            }
            this.schemaVersion = schemaVersion;
            return this;
        }

        /**
         * Sets an explicit selector that overrides the event-type-derived one. A filter broader than the registered
         * handlers is safe (the fold no-ops on events it does not handle). Can be set only once.
         */
        public Builder<S, E> filter(Filter filter) {
            if (this.filter != null) {
                throw new IllegalStateException("filter(...) has already been set and can only be set once");
            }
            this.filter = requireNonNull(filter, "filter cannot be null");
            return this;
        }

        /**
         * Builds the {@code SnapshotView}.
         */
        public SnapshotView<S, E> build() {
            View<S, E> view = View.create(initialState, new HandlerDispatch<>(handlers));
            return new SnapshotView<>(view, schemaVersion, new LinkedHashSet<>(eventTypes), filter);
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
