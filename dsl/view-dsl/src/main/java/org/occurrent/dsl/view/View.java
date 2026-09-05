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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A structure for representing and updating views based on state and an event
 *
 * @param <S> The type of the state that this view produces
 * @param <E> The type of the event that is used to create the state
 */
public interface View<S extends @Nullable Object, E> {
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
    S evolve(@Nullable S state, @NonNull E event);

    /**
     * Evolve state by applying the event together with its {@link EventMetadata} (stream id and version, global
     * position, DCB tags, and any CloudEvent extensions). Metadata is only available where an event arrives as a
     * CloudEvent (the live and catch-up runner paths); the on-demand query/replay paths ({@link #evolve(List)},
     * {@link #evolve(Stream)}, the varargs forms) have no CloudEvent and fold with {@link EventMetadata#empty()}.
     * <p>
     * The default ignores the metadata and delegates to {@link #evolve(Object, Object)}, so a plain event-only view
     * transparently sees a metadata-carrying feed. Build a metadata-aware view with {@link #create(Object, Fold)}.
     *
     * @param state    The current state
     * @param metadata The event's metadata
     * @param event    The event
     * @return The evolved state
     */
    default S evolve(@Nullable S state, EventMetadata metadata, @NonNull E event) {
        return evolve(state, event);
    }

    /**
     * A metadata-aware fold: current state, the event's {@link EventMetadata}, and the event, returning the new state.
     * The three-argument counterpart to a {@code BiFunction<S, E, S>}, used by {@link #create(Object, Fold)}.
     *
     * @param <S> The state type
     * @param <E> The event type
     */
    @FunctionalInterface
    interface Fold<S extends @Nullable Object, E> {
        // Deliberately unannotated (like the BiFunction in create(S, BiFunction)), so a Kotlin (S, EventMetadata, E) -> S
        // lambda binds with the same flexible nullability as the two-arg view builder rather than forcing S? on state.
        S evolve(S state, EventMetadata metadata, E event);
    }

    /**
     * Evolve initial state from events
     *
     * @return The evolved state
     */
    @SuppressWarnings("unchecked")
    default S evolve(@NonNull E event, @NonNull E event2, @NonNull E... moreEvents) {
        return evolve(initialState(), event, event2, moreEvents);
    }

    /**
     * Evolve from events
     *
     * @return The state
     */
    @SuppressWarnings("unchecked")
    default S evolve(S state, @NonNull E event, @NonNull E event2, @NonNull E... moreEvents) {
        List<E> events = new ArrayList<>(moreEvents.length + 2);
        events.add(event);
        events.add(event2);
        events.addAll(Arrays.asList(moreEvents));
        return evolve(state, events);
    }

    default S evolve(S state, @NonNull List<E> events) {
        S result = state;
        for (E event : events) {
            result = evolve(result, event);
        }
        return result;
    }

    /**
     * Evolve initial state from events
     *
     * @return The evolved state
     */
    default S evolve(@NonNull List<E> events) {
        return evolve(initialState(), events);
    }

    /**
     * Evolve initial state from a lazily-produced stream of events, for example a query result. The stream
     * is folded incrementally, one event at a time, without collecting it into a list first.
     *
     * @return The evolved state
     */
    default S evolve(@NonNull Stream<E> events) {
        return evolve(initialState(), events);
    }

    /**
     * Evolve from an explicit state and a lazily-produced stream of events. The stream is folded
     * incrementally, one event at a time, without collecting it into a list first.
     *
     * @return The evolved state
     */
    default S evolve(S state, @NonNull Stream<E> events) {
        S result = state;
        try (events) {
            // Fold sequentially in encounter order, since a view fold is order-dependent and a parallel forEach is not.
            Iterator<E> iterator = events.iterator();
            while (iterator.hasNext()) {
                result = evolve(result, iterator.next());
            }
        }
        return result;
    }

    static <S extends @Nullable Object, E> View<S, E> create(S initialState, @NonNull BiFunction<S, E, S> evolve) {
        return new View<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @Override
            public S evolve(@Nullable S state, @NonNull E event) {
                return evolve.apply(state, event);
            }
        };
    }

    /**
     * Creates a {@code View} whose fold begins from no state. The state stays {@code null} until {@code evolve}
     * replaces it. See {@link #create(Object, BiFunction)} for the fold.
     */
    static <S extends @Nullable Object, E> View<S, E> create(@NonNull BiFunction<S, E, S> evolve) {
        return create(null, evolve);
    }

    /**
     * Creates a metadata-aware view from a three-argument {@link Fold}. Its {@link #evolve(Object, EventMetadata, Object)}
     * applies the fold with the event's metadata, and its metadata-less {@link #evolve(Object, Object)} applies the fold
     * with {@link EventMetadata#empty()}, so the same fold drives both the CloudEvent-fed paths (with real metadata) and
     * the query/replay paths (with empty metadata).
     */
    static <S extends @Nullable Object, E> View<S, E> create(S initialState, @NonNull Fold<S, E> fold) {
        return new View<>() {
            @Override
            public S initialState() {
                return initialState;
            }

            @Override
            public S evolve(@Nullable S state, @NonNull E event) {
                return fold.evolve(state, EventMetadata.empty(), event);
            }

            @Override
            public S evolve(@Nullable S state, EventMetadata metadata, @NonNull E event) {
                return fold.evolve(state, metadata, event);
            }
        };
    }

    /**
     * Creates a metadata-aware {@code View} (see {@link #create(Object, Fold)}) whose fold begins from no state.
     * The state stays {@code null} until {@code fold} replaces it.
     */
    static <S extends @Nullable Object, E> View<S, E> create(@NonNull Fold<S, E> fold) {
        return create(null, fold);
    }
}
