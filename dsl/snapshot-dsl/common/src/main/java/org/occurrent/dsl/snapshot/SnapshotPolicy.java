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

import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Decides whether an execute should write a new snapshot, given its {@link SnapshotDecision}. This is the single trigger
 * abstraction for both technical snapshots ({@link #everyNEvents(int)}) and domain-driven ones ({@link #onEvent(Class)},
 * {@link #whenState(Predicate)}, and the decider-backed {@code SnapshotPolicies.whenTerminal(...)} in the blocking and
 * reactor executor modules, kept out of this module so a {@code SnapshotView}-only consumer doesn't need a
 * {@code Decider} on its classpath).
 *
 * @param <S> the state type
 * @param <E> the event type
 */
@FunctionalInterface
public interface SnapshotPolicy<S extends @Nullable Object, E> {

    /**
     * @return {@code true} if a snapshot should be written for this execute
     */
    boolean shouldSnapshot(SnapshotDecision<S, E> decision);

    /**
     * Snapshots once at least {@code n} events have been folded since the last snapshot. This is the technical
     * "every N events" trigger, driven by {@link SnapshotDecision#eventsSinceSnapshot()}.
     *
     * @param n the event threshold, must be positive
     */
    static <S extends @Nullable Object, E> SnapshotPolicy<S, E> everyNEvents(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }
        return decision -> decision.eventsSinceSnapshot() >= n;
    }

    /**
     * Never snapshots. The default when snapshot writing is not wanted.
     */
    static <S extends @Nullable Object, E> SnapshotPolicy<S, E> never() {
        return decision -> false;
    }

    /**
     * Snapshots after every execute that produced at least one event.
     */
    static <S extends @Nullable Object, E> SnapshotPolicy<S, E> always() {
        return decision -> !decision.newEvents().isEmpty();
    }

    /**
     * Snapshots when this execute produced an event of the given type (or a subtype), for example a period-boundary
     * event such as {@code BooksClosed}. This is the "closing the books" trigger when the boundary is a domain event.
     *
     * @param type the event type that triggers a snapshot
     */
    static <S extends @Nullable Object, E> SnapshotPolicy<S, E> onEvent(Class<? extends E> type) {
        requireNonNull(type, "type cannot be null");
        return decision -> decision.newEvents().stream().anyMatch(type::isInstance);
    }

    /**
     * Snapshots when the new state satisfies the predicate, for example {@code state -> state.isPeriodClosed()}. This is
     * the "closing the books" trigger when the boundary is a state condition rather than a specific event.
     *
     * @param statePredicate tested against {@link SnapshotDecision#newState()}
     */
    static <S extends @Nullable Object, E> SnapshotPolicy<S, E> whenState(Predicate<? super S> statePredicate) {
        requireNonNull(statePredicate, "statePredicate cannot be null");
        return decision -> statePredicate.test(decision.newState());
    }

    /**
     * Combines two policies, snapshotting when either fires.
     */
    default SnapshotPolicy<S, E> or(SnapshotPolicy<S, E> other) {
        requireNonNull(other, "other cannot be null");
        return decision -> shouldSnapshot(decision) || other.shouldSnapshot(decision);
    }
}
