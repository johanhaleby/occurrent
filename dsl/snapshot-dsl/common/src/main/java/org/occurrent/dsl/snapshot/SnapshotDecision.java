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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The outcome of one execute, handed to a {@link SnapshotPolicy} to decide whether to write a new snapshot.
 *
 * @param newState                the state after folding this execute's produced events onto the resumed state
 * @param newEvents               the events this execute produced (empty when the execute wrote nothing)
 * @param newVersion              the version the state is now folded up to (the stream version, or the DCB position)
 * @param previousSnapshotVersion the version of the snapshot this execute resumed from, or {@link #NO_PREVIOUS_SNAPSHOT}
 *                                when it started from the initial state
 * @param eventsSinceSnapshot     how many events have been folded since the last snapshot: the resumed tail length plus
 *                                the number of newly produced events. This is what {@link SnapshotPolicy#everyNEvents(int)} counts.
 * @param <S>                     the state type
 * @param <E>                     the event type
 */
public record SnapshotDecision<S extends @Nullable Object, E>(S newState, List<E> newEvents, long newVersion,
                                                              long previousSnapshotVersion, int eventsSinceSnapshot) {

    /** The value of {@link #previousSnapshotVersion()} when the execute did not resume from a snapshot. */
    public static final long NO_PREVIOUS_SNAPSHOT = -1L;

    public SnapshotDecision {
        requireNonNull(newEvents, "newEvents cannot be null");
        newEvents = List.copyOf(newEvents);
        if (newVersion < 0) {
            throw new IllegalArgumentException("newVersion cannot be negative");
        }
        if (eventsSinceSnapshot < 0) {
            throw new IllegalArgumentException("eventsSinceSnapshot cannot be negative");
        }
    }
}
