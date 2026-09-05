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

package org.occurrent.dsl.view

import org.occurrent.cloudevents.EventMetadata

fun <S, E : Any> view(initialState: S, updateState: (S, E) -> S): View<S, E> = View.create(initialState, updateState)

/**
 * Builds a [View] whose fold begins from no state. The block and the returned `View` see `S?` rather than `S`,
 * since the fold starts from `null` until `updateState` replaces it. See [view] for the fold.
 */
fun <S, E : Any> view(updateState: (S?, E) -> S?): View<S?, E> = view(null, updateState)

/**
 * Builds a metadata-aware [View]: the fold sees the event's [EventMetadata] (stream id and version, global position,
 * DCB tags, CloudEvent extensions) as well as the event. Metadata is only available on the CloudEvent-fed paths; the
 * query/replay `evolve` overloads fold with [EventMetadata.empty].
 */
fun <S, E : Any> view(initialState: S, updateState: (S, EventMetadata, E) -> S): View<S, E> =
    View.create(initialState, View.Fold { state, metadata, event -> updateState(state, metadata, event) })

/**
 * Builds a metadata-aware [View] whose fold begins from no state. The block and the returned `View` see `S?`
 * rather than `S`, since the fold starts from `null` until `updateState` replaces it. See [view] for what
 * metadata-aware means.
 */
fun <S, E : Any> view(updateState: (S?, EventMetadata, E) -> S?): View<S?, E> = view(null, updateState)

// =========================
// Single event (convenience)
// =========================

// Non-null S  -> returns S
@JvmName("evolveEventNonNull")
fun <S : Any, E : Any> View<S, E>.evolveEvent(event: E): S =
    evolve(initialState(), event)

// Nullable S  -> returns S?
@JvmName("evolveEventNullable")
fun <S : Any, E : Any> View<S?, E>.evolveEvent(event: E): S? =
    evolve(initialState(), event)


// ==============================================
// Two-or-more events (varargs entry-point forms)
// ==============================================

// Start from initial (non-null S) -> S
@JvmName("evolveAllVarargsNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(event1: E, event2: E, vararg more: E): S =
    evolve(initialState(), event1, event2, *more)

// Start from initial (nullable S) -> S?
@JvmName("evolveAllVarargsNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(event1: E, event2: E, vararg more: E): S? =
    evolve(null, event1, event2, *more)

// Start from explicit state (non-null S) -> S
@JvmName("evolveFromVarargsNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, event1: E, event2: E, vararg more: E): S =
    evolve(state, event1, event2, *more)

// Start from explicit state (nullable S) -> S?
@JvmName("evolveFromVarargsNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, event1: E, event2: E, vararg more: E): S? =
    evolve(state, event1, event2, *more)


// ======================
// List-based entry-points
// ======================

// Start from initial (non-null S) -> S
@JvmName("evolveAllListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: List<E>): S =
    evolve(initialState(), events)

// Start from initial (nullable S) -> S?
@JvmName("evolveAllListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: List<E>): S? =
    evolve(events)

// Start from explicit state (non-null S) -> S
@JvmName("evolveFromListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: List<E>): S =
    evolve(state, events)

// Start from explicit state (nullable S) -> S?
@JvmName("evolveFromListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: List<E>): S? =
    evolve(state, events)


// ===================================
// Sequence and Iterable entry-points
// ===================================
// Folding events into a view is a read-side operation, and a lazily-queried Sequence (from the query
// DSL) or any Iterable is a natural source. These fold the source directly rather than materializing it
// into a List first, so a large or lazy Sequence stays lazy.

/**
 * Fold a lazily-produced [Sequence] of events, for example a `queryForSequence` result, into the view.
 * The sequence is consumed once and folded directly without being copied into a `List`.
 */
@JvmName("evolveAllSequenceNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Sequence<E>): S =
    events.fold(initialState()) { state, event -> evolve(state, event) }

@JvmName("evolveAllSequenceNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Sequence<E>): S? =
    events.fold(initialState()) { state, event -> evolve(state, event) }

/**
 * Fold any [Iterable] of events into the view, folding directly without an intermediate `List`.
 */
@JvmName("evolveAllIterableNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Iterable<E>): S =
    events.fold(initialState()) { state, event -> evolve(state, event) }

@JvmName("evolveAllIterableNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Iterable<E>): S? =
    events.fold(initialState()) { state, event -> evolve(state, event) }

/**
 * Fold a lazily-produced [Sequence] of events into the view from an explicit [state], consuming the
 * sequence once and folding directly.
 */
@JvmName("evolveFromSequenceNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Sequence<E>): S =
    events.fold(state) { acc, event -> evolve(acc, event) }

@JvmName("evolveFromSequenceNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Sequence<E>): S? =
    events.fold(state) { acc, event -> evolve(acc, event) }

/**
 * Fold any [Iterable] of events into the view from an explicit [state], folding directly without an
 * intermediate `List`.
 */
@JvmName("evolveFromIterableNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Iterable<E>): S =
    events.fold(state) { acc, event -> evolve(acc, event) }

@JvmName("evolveFromIterableNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Iterable<E>): S? =
    events.fold(state) { acc, event -> evolve(acc, event) }
