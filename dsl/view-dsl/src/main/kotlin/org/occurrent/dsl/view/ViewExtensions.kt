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

fun <S, E : Any> view(initialState: S, updateState: (S, E) -> S): View<S, E> = View.create(initialState, updateState)

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
// DSL) or any Iterable is a natural source, so these delegate to the List forms.

@JvmName("evolveAllSequenceNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Sequence<E>): S =
    evolveAll(events.toList())

@JvmName("evolveAllSequenceNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Sequence<E>): S? =
    evolveAll(events.toList())

@JvmName("evolveAllIterableNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Iterable<E>): S =
    evolveAll(events.toList())

@JvmName("evolveAllIterableNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Iterable<E>): S? =
    evolveAll(events.toList())

@JvmName("evolveFromSequenceNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Sequence<E>): S =
    evolveFrom(state, events.toList())

@JvmName("evolveFromSequenceNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Sequence<E>): S? =
    evolveFrom(state, events.toList())

@JvmName("evolveFromIterableNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Iterable<E>): S =
    evolveFrom(state, events.toList())

@JvmName("evolveFromIterableNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Iterable<E>): S? =
    evolveFrom(state, events.toList())
