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

import java.util.stream.Stream

fun <S, E> view(initialState: S, updateState: (S, E) -> S) = View.create(initialState, updateState)

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

// ======================
// Stream-based entry-points
// ======================

// Start from initial (non-null S) -> S
@JvmName("evolveAllListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Stream<E>): S =
    evolve(initialState(), events)

// Start from initial (nullable S) -> S?
@JvmName("evolveAllListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Stream<E>): S? =
    evolve(events)

// Start from explicit state (non-null S) -> S
@JvmName("evolveFromListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Stream<E>): S =
    evolve(state, events)

// Start from explicit state (nullable S) -> S?
@JvmName("evolveFromListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Stream<E>): S? =
    evolve(state, events)

// ======================
// Sequence-based entry-points
// ======================

// Start from initial (non-null S) -> S
@JvmName("evolveAllListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveAll(events: Sequence<E>): S = evolveFrom(initialState(), events)

// Start from initial (nullable S) -> S?
@JvmName("evolveAllListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveAll(events: Sequence<E>): S? = events.fold(initialState(), this::evolve)

// Start from explicit state (non-null S) -> S
@JvmName("evolveFromListNonNull")
fun <S : Any, E : Any> View<S, E>.evolveFrom(state: S, events: Sequence<E>): S = events.fold(state, this::evolve)

// Start from explicit state (nullable S) -> S?
@JvmName("evolveFromListNullable")
fun <S : Any, E : Any> View<S?, E>.evolveFrom(state: S?, events: Sequence<E>): S? = events.fold(state, this::evolve)

// Non-null state
fun <S : Any, E : Any> View<S, E>.evolve(state: S, events: Sequence<E>): S = events.fold(state, this::evolve)

// Nullable state (but works when S is nullable)
@JvmName("evolveNullable")
fun <S : Any, E : Any> View<S?, E>.evolve(state: S?, events: Sequence<E>): S? = events.fold(state, this::evolve)