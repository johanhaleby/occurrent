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

package org.occurrent.dsl.decider

import org.occurrent.dsl.decider.Decider.Decision

/**
 * A utility function for creating deciders a bit more nicer in Kotlin
 */
fun <C, S, E> decider(initialState: S, decide: (C, S) -> List<E>, evolve: (S, E) -> S, isTerminal: (S) -> Boolean = { false }): Decider<C, S, E> = Decider.create(initialState, decide, evolve, isTerminal)

/**
 * Adapt a decider over subtype commands [SubC] and events [SubE] into one over the supertypes [C] and [E], so it can run
 * against a service or be composed with deciders over those supertypes. Commands that are not [SubC] produce no events and
 * events that are not [SubE] leave the state unchanged. Delegates to [Decider.adapt], inferring [SubC] and [SubE] from the
 * receiver and [C] and [E] from the call site (typically the expected type at an `execute` call).
 */
inline fun <reified SubC : C, S, reified SubE : E, C, E : Any> Decider<SubC, S, SubE>.adapt(): Decider<C, S, E> =
    Decider.adapt(this, SubC::class.java, SubE::class.java)

/**
 * Compose two deciders that share command type [C] and event type [E] (typically reached with [adapt]) into one whose
 * state is the [Pair] of their states. Each command is offered to both deciders, only the one that recognizes it emits
 * events, and each state evolves independently, skipping a slice once it is terminal.
 */
fun <C : Any, S1, S2, E : Any> compose(first: Decider<C, S1, E>, second: Decider<C, S2, E>): Decider<C, Pair<S1, S2>, E> =
    decider<C, Pair<S1, S2>, E>(
        initialState = first.initialState() to second.initialState(),
        decide = { command, (s1, s2) -> first.decide(command, s1) + second.decide(command, s2) },
        evolve = { (s1, s2), event ->
            val next1 = if (first.isTerminal(s1)) s1 else first.evolve(s1, event)
            val next2 = if (second.isTerminal(s2)) s2 else second.evolve(s2, event)
            next1 to next2
        },
        isTerminal = { (s1, s2) -> first.isTerminal(s1) && second.isTerminal(s2) }
    )

/**
 * Compose three deciders that share command type [C] and event type [E] (typically reached with [adapt]) into one whose
 * state is the [Triple] of their states. Behaves like the two decider [compose], extended to a third slice.
 */
fun <C : Any, S1, S2, S3, E : Any> compose(first: Decider<C, S1, E>, second: Decider<C, S2, E>, third: Decider<C, S3, E>): Decider<C, Triple<S1, S2, S3>, E> =
    decider<C, Triple<S1, S2, S3>, E>(
        initialState = Triple(first.initialState(), second.initialState(), third.initialState()),
        decide = { command, (s1, s2, s3) -> first.decide(command, s1) + second.decide(command, s2) + third.decide(command, s3) },
        evolve = { (s1, s2, s3), event ->
            Triple(
                if (first.isTerminal(s1)) s1 else first.evolve(s1, event),
                if (second.isTerminal(s2)) s2 else second.evolve(s2, event),
                if (third.isTerminal(s3)) s3 else third.evolve(s3, event)
            )
        },
        isTerminal = { (s1, s2, s3) -> first.isTerminal(s1) && second.isTerminal(s2) && third.isTerminal(s3) }
    )

/**
 * Compose any number of deciders that share command type [C] and event type [E] (typically reached with [adapt]) into one
 * whose state is a [CompositeState] holding each decider's state positionally. Read a slice with [CompositeState.slice].
 * For two or three deciders prefer the [compose] overloads that return a typed [Pair] or [Triple]. Delegates to
 * [Decider.compose].
 */
fun <C, E : Any> compose(vararg deciders: Decider<C, *, E>): Decider<C, CompositeState, E> =
    Decider.compose(deciders.toList())

fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, command: C, vararg moreCommands: C): Decision<S, E> = decideOnEvents(events, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, commands: List<C>): Decision<S, E> = decideOnEvents(events, commands)
fun <C, S, E> Decider<C, S, E>.decide(state: S, command: C, vararg moreCommands: C): Decision<S, E> = decideOnState(state, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(state: S, commands: List<C>): Decision<S, E> = decideOnState(state, commands)

operator fun <S, E> Decision<S, E>.component1(): S = state
operator fun <S, E> Decision<S, E>.component2(): List<E> = events