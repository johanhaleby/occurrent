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
 * Compose two feature deciders into one whose state is the [Pair] of their states. Each decider is [adapt]ed to the shared
 * command type [C] and event type [E] automatically, so you can pass deciders over their own narrow command and event
 * subtypes directly. Each command is offered to both deciders, only the one that recognizes it emits events, and each
 * state evolves independently, skipping a slice once it is terminal.
 *
 * [C] and [E] are inferred from the expected type (annotate the result, for example `Decider<DomainCommand, ..., DomainEvent>`).
 */
inline fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, E : Any> compose(
    first: Decider<C1, S1, E1>,
    second: Decider<C2, S2, E2>
): Decider<C, Pair<S1, S2>, E> {
    val a: Decider<C, S1, E> = first.adapt()
    val b: Decider<C, S2, E> = second.adapt()
    return decider(
        initialState = a.initialState() to b.initialState(),
        decide = { command, (s1, s2) -> a.decide(command, s1) + b.decide(command, s2) },
        evolve = { (s1, s2), event ->
            val next1 = if (a.isTerminal(s1)) s1 else a.evolve(s1, event)
            val next2 = if (b.isTerminal(s2)) s2 else b.evolve(s2, event)
            next1 to next2
        },
        isTerminal = { (s1, s2) -> a.isTerminal(s1) && b.isTerminal(s2) }
    )
}

/**
 * Compose three feature deciders into one whose state is the [Triple] of their states. Like the two decider [compose], each
 * decider is [adapt]ed to the shared [C] and [E] automatically, so you can pass narrow feature deciders directly.
 *
 * [C] and [E] are inferred from the expected type (annotate the result, for example `Decider<DomainCommand, ..., DomainEvent>`).
 */
inline fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, reified C3 : C, S3, reified E3 : E, E : Any> compose(
    first: Decider<C1, S1, E1>,
    second: Decider<C2, S2, E2>,
    third: Decider<C3, S3, E3>
): Decider<C, Triple<S1, S2, S3>, E> {
    val a: Decider<C, S1, E> = first.adapt()
    val b: Decider<C, S2, E> = second.adapt()
    val c: Decider<C, S3, E> = third.adapt()
    return decider(
        initialState = Triple(a.initialState(), b.initialState(), c.initialState()),
        decide = { command, (s1, s2, s3) -> a.decide(command, s1) + b.decide(command, s2) + c.decide(command, s3) },
        evolve = { (s1, s2, s3), event ->
            Triple(
                if (a.isTerminal(s1)) s1 else a.evolve(s1, event),
                if (b.isTerminal(s2)) s2 else b.evolve(s2, event),
                if (c.isTerminal(s3)) s3 else c.evolve(s3, event)
            )
        },
        isTerminal = { (s1, s2, s3) -> a.isTerminal(s1) && b.isTerminal(s2) && c.isTerminal(s3) }
    )
}

/**
 * Compose a list of deciders that already share command type [C] and event type [E] into one whose state is a
 * [CompositeState] holding each decider's state positionally. Read a slice with [CompositeState.slice]. Unlike the two and
 * three decider overloads, this list form does NOT adapt the deciders for you, because the elements of a single list
 * cannot carry the per element types needed for that. Pass deciders that are already over [C] and [E] (call [adapt] on
 * each first). For two or three deciders prefer the typed [Pair] and [Triple] overloads, which adapt for you. Delegates to
 * [Decider.compose]. It is a `List` rather than a `vararg` so that a two or three decider call resolves to the typed,
 * auto-adapting overloads above rather than this one.
 */
fun <C, E : Any> compose(deciders: List<Decider<C, *, E>>): Decider<C, CompositeState, E> =
    Decider.compose(deciders)

fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, command: C, vararg moreCommands: C): Decision<S, E> = decideOnEvents(events, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, commands: List<C>): Decision<S, E> = decideOnEvents(events, commands)
fun <C, S, E> Decider<C, S, E>.decide(state: S, command: C, vararg moreCommands: C): Decision<S, E> = decideOnState(state, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(state: S, commands: List<C>): Decision<S, E> = decideOnState(state, commands)

operator fun <S, E> Decision<S, E>.component1(): S = state
operator fun <S, E> Decision<S, E>.component2(): List<E> = events