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
 * Widen a feature decider so it can run where a decider over broader command and event types is expected, for example
 * against an application service for the whole domain. Commands and events that are not this decider's own are ignored.
 * The narrow types come from the receiver and the broader [C] and [E] from the call site (usually the expected type at an
 * `execute` call), so you normally just write `courseDecider.adapt()`.
 *
 * ```
 * // courseDecider: Decider<CourseCommand, CourseState, CourseEvent>
 * val widened: Decider<DomainCommand, CourseState, DomainEvent> = courseDecider.adapt()
 * ```
 */
inline fun <reified SubC : C, S, reified SubE : E, C, E : Any> Decider<SubC, S, SubE>.adapt(): Decider<C, S, E> =
    Decider.adapt(this, SubC::class.java, SubE::class.java)

/**
 * Widen only the event type of a decider, from [SubE] to the broader [E], leaving the command type unchanged. Events that
 * are not [SubE] are ignored. This is the event-only counterpart to [adapt], for when the command type already matches but
 * the decider's event type is narrower than the service it runs against. It is what the `execute` extensions use to accept
 * a feature decider directly, so you rarely call it by hand.
 *
 * ```
 * // courseDecider: Decider<CourseCommand, CourseState, CourseEvent>
 * val widened: Decider<CourseCommand, CourseState, DomainEvent> = courseDecider.adaptEvents()
 * ```
 */
inline fun <C : Any, S, reified SubE : E, E : Any> Decider<C, S, SubE>.adaptEvents(): Decider<C, S, E> {
    val self = this
    return decider<C, S, E>(
        initialState = self.initialState(),
        decide = { command, state -> self.decide(command, state) },
        evolve = { state, event -> if (event is SubE) self.evolve(state, event) else state },
        isTerminal = { state -> self.isTerminal(state) }
    )
}

/**
 * Combine two feature deciders into one whose state is the [Pair] of their states. The deciders are [adapt]ed to the
 * shared command type [C] and event type [E] for you, so you can pass them over their own narrow types directly. A command
 * goes to whichever decider recognizes it, and each event updates only its own decider's state, so they stay independent.
 * Annotate the result type so [C] and [E] are known.
 *
 * ```
 * val decider: Decider<DomainCommand, Pair<CourseState, StudentState>, DomainEvent> =
 *     compose(courseDecider, studentDecider)
 * ```
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
 * Infix form of the two decider [compose], so you can write `courseDecider compose studentDecider`. Two deciders only:
 * because infix is left associative, `a compose b compose c` would give a nested `Pair<Pair<S1, S2>, S3>` rather than a
 * `Triple`, so use the prefix `compose(a, b, c)` for three.
 *
 * ```
 * val decider: Decider<DomainCommand, Pair<CourseState, StudentState>, DomainEvent> =
 *     courseDecider compose studentDecider
 * ```
 *
 * (The `@JvmName` only avoids a JVM signature clash with the prefix `compose`. Kotlin callers use `compose` either way.)
 */
@JvmName("composeWith")
inline infix fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, E : Any> Decider<C1, S1, E1>.compose(
    other: Decider<C2, S2, E2>
): Decider<C, Pair<S1, S2>, E> = compose(this, other)

/**
 * Combine three feature deciders into one whose state is the [Triple] of their states. Works like the two decider
 * [compose], extended to a third. The deciders are [adapt]ed for you, so pass them over their own narrow types directly.
 * Annotate the result type so [C] and [E] are known.
 *
 * ```
 * val decider: Decider<DomainCommand, Triple<CourseState, StudentState, EnrollmentState>, DomainEvent> =
 *     compose(courseDecider, studentDecider, enrollmentDecider)
 * ```
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
 * Combine deciders given as a list into one whose state is a [CompositeState] (read a slice with [CompositeState.slice]).
 * This does NOT adapt for you, so the deciders must already share command type [C] and event type [E] (call [adapt] on
 * each first). For two or three deciders prefer the typed [Pair] and [Triple] overloads, which adapt for you. Use this, or
 * the vararg [compose], for four or more. It takes a `List` rather than a `vararg` so that two and three decider calls
 * still resolve to the typed, auto-adapting overloads.
 *
 * ```
 * val decider: Decider<DomainCommand, CompositeState, DomainEvent> =
 *     compose(listOf(courseDecider.adapt(), studentDecider.adapt(), enrollmentDecider.adapt(), roomDecider.adapt()))
 * ```
 */
fun <C, E : Any> compose(deciders: List<Decider<C, *, E>>): Decider<C, CompositeState, E> =
    Decider.compose(deciders)

/**
 * Combine four or more deciders into one whose state is a [CompositeState] (read a slice with [CompositeState.slice]).
 * Like the list [compose] it does NOT adapt for you, so pass deciders that already share command type [C] and event type
 * [E] (call [adapt] on each first). Two and three deciders are handled by the typed [Pair] and [Triple] overloads, so this
 * form requires four leading deciders, which is also what keeps it from clashing with those overloads.
 *
 * ```
 * val decider: Decider<DomainCommand, CompositeState, DomainEvent> =
 *     compose(courseDecider.adapt(), studentDecider.adapt(), enrollmentDecider.adapt(), roomDecider.adapt())
 * ```
 */
fun <C, E : Any> compose(first: Decider<C, *, E>, second: Decider<C, *, E>, third: Decider<C, *, E>, fourth: Decider<C, *, E>, vararg rest: Decider<C, *, E>): Decider<C, CompositeState, E> =
    Decider.compose(listOf(first, second, third, fourth, *rest))

fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, command: C, vararg moreCommands: C): Decision<S, E> = decideOnEvents(events, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, commands: List<C>): Decision<S, E> = decideOnEvents(events, commands)
fun <C, S, E> Decider<C, S, E>.decide(state: S, command: C, vararg moreCommands: C): Decision<S, E> = decideOnState(state, listOf(command, *moreCommands))
fun <C, S, E> Decider<C, S, E>.decide(state: S, commands: List<C>): Decision<S, E> = decideOnState(state, commands)

operator fun <S, E> Decision<S, E>.component1(): S = state
operator fun <S, E> Decision<S, E>.component2(): List<E> = events