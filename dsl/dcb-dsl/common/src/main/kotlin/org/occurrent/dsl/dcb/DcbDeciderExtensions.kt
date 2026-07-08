/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.dsl.dcb

import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.dsl.decider.CompositeState
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.dsl.decider.compose as decidersCompose

/**
 * A utility function for creating a [DcbDecider] a bit more nicely in Kotlin.
 */
fun <C : Any, S, E : Any> dcbDecider(decider: Decider<C, S, E>, criteria: (C) -> DcbCriteria, tags: (E) -> Set<Tag>): DcbDecider<C, S, E> =
    DcbDecider.from(decider, { c -> criteria(c) }, TagGenerator { e -> tags(e) })

/**
 * Builds a [DcbDecider] directly from decision parts plus its DCB [criteria] and [tags], without naming an
 * intermediate [Decider].
 */
fun <C : Any, S, E : Any> dcbDecider(
    initialState: S,
    decide: (C, S) -> List<E>,
    evolve: (S, E) -> S,
    criteria: (C) -> DcbCriteria,
    tags: (E) -> Set<Tag>,
    isTerminal: (S) -> Boolean = { false },
): DcbDecider<C, S, E> = dcbDecider(decider(initialState, decide, evolve, isTerminal), criteria, tags)

/**
 * Wraps this [Decider] with the DCB [criteria] (the read boundary for a command) and [tags] (the tags for events it
 * emits) it needs to run against a DCB event store, producing a [DcbDecider].
 */
fun <C : Any, S, E : Any> Decider<C, S, E>.toDcb(criteria: (C) -> DcbCriteria, tags: (E) -> Set<Tag>): DcbDecider<C, S, E> =
    dcbDecider(this, criteria, tags)

/**
 * Combine two feature DcbDeciders into one whose state is the [Pair] of their states, mirroring the [Decider] two-ary
 * [compose][org.occurrent.dsl.decider.compose]. The deciders are adapted to the shared command type [C] and event type
 * [E] for you. The combined criteria is the union of the boundaries of whichever of [first]/[second] recognizes the
 * command, and the combined tags is the union of tags contributed by whichever recognizes the event.
 */
inline fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, E : Any> compose(
    first: DcbDecider<C1, S1, E1>,
    second: DcbDecider<C2, S2, E2>
): DcbDecider<C, Pair<S1, S2>, E> {
    val combinedDecider: Decider<C, Pair<S1, S2>, E> = decidersCompose(first.decider(), second.decider())
    val combinedCriteria: (C) -> DcbCriteria? = { c ->
        val parts = listOfNotNull(
            if (c is C1) first.criteria().apply(c) else null,
            if (c is C2) second.criteria().apply(c) else null
        )
        if (parts.isEmpty()) null else DcbCriteria.anyOf(parts)
    }
    val combinedTags = TagGenerator<E> { e ->
        buildSet {
            if (e is E1) addAll(first.tags().tags(e))
            if (e is E2) addAll(second.tags().tags(e))
        }
    }
    return DcbDecider.from(combinedDecider, combinedCriteria, combinedTags)
}

/**
 * Infix form of the two DcbDecider [compose], so you can write `courseDcbDecider compose studentDcbDecider`. Two
 * deciders only, for the same reason as the [Decider] infix compose: use the prefix `compose(a, b, c)` for three.
 */
@JvmName("composeWith")
inline infix fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, E : Any> DcbDecider<C1, S1, E1>.compose(
    other: DcbDecider<C2, S2, E2>
): DcbDecider<C, Pair<S1, S2>, E> = compose(this, other)

/**
 * Combine three feature DcbDeciders into one whose state is the [Triple] of their states. Works like the two-ary
 * [compose], extended to a third.
 */
inline fun <C : Any, reified C1 : C, S1, reified E1 : E, reified C2 : C, S2, reified E2 : E, reified C3 : C, S3, reified E3 : E, E : Any> compose(
    first: DcbDecider<C1, S1, E1>,
    second: DcbDecider<C2, S2, E2>,
    third: DcbDecider<C3, S3, E3>
): DcbDecider<C, Triple<S1, S2, S3>, E> {
    val combinedDecider: Decider<C, Triple<S1, S2, S3>, E> = decidersCompose(first.decider(), second.decider(), third.decider())
    val combinedCriteria: (C) -> DcbCriteria? = { c ->
        val parts = listOfNotNull(
            if (c is C1) first.criteria().apply(c) else null,
            if (c is C2) second.criteria().apply(c) else null,
            if (c is C3) third.criteria().apply(c) else null
        )
        if (parts.isEmpty()) null else DcbCriteria.anyOf(parts)
    }
    val combinedTags = TagGenerator<E> { e ->
        buildSet {
            if (e is E1) addAll(first.tags().tags(e))
            if (e is E2) addAll(second.tags().tags(e))
            if (e is E3) addAll(third.tags().tags(e))
        }
    }
    return DcbDecider.from(combinedDecider, combinedCriteria, combinedTags)
}

/**
 * Combine DcbDeciders given as a list into one whose state is a [CompositeState]. Does NOT adapt for you, so the
 * deciders must already share command type [C] and event type [E] (call [DcbDecider.adapt] on each first). For two or
 * three deciders prefer the typed [Pair]/[Triple] overloads, which adapt for you.
 */
fun <C : Any, E : Any> compose(deciders: List<DcbDecider<C, *, E>>): DcbDecider<C, CompositeState, E> =
    DcbDecider.compose(deciders)

/**
 * Combine four or more DcbDeciders into one whose state is a [CompositeState]. Like the list [compose] it does NOT
 * adapt for you. Two and three deciders are handled by the typed [Pair]/[Triple] overloads, so this form requires four
 * leading deciders.
 */
fun <C : Any, E : Any> compose(
    first: DcbDecider<C, *, E>,
    second: DcbDecider<C, *, E>,
    third: DcbDecider<C, *, E>,
    fourth: DcbDecider<C, *, E>,
    vararg rest: DcbDecider<C, *, E>
): DcbDecider<C, CompositeState, E> =
    DcbDecider.compose(listOf(first, second, third, fourth, *rest))
