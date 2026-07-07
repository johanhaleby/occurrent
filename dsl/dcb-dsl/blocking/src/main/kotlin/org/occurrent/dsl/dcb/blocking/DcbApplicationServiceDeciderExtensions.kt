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

package org.occurrent.dsl.dcb.blocking

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions
import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.adaptEvents
import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbCriteria
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream

// A DcbDecider carries both the read boundary (derived from the command) and the tags for the events it emits, so the
// caller no longer passes a separate DcbCriteria. The decider's event type may be a subtype of the service event type
// [E]. These overloads widen it with `adaptEvents` and route the decider's tags through DcbExecuteOptions so they take
// precedence over any global TagGenerator on the service.

/**
 * Resolve the read boundary for [commands] from [dcbDecider]. The boundary comes from the command, and every command in
 * one execute must resolve to the same boundary because they are appended atomically under one condition. Returns
 * `null` when the decider does not recognize the command(s), which the callers treat as a no-op.
 */
@PublishedApi
internal fun <C : Any, E : Any> dcbCriteriaFor(commands: List<C>, dcbDecider: DcbDecider<C, *, E>): DcbCriteria? {
    require(commands.isNotEmpty()) { "Must supply at least one command" }
    val first = dcbDecider.criteria().apply(commands.first())
    for (i in 1 until commands.size) {
        require(dcbDecider.criteria().apply(commands[i]) == first) {
            "All commands in a single execute must resolve to the same DcbCriteria boundary, they are appended atomically under one condition"
        }
    }
    return first
}

/**
 * Execute a decider command. The [dcbDecider] carries the DCB decision boundary and the tags for the events it emits.
 *
 * Returns the [DcbAppendResult], or `null` when the decider produced no new events (a no-op command). This is the
 * Kotlin-idiomatic counterpart to the Java [DcbApplicationService.execute] which returns `Optional<DcbAppendResult>`.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    command: C,
    dcbDecider: DcbDecider<C, S, SubE>
): DcbAppendResult? = execute(listOf(command), dcbDecider)

/**
 * Execute decider commands in order. The [dcbDecider] carries the DCB decision boundary and the tags for the events it
 * emits.
 *
 * Returns the [DcbAppendResult], or `null` when the decider produced no new events.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    commands: List<C>,
    dcbDecider: DcbDecider<C, S, SubE>
): DcbAppendResult? {
    val criteria = dcbCriteriaFor(commands, dcbDecider) ?: return null
    val widened: Decider<C, S, E> = dcbDecider.decider().adaptEvents()
    val tags = TagGenerator<E> { event -> if (event is SubE) dcbDecider.tags().tags(event) else emptySet() }
    val options = DcbExecuteOptions.options<E>().tagGenerator(tags)
    return execute(criteria, options) { events: Stream<E> ->
        widened.decideOnEventsAndReturnEvents(events.toList(), commands).stream()
    }.orElse(null)
}

/**
 * Execute a command and return the folded state plus the new events decided by [dcbDecider].
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    command: C,
    dcbDecider: DcbDecider<C, S, SubE>
): Decider.Decision<S, E> = executeAndReturnDecision(listOf(command), dcbDecider)

/**
 * Execute commands and return the folded state plus the new events decided by [dcbDecider].
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    commands: List<C>,
    dcbDecider: DcbDecider<C, S, SubE>
): Decider.Decision<S, E> {
    val criteria = requireNotNull(dcbCriteriaFor(commands, dcbDecider)) {
        "The decider does not recognize the given command(s), so there is no boundary to read and no decision to make"
    }
    val widened: Decider<C, S, E> = dcbDecider.decider().adaptEvents()
    val tags = TagGenerator<E> { event -> if (event is SubE) dcbDecider.tags().tags(event) else emptySet() }
    val options = DcbExecuteOptions.options<E>().tagGenerator(tags)
    val decision = AtomicReference<Decider.Decision<S, E>>()
    execute(criteria, options) { events: Stream<E> ->
        val result = widened.decideOnEvents(events.toList(), commands)
        decision.set(result)
        result.events.stream()
    }
    return decision.get()
}

/**
 * Execute a command and return the folded state after the decision.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(command: C, dcbDecider: DcbDecider<C, S, SubE>): S =
    executeAndReturnDecision(command, dcbDecider).state

/**
 * Execute commands and return the folded state after the decision.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(commands: List<C>, dcbDecider: DcbDecider<C, S, SubE>): S =
    executeAndReturnDecision(commands, dcbDecider).state

/**
 * Execute a command and return the new events decided by [dcbDecider].
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(command: C, dcbDecider: DcbDecider<C, S, SubE>): List<E> =
    executeAndReturnDecision(command, dcbDecider).events

/**
 * Execute commands and return the new events decided by [dcbDecider].
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(commands: List<C>, dcbDecider: DcbDecider<C, S, SubE>): List<E> =
    executeAndReturnDecision(commands, dcbDecider).events
