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

package org.occurrent.dsl.dcb.reactor

import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.application.service.reactor.dcb.DcbExecuteOptions
import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.adaptEvents
import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbCriteria
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream

// A DcbDecider carries both the read boundary (derived from the command) and the tags for the events it emits, so the
// caller no longer passes a separate DcbCriteria. The decider's event type may be a subtype of the service event type
// [E]. These overloads widen it with `adaptEvents` and route the decider's tags through DcbExecuteOptions so they take
// precedence over any global TagGenerator on the service. The decision itself is synchronous because the reactive
// service runs the domain function synchronously, only the read and append I/O are reactive.

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
 * Returns a [Mono] of the [DcbAppendResult], or an empty [Mono] when the decider produced no new events (a no-op
 * command).
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    command: C,
    dcbDecider: DcbDecider<C, S, SubE>
): Mono<DcbAppendResult> = execute(listOf(command), dcbDecider)

/**
 * Execute decider commands in order. The [dcbDecider] carries the DCB decision boundary and the tags for the events it
 * emits.
 *
 * Returns a [Mono] of the [DcbAppendResult], or an empty [Mono] when the decider produced no new events.
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    commands: List<C>,
    dcbDecider: DcbDecider<C, S, SubE>
): Mono<DcbAppendResult> {
    val criteria = dcbCriteriaFor(commands, dcbDecider) ?: return Mono.empty()
    val widened: Decider<C, S, E> = dcbDecider.decider().adaptEvents()
    val tags = TagGenerator<E> { event -> if (event is SubE) dcbDecider.tags().tags(event) else emptySet() }
    val options = DcbExecuteOptions.options<E>().tagGenerator(tags)
    return execute(criteria, options) { events: Stream<E> ->
        widened.decideOnEventsAndReturnEvents(events.toList(), commands).stream()
    }
}

/**
 * Execute a command and return the folded state plus the new events decided by [dcbDecider].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    command: C,
    dcbDecider: DcbDecider<C, S, SubE>
): Mono<Decider.Decision<S, E>> = executeAndReturnDecision(listOf(command), dcbDecider)

/**
 * Execute commands and return the folded state plus the new events decided by [dcbDecider].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    commands: List<C>,
    dcbDecider: DcbDecider<C, S, SubE>
): Mono<Decider.Decision<S, E>> {
    val widened: Decider<C, S, E> = dcbDecider.decider().adaptEvents()
    val tags = TagGenerator<E> { event -> if (event is SubE) dcbDecider.tags().tags(event) else emptySet() }
    // Defer so the AtomicReference is created per subscription. A shared one would let concurrent or repeat
    // subscribers see each other's decision.
    return Mono.defer {
        val criteria = requireNotNull(dcbCriteriaFor(commands, dcbDecider)) {
            "The decider does not recognize the given command(s), so there is no boundary to read and no decision to make"
        }
        val options = DcbExecuteOptions.options<E>().tagGenerator(tags)
        val decision = AtomicReference<Decider.Decision<S, E>>()
        execute(criteria, options) { events: Stream<E> ->
            val result = widened.decideOnEvents(events.toList(), commands)
            decision.set(result)
            result.events.stream()
        }.then(Mono.fromCallable { requireNotNull(decision.get()) { "The decider produced no decision" } })
    }
}

/**
 * Execute a command and return the folded state after the decision.
 *
 * The state is bound to a non-null type because a [Mono] cannot carry a null value. A decider whose folded state can be
 * null (the common "does not exist yet" initial state) should use [executeAndReturnDecision] and read its state, or
 * [executeAndReturnEvents], instead.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(command: C, dcbDecider: DcbDecider<C, S, SubE>): Mono<S> =
    executeAndReturnDecision(command, dcbDecider).map { it.state }

/**
 * Execute commands and return the folded state after the decision.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(commands: List<C>, dcbDecider: DcbDecider<C, S, SubE>): Mono<S> =
    executeAndReturnDecision(commands, dcbDecider).map { it.state }

/**
 * Execute a command and return the new events decided by [dcbDecider].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(command: C, dcbDecider: DcbDecider<C, S, SubE>): Mono<List<E>> =
    executeAndReturnDecision(command, dcbDecider).map { it.events }

/**
 * Execute commands and return the new events decided by [dcbDecider].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(commands: List<C>, dcbDecider: DcbDecider<C, S, SubE>): Mono<List<E>> =
    executeAndReturnDecision(commands, dcbDecider).map { it.events }
