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

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.adaptEvents
import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbQuery
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream

// The decider's event type may be a subtype of the service's event type [E]. These overloads widen it with `adaptEvents`,
// so a feature decider over its own narrow event type can run directly against an injected DcbApplicationService over the
// broader domain event type, without the caller calling `adaptEvents`. When the decider already uses [E] it is a no-op.
// The decision itself is synchronous because the reactive service runs the domain function synchronously, only the read
// and append I/O are reactive.

/**
 * Execute a decider command where [query] is the DCB decision boundary,
 * equivalent to the stream id in stream-based decider helpers.
 *
 * Returns a [Mono] of the [DcbAppendResult], or an empty [Mono] when the decider produced no new events (a no-op
 * command). This is the Kotlin-idiomatic counterpart to the Java [DcbApplicationService.execute] which returns a
 * `Mono<Optional<DcbAppendResult>>`.
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    query: DcbQuery,
    command: C,
    decider: Decider<C, S, SubE>
): Mono<DcbAppendResult> = execute(query, listOf(command), decider)

/**
 * Execute decider commands in order where [query] is the DCB decision boundary.
 *
 * Returns a [Mono] of the [DcbAppendResult], or an empty [Mono] when the decider produced no new events.
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.execute(
    query: DcbQuery,
    commands: List<C>,
    decider: Decider<C, S, SubE>
): Mono<DcbAppendResult> {
    val widened: Decider<C, S, E> = decider.adaptEvents()
    return execute(query) { events: Stream<E> ->
        widened.decideOnEventsAndReturnEvents(events.toList(), commands).stream()
    }.flatMap { Mono.justOrEmpty(it) }
}

/**
 * Execute a command and return the folded state plus the new events decided for [query].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    query: DcbQuery,
    command: C,
    decider: Decider<C, S, SubE>
): Mono<Decider.Decision<S, E>> = executeAndReturnDecision(query, listOf(command), decider)

/**
 * Execute commands and return the folded state plus the new events decided for [query].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnDecision(
    query: DcbQuery,
    commands: List<C>,
    decider: Decider<C, S, SubE>
): Mono<Decider.Decision<S, E>> {
    val widened: Decider<C, S, E> = decider.adaptEvents()
    val decision = AtomicReference<Decider.Decision<S, E>>()
    return execute(query) { events: Stream<E> ->
        val result = widened.decideOnEvents(events.toList(), commands)
        decision.set(result)
        result.events.stream()
    }.then(Mono.fromCallable { decision.get() })
}

/**
 * Execute a command and return the folded state after the decision.
 *
 * The state is bound to a non-null type because a [Mono] cannot carry a null value. A decider whose folded state can be
 * null (the common "does not exist yet" initial state) should use [executeAndReturnDecision] and read its state, or
 * [executeAndReturnEvents], instead.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(query: DcbQuery, command: C, decider: Decider<C, S, SubE>): Mono<S> =
    executeAndReturnDecision(query, command, decider).map { it.state }

/**
 * Execute commands and return the folded state after the decision.
 */
inline fun <C : Any, S : Any, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnState(query: DcbQuery, commands: List<C>, decider: Decider<C, S, SubE>): Mono<S> =
    executeAndReturnDecision(query, commands, decider).map { it.state }

/**
 * Execute a command and return the new events decided for [query].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(query: DcbQuery, command: C, decider: Decider<C, S, SubE>): Mono<List<E>> =
    executeAndReturnDecision(query, command, decider).map { it.events }

/**
 * Execute commands and return the new events decided for [query].
 */
inline fun <C : Any, S, reified SubE : E, E : Any> DcbApplicationService<E>.executeAndReturnEvents(query: DcbQuery, commands: List<C>, decider: Decider<C, S, SubE>): Mono<List<E>> =
    executeAndReturnDecision(query, commands, decider).map { it.events }
