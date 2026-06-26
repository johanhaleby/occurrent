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

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.executeList
import org.occurrent.eventstore.api.WriteResult
import java.util.*
import java.util.concurrent.atomic.AtomicReference


// Execute
//
// The decider's event type may be a subtype of the service's event type [E]. The overloads widen it with `adaptEvents`,
// so a feature decider over its own narrow event type can run directly against a service over a broader event type (for
// example an injected ApplicationService<DomainEvent>) without the caller calling `adaptEvents`. When the decider already
// uses [E] the widening is a no-op.
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.execute(streamId: UUID, command: C, decider: Decider<C, S, SubE>): WriteResult = execute(streamId.toString(), command, decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.execute(streamId: String, command: C, decider: Decider<C, S, SubE>): WriteResult = execute(streamId, listOf(command), decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.execute(streamId: UUID, commands: List<C>, decider: Decider<C, S, SubE>): WriteResult = execute(streamId.toString(), commands, decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.execute(streamId: String, commands: List<C>, decider: Decider<C, S, SubE>): WriteResult {
    val widened: Decider<C, S, E> = decider.adaptEvents()
    return executeList(streamId) { events: List<E> -> widened.decideOnEventsAndReturnEvents(events, commands) }
}

// ExecuteAndReturnDecision
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnDecision(streamId: UUID, command: C, decider: Decider<C, S, SubE>): Decider.Decision<S, E> = executeAndReturnDecision(streamId.toString(), command, decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnDecision(streamId: UUID, commands: List<C>, decider: Decider<C, S, SubE>): Decider.Decision<S, E> = executeAndReturnDecision(streamId.toString(), commands, decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnDecision(streamId: String, command: C, decider: Decider<C, S, SubE>): Decider.Decision<S, E> = executeAndReturnDecision(streamId, listOf(command), decider)
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnDecision(streamId: String, commands: List<C>, decider: Decider<C, S, SubE>): Decider.Decision<S, E> {
    val widened: Decider<C, S, E> = decider.adaptEvents()
    val cheat = AtomicReference<Decider.Decision<S, E>>()
    executeList(streamId) { events: List<E> ->
        val decision: Decider.Decision<S, E> = widened.decideOnEvents(events, commands)
        cheat.set(decision)
        decision.events
    }
    return cheat.get()
}

// ExecuteAndReturnState
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnState(streamId: String, command: C, decider: Decider<C, S, SubE>): S = executeAndReturnDecision(streamId, command, decider).state
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnState(streamId: UUID, command: C, decider: Decider<C, S, SubE>): S = executeAndReturnDecision(streamId, command, decider).state
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnState(streamId: String, commands: List<C>, decider: Decider<C, S, SubE>): S = executeAndReturnDecision(streamId, commands, decider).state
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnState(streamId: UUID, commands: List<C>, decider: Decider<C, S, SubE>): S = executeAndReturnDecision(streamId, commands, decider).state

// ExecuteAndReturnEvents
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnEvents(streamId: String, command: C, decider: Decider<C, S, SubE>): List<E> = executeAndReturnDecision(streamId, command, decider).events
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnEvents(streamId: UUID, command: C, decider: Decider<C, S, SubE>): List<E> = executeAndReturnDecision(streamId, command, decider).events
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnEvents(streamId: String, commands: List<C>, decider: Decider<C, S, SubE>): List<E> = executeAndReturnDecision(streamId, commands, decider).events
inline fun <C : Any, S, reified SubE : E, E : Any> ApplicationService<E>.executeAndReturnEvents(streamId: UUID, commands: List<C>, decider: Decider<C, S, SubE>): List<E> = executeAndReturnDecision(streamId, commands, decider).events
