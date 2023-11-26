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
import org.occurrent.application.service.blocking.execute
import org.occurrent.eventstore.api.WriteResult
import java.util.*
import java.util.concurrent.atomic.AtomicReference


// Execute
fun <C, S, E> ApplicationService<E>.execute(streamId: UUID, command: C, decider: Decider<C, S, E>): WriteResult = execute(streamId.toString(), command, decider)
fun <C, S, E> ApplicationService<E>.execute(streamId: String, command: C, decider: Decider<C, S, E>): WriteResult = execute(streamId, listOf(command), decider)
fun <C, S, E> ApplicationService<E>.execute(streamId: UUID, commands: List<C>, decider: Decider<C, S, E>): WriteResult = execute(streamId.toString(), commands, decider)
fun <C, S, E> ApplicationService<E>.execute(streamId: String, commands: List<C>, decider: Decider<C, S, E>): WriteResult = execute(streamId) { events: List<E> ->
    decider.decideOnEventsAndReturnEvents(events, commands)
}

// ExecuteAndReturnDecision
fun <C, S, E> ApplicationService<E>.executeAndReturnDecision(streamId: UUID, command: C, decider: Decider<C, S, E>): Decider.Decision<S, E> = executeAndReturnDecision(streamId.toString(), command, decider)
fun <C, S, E> ApplicationService<E>.executeAndReturnDecision(streamId: UUID, commands: List<C>, decider: Decider<C, S, E>): Decider.Decision<S, E> = executeAndReturnDecision(streamId.toString(), commands, decider)
fun <C, S, E> ApplicationService<E>.executeAndReturnDecision(streamId: String, command: C, decider: Decider<C, S, E>): Decider.Decision<S, E> = executeAndReturnDecision(streamId, listOf(command), decider)
fun <C, S, E> ApplicationService<E>.executeAndReturnDecision(streamId: String, commands: List<C>, decider: Decider<C, S, E>): Decider.Decision<S, E> {
    val cheat = AtomicReference<Decider.Decision<S, E>>()
    execute(streamId) { events: List<E> ->
        val decision: Decider.Decision<S, E> = decider.decideOnEvents(events, commands)
        cheat.set(decision)
        decision.events
    }
    return cheat.get()
}

// ExecuteAndReturnState
fun <C, S, E> ApplicationService<E>.executeAndReturnState(streamId: String, command: C, decider: Decider<C, S, E>): S = executeAndReturnDecision(streamId, command, decider).state
fun <C, S, E> ApplicationService<E>.executeAndReturnState(streamId: UUID, command: C, decider: Decider<C, S, E>): S = executeAndReturnDecision(streamId, command, decider).state
fun <C, S, E> ApplicationService<E>.executeAndReturnState(streamId: String, commands: List<C>, decider: Decider<C, S, E>): S = executeAndReturnDecision(streamId, commands, decider).state
fun <C, S, E> ApplicationService<E>.executeAndReturnState(streamId: UUID, commands: List<C>, decider: Decider<C, S, E>): S = executeAndReturnDecision(streamId, commands, decider).state

// ExecuteAndReturnEvents
fun <C, S, E> ApplicationService<E>.executeAndReturnEvents(streamId: String, command: C, decider: Decider<C, S, E>): List<E> = executeAndReturnDecision(streamId, command, decider).events
fun <C, S, E> ApplicationService<E>.executeAndReturnEvents(streamId: UUID, command: C, decider: Decider<C, S, E>): List<E> = executeAndReturnDecision(streamId, command, decider).events
fun <C, S, E> ApplicationService<E>.executeAndReturnEvents(streamId: String, commands: List<C>, decider: Decider<C, S, E>): List<E> = executeAndReturnDecision(streamId, commands, decider).events
fun <C, S, E> ApplicationService<E>.executeAndReturnEvents(streamId: UUID, commands: List<C>, decider: Decider<C, S, E>): List<E> = executeAndReturnDecision(streamId, commands, decider).events