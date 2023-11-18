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
import java.util.*

fun <C, S, E> decider(initialState: S, decide: (C, S) -> List<E>, evolve: (S, E) -> S, isTerminal: (S) -> Boolean = { false }): Decider<C, S, E> = Decider.create(initialState, decide, evolve, isTerminal)

fun <C, S, E> Decider<C, S, E>.decide(events: List<E>, command: C): Decider.Decision<S, E> = decideOnEvents(events, command)

operator fun <S, E> Decider.Decision<S, E>.component1() : S? = state
operator fun <S, E> Decider.Decision<S, E>.component2() : List<E> = events


fun <C, S, E> ApplicationService<E>.execute(streamId: String, c: C, decider: Decider<C, S, E>) = execute(streamId) { events: List<E> ->
    decider.decideOnEventsAndReturnEvents(events, c)
}

fun <C, S, E> ApplicationService<E>.execute(streamId: UUID, c: C, decider: Decider<C, S, E>) = execute(streamId.toString(), c, decider)