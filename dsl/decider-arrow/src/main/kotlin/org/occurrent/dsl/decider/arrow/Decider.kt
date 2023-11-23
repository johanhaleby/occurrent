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

package org.occurrent.dsl.decider.arrow

import arrow.core.Either

interface Decider<C, S, E, ER> {
    fun initialState(): S
    fun decide(command: C, state: S): Either<ER, List<E>>
    fun evolve(state: S, event: E): S
    fun isTerminal(state: S): Boolean = false
}


fun <C, S, E, ER> decider(initialState: S, decide: (C, S) -> Either<ER, List<E>>, evolve: (S, E) -> S, isTerminal: (S) -> Boolean = { false }): Decider<C, S, E, ER> =
    object : Decider<C, S, E, ER> {
        override fun initialState(): S = initialState
        override fun evolve(state: S, event: E): S = evolve(state, event)
        override fun decide(command: C, state: S): Either<ER, List<E>> = decide(command, state)
        override fun isTerminal(state: S): Boolean = isTerminal(state)
    }