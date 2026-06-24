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

package org.occurrent.example.domain.courseenrollment.common

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider

/**
 * Lifts a decider that only understands a subset of events [E] (typically a single feature's events) into one over the
 * whole [DomainEvent] type, so it can run against the shared `DcbApplicationService<DomainEvent>`.
 *
 * The decision boundary may return events from other features (for example the course boundary also returns enrollment
 * events tagged with that course). Those foreign events are ignored by `evolve`, and `decide` still only emits [E].
 *
 * This is only sound for deciders whose decision does NOT depend on other features' events, such as the course and
 * student deciders. The enrollment decider must stay `Decider<…, DomainEvent>` because it genuinely reads `CourseDefined`
 * and `StudentRegistered` to make its decision, so it cannot be narrowed.
 */
inline fun <C : Any, S, reified E : DomainEvent> Decider<C, S, E>.forDomainEvents(): Decider<C, S, DomainEvent> {
    val self = this
    return decider(
        initialState = self.initialState(),
        decide = { command, state -> self.decide(command, state) },
        evolve = { state, event -> if (event is E) self.evolve(state, event) else state },
        isTerminal = { state -> self.isTerminal(state) }
    )
}
