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

package org.occurrent.example.domain.courseenrollment.features.coursemanagement.model

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainCommand
import java.time.Instant
import java.util.*

/**
 * Decider for the course's own lifecycle. Single boundary: the course (see
 * [org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries.courseDecisionContext]).
 */
val courseDecider: Decider<CourseCommand, CourseState, CourseEvent> =
    decider(
        initialState = CourseState.NotDefined,
        decide = ::decide,
        evolve = ::evolve
    )

sealed interface CourseCommand : DomainCommand {
    data class DefineCourse(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val title: String, val capacity: Int) : CourseCommand
}

sealed interface CourseState {
    data object NotDefined : CourseState
    data class Defined(val capacity: Int) : CourseState
}

private fun decide(command: CourseCommand, state: CourseState): List<CourseEvent> =
    when (command) {
        is CourseCommand.DefineCourse -> when (state) {
            CourseState.NotDefined -> listOf(CourseDefined(UUID.randomUUID(), command.occurredAt, command.courseId, command.title, command.capacity))
            is CourseState.Defined -> throw IllegalArgumentException("Course ${command.title} is already defined")
        }
    }

/**
 * TODO(human): fold the course boundary into [CourseState]. The boundary also returns enrollment events tagged with this
 * course, which this decider does not care about, so they leave the state unchanged.
 */
private fun evolve(state: CourseState, event: CourseEvent): CourseState =
    TODO("update CourseState for $event (CourseEvent is sealed, so 'when (event)' is exhaustive: CourseDefined -> Defined)")
