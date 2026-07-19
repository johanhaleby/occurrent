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

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainCommand
import java.time.Instant
import java.util.*

/** The boundary for defining or cancelling a course (the course's own events). Also used by the course read side. */
internal fun courseCriteria(courseId: CourseId): DcbCriteria = DcbCriteria.tags(CourseTags.course(courseId))

/**
 * Decider for the course's own lifecycle, wired to its DCB boundary and event tags. Single boundary: the course (see
 * [courseCriteria]).
 */
val courseDcbDecider: DcbDecider<CourseCommand, CourseState, CourseEvent> = dcbDecider(
    initialState = CourseState.NotDefined,
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

private fun criteria(command: CourseCommand): DcbCriteria {
    val courseId = when (command) {
        is CourseCommand.DefineCourse -> command.courseId
        is CourseCommand.CancelCourse -> command.courseId
    }
    return courseCriteria(courseId)
}

private fun tags(event: CourseEvent): Set<Tag> = when (event) {
    is CourseDefined -> setOf(CourseTags.course(event.courseId))
    is CourseCancelled -> setOf(CourseTags.course(event.courseId))
}

sealed interface CourseCommand : DomainCommand {
    data class DefineCourse(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val title: String, val capacity: Int) : CourseCommand
    data class CancelCourse(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId) : CourseCommand
}

sealed interface CourseState {
    data object NotDefined : CourseState
    data class Defined(val courseId: CourseId, val title: String, val capacity: Int, val definedAt: Instant) : CourseState
    data object Cancelled : CourseState
}

private fun decide(command: CourseCommand, state: CourseState): List<CourseEvent> = when (command) {
    is CourseCommand.DefineCourse -> when (state) {
        CourseState.NotDefined -> listOf(CourseDefined(UUID.randomUUID(), command.occurredAt, command.courseId, command.title, command.capacity))
        is CourseState.Defined -> throw CourseAlreadyDefinedException(command.title)
        CourseState.Cancelled -> throw CourseCancelledCannotBeRedefinedException(command.courseId)
    }

    is CourseCommand.CancelCourse -> when (state) {
        is CourseState.Defined -> listOf(CourseCancelled(UUID.randomUUID(), command.occurredAt, command.courseId))
        CourseState.NotDefined -> throw CourseNotDefinedException(command.courseId)
        CourseState.Cancelled -> throw CourseAlreadyCancelledException(command.courseId)
    }
}

private fun evolve(state: CourseState, event: CourseEvent): CourseState = when (event) {
    is CourseDefined -> CourseState.Defined(event.courseId, event.title, event.capacity, event.occurredAt)
    is CourseCancelled -> CourseState.Cancelled
}
