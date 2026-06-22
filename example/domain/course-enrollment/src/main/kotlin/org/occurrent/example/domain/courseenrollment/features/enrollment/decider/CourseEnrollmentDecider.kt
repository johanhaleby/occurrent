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

package org.occurrent.example.domain.courseenrollment.features.enrollment.decider

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.courseenrollment.CourseEnrollmentEvent
import org.occurrent.example.domain.courseenrollment.CourseId
import org.occurrent.example.domain.courseenrollment.StudentId
import java.time.Instant
import java.util.UUID

/**
 * The decider is the pure heart of the domain: given the state folded from a decision boundary and a command, it
 * decides which events to emit. The application service reads the boundary, folds it with [evolve], calls [decide], and
 * conditionally appends the result, retrying on a DCB conflict.
 *
 * The commands below are defined for you. The state shape, [decide], and [evolve] are yours to implement.
 */
fun courseEnrollmentDecider(): Decider<CourseEnrollmentCommand, CourseEnrollmentState, CourseEnrollmentEvent> =
    decider(
        initialState = CourseEnrollmentState(),
        decide = ::decide,
        evolve = ::evolve
    )

sealed interface CourseEnrollmentCommand {
    data class DefineCourse(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val title: String, val capacity: Int) : CourseEnrollmentCommand
    data class RegisterStudent(val eventId: UUID, val occurredAt: Instant, val studentId: StudentId, val name: String) : CourseEnrollmentCommand
    data class EnrollStudent(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val studentId: StudentId) : CourseEnrollmentCommand
    data class UnenrollStudent(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val studentId: StudentId) : CourseEnrollmentCommand
}

/**
 * State folded from a decision boundary. The enrollment boundary spans a course AND a student, so the state must carry
 * enough of both to check every invariant.
 *
 * TODO(human): refine this shape. A workable starting point:
 *  - capacity: Int?                      // null until CourseDefined seen -> "course does not exist"
 *  - studentsEnrolledInCourse: Set<StudentId>
 *  - studentExists: Boolean
 *  - coursesEnrolledByStudent: Set<CourseId>
 * (The decision-context query is scoped to one course id and one student id, so these sets only ever concern those two.)
 */
data class CourseEnrollmentState(
    val todo: Unit = Unit
)

/**
 * TODO(human): implement the decision logic.
 *
 *  - DefineCourse:     emit CourseDefined if the course does not already exist, otherwise reject.
 *  - RegisterStudent:  emit StudentRegistered if the student does not already exist, otherwise reject.
 *  - EnrollStudent:    THE showcase. Reject (throw IllegalArgumentException / IllegalStateException) unless ALL hold:
 *                        * the course exists and the student exists
 *                        * the student is not already enrolled in this course
 *                        * the course is not full (enrolled count < capacity)
 *                        * the student is enrolled in fewer than EnrollmentPolicy.MAX_COURSES_PER_STUDENT courses
 *                      then emit StudentEnrolledInCourse.
 *  - UnenrollStudent:  emit StudentUnenrolledFromCourse if currently enrolled, otherwise reject (or no-op).
 */
private fun decide(command: CourseEnrollmentCommand, state: CourseEnrollmentState): List<CourseEnrollmentEvent> =
    when (command) {
        is CourseEnrollmentCommand.DefineCourse -> TODO("emit CourseDefined if the course doesn't exist yet")
        is CourseEnrollmentCommand.RegisterStudent -> TODO("emit StudentRegistered if the student doesn't exist yet")
        is CourseEnrollmentCommand.EnrollStudent -> TODO("validate the capacity and per-student invariants, then emit StudentEnrolledInCourse")
        is CourseEnrollmentCommand.UnenrollStudent -> TODO("emit StudentUnenrolledFromCourse if currently enrolled")
    }

/**
 * TODO(human): fold each event into the state so [decide] can see the current course capacity, who is enrolled where,
 * and which entities exist.
 */
private fun evolve(state: CourseEnrollmentState, event: CourseEnrollmentEvent): CourseEnrollmentState =
    TODO("update the state for $event")
