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

package org.occurrent.example.domain.courseenrollment.features.enrollment.model

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainCommand
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import java.time.Instant
import java.util.*

/**
 * The cross-boundary decider, and the point of the example. Its boundary spans a course AND a student at once (see
 * [org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries.enrollmentDecisionContext]), so
 * one conditional append holds both the course-capacity invariant and the per-student-limit invariant.
 *
 * Note how this differs from [org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.courseDecider] and [org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.studentDecider]: it does not emit [org.occurrent.example.domain.courseenrollment.common.CourseDefined] or
 * [org.occurrent.example.domain.courseenrollment.common.StudentRegistered], it only reads them (to learn the capacity and that the entities exist). Most deciders are
 * single-boundary like those two, this is the one that genuinely needs DCB.
 */
val enrollmentDecider: Decider<EnrollmentCommand, EnrollmentState, DomainEvent> =
    decider(
        initialState = EnrollmentState(),
        decide = ::decide,
        evolve = ::evolve
    )

/** Domain policy constants. */
object EnrollmentPolicy {
    /** The maximum number of courses a single student may be enrolled in at once. */
    const val MAX_COURSES_PER_STUDENT: Int = 10
}

sealed interface EnrollmentCommand : DomainCommand {
    data class EnrollStudent(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val studentId: StudentId) : EnrollmentCommand
    data class UnenrollStudent(val eventId: UUID, val occurredAt: Instant, val courseId: CourseId, val studentId: StudentId) : EnrollmentCommand
}

/**
 * State folded from the enrollment boundary.
 *
 * The boundary returns events for the target course (CourseDefined plus enrollments in it) AND the target student
 * (StudentRegistered plus that student's enrollments in any course). [evolve] does not receive the command, so it cannot
 * know which course or student is being decided on. The simplest sound approach is therefore to key the state by id and
 * let [decide] look up the entry for the command's course and student. Because the query is scoped to one course and one
 * student, these maps only ever hold those two.
 *
 * TODO(human): refine this shape if you like. A workable starting point:
 *  - capacityByCourse: Map<CourseId, Int>            (a course exists once it has a capacity here)
 *  - studentsByCourse: Map<CourseId, Set<StudentId>> (to count seats taken and detect a double enroll)
 *  - registeredStudents: Set<StudentId>              (does the student exist)
 *  - coursesByStudent: Map<StudentId, Set<CourseId>> (to enforce MAX_COURSES_PER_STUDENT)
 */
data class EnrollmentState(
    val capacityByCourse: Map<CourseId, Int> = emptyMap(),
    val studentsByCourse: Map<CourseId, Set<StudentId>> = emptyMap(),
    val registeredStudents: Set<StudentId> = emptySet(),
    val coursesByStudent: Map<StudentId, Set<CourseId>> = emptyMap()
)

/**
 * TODO(human): implement the decision, the heart of the example.
 *
 *  - EnrollStudent: reject unless ALL hold, then emit [org.occurrent.example.domain.courseenrollment.common.StudentEnrolledInCourse]:
 *      * the course exists and the student exists
 *      * the student is not already enrolled in this course
 *      * the course is not full (seats taken < capacity)
 *      * the student is in fewer than [EnrollmentPolicy.MAX_COURSES_PER_STUDENT] courses
 *  - UnenrollStudent: emit [org.occurrent.example.domain.courseenrollment.common.StudentUnenrolledFromCourse] if currently enrolled, otherwise reject (or no-op).
 */
private fun decide(command: EnrollmentCommand, state: EnrollmentState): List<DomainEvent> =
    when (command) {
        is EnrollmentCommand.EnrollStudent -> TODO("validate the capacity and per-student invariants, then emit StudentEnrolledInCourse")
        is EnrollmentCommand.UnenrollStudent -> TODO("emit StudentUnenrolledFromCourse if currently enrolled")
    }

/**
 * TODO(human): fold each event into [EnrollmentState]. All four event types appear in this boundary:
 * CourseDefined (capacity), StudentRegistered (existence), StudentEnrolledInCourse / StudentUnenrolledFromCourse (seats
 * and per-student courses). Update both the by-course and by-student maps for the enroll/unenroll events.
 */
private fun evolve(state: EnrollmentState, event: DomainEvent): EnrollmentState =
    TODO("update EnrollmentState for $event")
