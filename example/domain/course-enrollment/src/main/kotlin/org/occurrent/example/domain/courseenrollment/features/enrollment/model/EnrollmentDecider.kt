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
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
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
    val courseId: CourseId
    val studentId: StudentId

    data class EnrollStudent(val eventId: UUID, val occurredAt: Instant, override val courseId: CourseId, override val studentId: StudentId) : EnrollmentCommand
    data class UnenrollStudent(val eventId: UUID, val occurredAt: Instant, override val courseId: CourseId, override val studentId: StudentId) : EnrollmentCommand
}

/**
 * State folded from the enrollment boundary.
 *
 * The boundary returns events for the target course (CourseDefined plus enrollments in it) AND the target student
 * (StudentRegistered plus that student's enrollments in any course). [evolve] does not receive the command, so it cannot
 * know which course or student is being decided on. The simplest sound approach is therefore to key the state by id and
 * let [decide] look up the entry for the command's course and student. Because the query is scoped to one course and one
 * student, these maps only ever hold those two.
 */
data class EnrollmentState(
    val capacityByCourse: Map<CourseId, Int> = emptyMap(),
    val studentsByCourse: Map<CourseId, Set<StudentId>> = emptyMap(),
    val registeredStudents: Set<StudentId> = emptySet(),
    val coursesByStudent: Map<StudentId, Set<CourseId>> = emptyMap()
)

private fun decide(command: EnrollmentCommand, state: EnrollmentState): List<DomainEvent> {
    val studentId = command.studentId
    val courseId = command.courseId

    require(state.isCourseDefined(courseId)) {
        "Course ${command.courseId} is not defined"
    }

    require(state.isStudentRegistered(studentId)) {
        "Student ${command.studentId} is not registered"
    }

    return when (command) {
        is EnrollmentCommand.EnrollStudent -> {
            require(!state.isCourseFull(courseId)) {
                "Course ${command.courseId} is full"
            }

            require(!state.isStudentAtCourseEnrollmentLimit(studentId)) {
                "Student ${command.studentId} is already enrolled in ${EnrollmentPolicy.MAX_COURSES_PER_STUDENT} courses"
            }

            require(!state.isStudentRegisteredToCourse(courseId, studentId)) {
                "Student ${command.studentId} is already enrolled in course ${command.courseId}"
            }

            listOf(StudentEnrolledInCourse(UUID.randomUUID(), command.occurredAt, courseId, studentId))
        }

        is EnrollmentCommand.UnenrollStudent -> {
            require(state.isStudentRegisteredToCourse(courseId, studentId)) {
                "Student ${command.studentId} is not enrolled in course ${command.courseId}"
            }

            listOf(StudentUnenrolledFromCourse(UUID.randomUUID(), command.occurredAt, courseId, studentId))
        }
    }
}

private fun evolve(state: EnrollmentState, event: DomainEvent): EnrollmentState = when (event) {
    is CourseDefined -> state.copy(capacityByCourse = state.capacityByCourse + (event.courseId to event.capacity))
    is StudentRegistered -> state.copy(registeredStudents = state.registeredStudents + event.studentId)
    is StudentEnrolledInCourse -> state.copy(
        studentsByCourse = state.studentsByCourse + (event.courseId to (state.studentsByCourse[event.courseId]?.plus(event.studentId) ?: setOf(event.studentId))),
        coursesByStudent = state.coursesByStudent + (event.studentId to (state.coursesByStudent[event.studentId]?.plus(event.courseId) ?: setOf(event.courseId)))
    )

    is StudentUnenrolledFromCourse -> state.copy(
        studentsByCourse = state.studentsByCourse + (event.courseId to (state.studentsByCourse[event.courseId]?.minus(event.studentId) ?: emptySet())),
        coursesByStudent = state.coursesByStudent + (event.studentId to (state.coursesByStudent[event.studentId]?.minus(event.courseId) ?: emptySet()))
    )

    else -> throw IllegalArgumentException("Unexpected event type ${event::class.simpleName} in enrollment boundary")
}


// Helpers
private fun EnrollmentState.isCourseFull(courseId: CourseId): Boolean {
    val capacity = capacityByCourse[courseId] ?: return false
    val enrolledStudents = studentsByCourse[courseId]?.size ?: 0
    return enrolledStudents >= capacity
}

private fun EnrollmentState.isStudentAtCourseEnrollmentLimit(studentId: StudentId): Boolean {
    return (coursesByStudent[studentId]?.size ?: 0) == EnrollmentPolicy.MAX_COURSES_PER_STUDENT
}

private fun EnrollmentState.isCourseDefined(courseId: CourseId): Boolean = capacityByCourse[courseId] != null

private fun EnrollmentState.isStudentRegistered(studentId: StudentId): Boolean = registeredStudents.contains(studentId)

private fun EnrollmentState.isStudentRegisteredToCourse(courseId: CourseId, studentId: StudentId): Boolean = studentsByCourse[courseId]?.contains(studentId) ?: false
