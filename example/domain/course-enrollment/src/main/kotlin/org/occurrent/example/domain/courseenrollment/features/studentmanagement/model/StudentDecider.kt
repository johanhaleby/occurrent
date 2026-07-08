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

package org.occurrent.example.domain.courseenrollment.features.studentmanagement.model

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.courseenrollment.common.DomainCommand
import org.occurrent.example.domain.courseenrollment.common.StudentId
import java.time.Instant
import java.util.*

/** The boundary for registering or deregistering a student (the student's own events). Also used by the student read side. */
internal fun studentCriteria(studentId: StudentId): DcbCriteria = DcbCriteria.tags(StudentTags.student(studentId))

/**
 * Decider for the student's own lifecycle, wired to its DCB boundary and event tags. The boundary is the student
 * itself (see [studentCriteria]).
 */
val studentDcbDecider: DcbDecider<StudentCommand, StudentRegistry, StudentEvent> = dcbDecider(
    initialState = StudentRegistry(),
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

private fun criteria(command: StudentCommand): DcbCriteria {
    val studentId = when (command) {
        is StudentCommand.RegisterStudent -> command.studentId
        is StudentCommand.DeregisterStudent -> command.studentId
    }
    return studentCriteria(studentId)
}

private fun tags(event: StudentEvent): Set<Tag> = when (event) {
    is StudentRegistered -> setOf(StudentTags.student(event.studentId))
    is StudentDeregistered -> setOf(StudentTags.student(event.studentId))
}

sealed interface StudentCommand : DomainCommand {
    data class RegisterStudent(val eventId: UUID, val occurredAt: Instant, val studentId: StudentId, val name: String) : StudentCommand
    data class DeregisterStudent(val eventId: UUID, val occurredAt: Instant, val studentId: StudentId) : StudentCommand
}

data class StudentRegistry(val students: Map<StudentId, Student> = emptyMap())

private fun decide(command: StudentCommand, state: StudentRegistry): List<StudentEvent> =
    when (command) {
        is StudentCommand.RegisterStudent -> {
            val studentId = command.studentId
            require(!state.isStudentRegistered(studentId)) { "Student $studentId is already registered" }

            listOf(StudentRegistered(command.eventId, command.occurredAt, studentId, command.name))
        }

        is StudentCommand.DeregisterStudent -> {
            val studentId = command.studentId
            require(state.isStudentRegistered(studentId)) { "Student $studentId is not registered" }

            listOf(StudentDeregistered(command.eventId, command.occurredAt, studentId))
        }
    }

private fun evolve(state: StudentRegistry, event: StudentEvent): StudentRegistry = when (event) {
    is StudentRegistered -> state.copy(students = state.students + (event.studentId to Student(event.studentId, event.name, event.occurredAt)))
    is StudentDeregistered -> state.copy(students = state.students - event.studentId)
}

// Helpers
data class Student(val studentId: StudentId, val name: String, val registeredAt: Instant)

private fun StudentRegistry.isStudentRegistered(studentId: StudentId): Boolean = students.containsKey(studentId)
