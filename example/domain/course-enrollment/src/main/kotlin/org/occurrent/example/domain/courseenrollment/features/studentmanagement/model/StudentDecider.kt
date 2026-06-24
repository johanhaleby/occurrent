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

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.courseenrollment.common.DomainCommand
import org.occurrent.example.domain.courseenrollment.common.StudentId
import java.time.Instant
import java.util.UUID

/**
 * Decider for the student's own lifecycle. Single boundary: the student (see
 * [org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries.studentDecisionContext]).
 */
val studentDecider: Decider<StudentCommand, StudentState, StudentEvent> =
    decider(
        initialState = StudentState.NotRegistered,
        decide = ::decide,
        evolve = ::evolve
    )

sealed interface StudentCommand : DomainCommand {
    data class RegisterStudent(val eventId: UUID, val occurredAt: Instant, val studentId: StudentId, val name: String) : StudentCommand
}

sealed interface StudentState {
    data object NotRegistered : StudentState
    data object Registered : StudentState
}

/**
 * TODO(human): emit [org.occurrent.example.domain.courseenrollment.common.StudentRegistered] when the student is not yet registered, otherwise reject.
 */
private fun decide(command: StudentCommand, state: StudentState): List<StudentEvent> =
    when (command) {
        is StudentCommand.RegisterStudent -> TODO("emit StudentRegistered if state is NotRegistered, otherwise reject")
    }

/**
 * TODO(human): fold the student boundary into [StudentState] (StudentRegistered -> Registered, everything else -> state).
 */
private fun evolve(state: StudentState, event: StudentEvent): StudentState =
    TODO("update StudentState for $event")
