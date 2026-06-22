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

package org.occurrent.example.domain.courseenrollment.features.enrollment.usecases

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.dsl.decider.Decider
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.example.domain.courseenrollment.CourseEnrollmentEvent
import org.occurrent.example.domain.courseenrollment.StudentId
import org.occurrent.example.domain.courseenrollment.features.dcb.CourseEnrollmentDcbQueries
import org.occurrent.example.domain.courseenrollment.features.enrollment.decider.CourseEnrollmentCommand
import org.occurrent.example.domain.courseenrollment.features.enrollment.decider.CourseEnrollmentState
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

/** Registers a student. Single-entity boundary (the student). */
@Service
class RegisterStudent(
    private val applicationService: DcbApplicationService<CourseEnrollmentEvent>,
    private val decider: Decider<CourseEnrollmentCommand, CourseEnrollmentState, CourseEnrollmentEvent>
) {

    @Transactional
    @Retryable(
        include = [DcbAppendConditionNotFulfilledException::class, DataIntegrityViolationException::class],
        maxAttempts = 5,
        backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000)
    )
    operator fun invoke(studentId: StudentId, name: String, occurredAt: Instant = Instant.now()) {
        applicationService.execute(
            CourseEnrollmentDcbQueries.studentDecisionContext(studentId),
            CourseEnrollmentCommand.RegisterStudent(UUID.randomUUID(), occurredAt, studentId, name),
            decider
        )
    }
}
