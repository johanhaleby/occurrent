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
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentCommand.EnrollStudent
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentCommand.UnenrollStudent
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.enrollmentDecider
import org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries.enrollmentDecisionContext
import java.time.Instant
import java.util.*

fun DcbApplicationService<DomainEvent>.enrollStudent(courseId: UUID, studentId: UUID, occurredAt: Instant = Instant.now()) = execute(
    enrollmentDecisionContext(courseId, studentId),
    EnrollStudent(UUID.randomUUID(), occurredAt, courseId, studentId),
    enrollmentDecider
)

fun DcbApplicationService<DomainEvent>.unenrollStudent(courseId: UUID, studentId: UUID, occurredAt: Instant = Instant.now()) = execute(
    enrollmentDecisionContext(courseId, studentId),
    UnenrollStudent(UUID.randomUUID(), occurredAt, courseId, studentId),
    enrollmentDecider
)
