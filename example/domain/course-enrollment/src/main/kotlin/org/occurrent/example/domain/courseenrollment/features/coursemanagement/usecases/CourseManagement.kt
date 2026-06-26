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

package org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCommand.DefineCourse
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.courseDecider
import org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries.courseDecisionContext
import java.time.Instant
import java.util.*

fun DcbApplicationService<DomainEvent>.defineCourse(courseId: UUID, title: String, capacity: Int, occurredAt: Instant = Instant.now()) = execute(
    courseDecisionContext(courseId),
    DefineCourse(UUID.randomUUID(), occurredAt, courseId, title, capacity),
    courseDecider
)