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

package org.occurrent.example.domain.courseenrollment.features.coursedashboard.readmodel

import org.occurrent.annotation.DcbSubscription
import org.occurrent.annotation.DcbSubscription.DcbStartPosition
import org.occurrent.annotation.DcbSubscription.ResumeBehavior
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCancelled
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.springframework.stereotype.Component

/**
 * Feeds the [CourseDashboard] read model from a DCB subscription declared with [DcbSubscription].
 *
 * The read model is in-memory only, so it must be rebuilt from the whole DCB history on every boot. That is why this
 * combines [DcbStartPosition.BEGINNING] with [ResumeBehavior.SAME_AS_START_AT]: BEGINNING alone would replay only the
 * first time and then resume from the stored position on later restarts, which would leave the in-memory model missing
 * all history before that position. SAME_AS_START_AT replays from the beginning on every boot (and keeps no checkpoint).
 * This catch-up is only available because the starter wires a DCB-mode catch-up subscription model in DCB-only mode. The
 * event types are narrowed on the annotation, so the subscription receives only the dashboard's events server-side.
 */
@Component
class CourseDashboardSubscriber(private val courseDashboard: CourseDashboard) {

    @DcbSubscription(
        id = "course-dashboard",
        eventTypes = [CourseDefined::class, CourseCancelled::class, StudentRegistered::class, StudentDeregistered::class, StudentEnrolledInCourse::class, StudentUnenrolledFromCourse::class],
        startAt = DcbStartPosition.BEGINNING,
        resumeBehavior = ResumeBehavior.SAME_AS_START_AT
    )
    fun update(event: DomainEvent) {
        courseDashboard.update(event)
    }
}
