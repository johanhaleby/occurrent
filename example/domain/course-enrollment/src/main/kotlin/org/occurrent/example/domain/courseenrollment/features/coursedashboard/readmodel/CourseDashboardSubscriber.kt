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

import jakarta.annotation.PostConstruct
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCancelled
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.blocking.durable.catchup.DcbSubscriptionPosition
import org.springframework.stereotype.Component

/**
 * Feeds the [CourseDashboard] read model from a DCB subscription.
 *
 * Starting at [DcbSubscriptionPosition] zero makes the subscription replay the whole DCB history by dcbposition on every
 * boot and then switch to live delivery, so the in-memory read model is rebuilt from scratch each start. This catch-up
 * is only available because the starter wires a DCB-mode catch-up subscription model in DCB-only mode.
 */
@Component
class CourseDashboardSubscriber(
    private val dcbSubscriptions: DcbSubscriptions<DomainEvent>,
    private val courseDashboard: CourseDashboard
) {

    @PostConstruct
    fun start() {
        dcbSubscriptions.subscribe("course-dashboard", DcbQuery.all(), StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0))) { event ->
            if (event is CourseDefined || event is CourseCancelled || event is StudentRegistered || event is StudentDeregistered ||
                event is StudentEnrolledInCourse || event is StudentUnenrolledFromCourse) {
                courseDashboard.update(event)
            }
        }.waitUntilStarted()
    }
}
