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

import org.occurrent.annotation.Projection
import org.occurrent.annotation.Projection.Mode
import org.occurrent.annotation.Projection.ResumeBehavior
import org.occurrent.annotation.Projection.StartPosition
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.projection.dcbProjection
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCancelled
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Registers the `course-dashboard` [Projection] that feeds [CourseDashboard], which also serves as its
 * [org.occurrent.dsl.view.ViewStateRepository] (bean name `courseDashboard`, matched by [Projection.store]).
 *
 * The read model is in-memory only, so it must be rebuilt from the whole DCB history on every boot. That is why this
 * combines [StartPosition.BEGINNING] with [ResumeBehavior.SAME_AS_START_AT]: BEGINNING alone would replay only the
 * first time and then resume from the stored position on later restarts, which would leave the in-memory model missing
 * all history before that position. SAME_AS_START_AT replays from the beginning on every boot (and keeps no checkpoint).
 * There is a single dashboard instance for the whole module, so [dcbProjection]'s `id` block returns the constant
 * [COURSE_DASHBOARD_ID] regardless of which event is being folded. The projection subscribes to all six event types it
 * folds and applies no further tag boundary, mirroring the event-type-only DCB subscription it replaces.
 */
@Configuration(proxyBeanMethods = false)
class CourseDashboardProjectionConfiguration {

    @Bean
    @Projection(
        id = COURSE_DASHBOARD_ID,
        startAt = StartPosition.BEGINNING,
        resumeBehavior = ResumeBehavior.SAME_AS_START_AT,
        mode = Mode.ASYNC,
        store = "courseDashboard"
    )
    fun courseDashboardProjection(): DcbProjection<DashboardState, DomainEvent, String> =
        dcbProjection(initialState = DashboardState.EMPTY) {
            id { COURSE_DASHBOARD_ID }

            on<CourseDefined> { state, event ->
                val existing = state.courses[event.courseId]
                val row = CourseRow(event.courseId, event.title, event.capacity, existing?.enrolled ?: emptySet())
                state.copy(courses = state.courses + (event.courseId to row))
            }

            on<StudentRegistered> { state, event ->
                state.copy(students = state.students + (event.studentId to event.name))
            }

            on<CourseCancelled> { state, event ->
                state.copy(courses = state.courses - event.courseId)
            }

            on<StudentDeregistered> { state, event ->
                state.copy(students = state.students - event.studentId)
            }

            on<StudentEnrolledInCourse> { state, event ->
                val existing = state.courses[event.courseId] ?: return@on state
                state.copy(courses = state.courses + (event.courseId to existing.copy(enrolled = existing.enrolled + event.studentId)))
            }

            on<StudentUnenrolledFromCourse> { state, event ->
                val existing = state.courses[event.courseId] ?: return@on state
                state.copy(courses = state.courses + (event.courseId to existing.copy(enrolled = existing.enrolled - event.studentId)))
            }
        }
}
