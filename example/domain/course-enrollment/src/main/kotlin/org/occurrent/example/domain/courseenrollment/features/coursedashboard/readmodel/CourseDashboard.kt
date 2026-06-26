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

import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.View
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCancelled
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicReference

/** A course as shown on the dashboard. Enrolled students are a set so replay stays idempotent and order-tolerant. */
data class CourseRow(val courseId: CourseId, val title: String, val capacity: Int, val enrolled: Set<StudentId>) {
    val enrolledCount: Int get() = enrolled.size
    val seatsRemaining: Int get() = capacity - enrolled.size
}

data class DashboardState(val courses: Map<CourseId, CourseRow>, val students: Map<StudentId, String>) {
    companion object {
        val EMPTY = DashboardState(emptyMap(), emptyMap())
    }
}

/**
 * An in-memory read model of all courses and students, kept current by a DCB subscription (see
 * [CourseDashboardSubscriber]). It is eventually consistent with the event store, which is exactly what a materialized
 * view is. For a strongly consistent read see the course-detail read model in the enrollment feature.
 */
@Component
class CourseDashboard {

    private val slot = AtomicReference(DashboardState.EMPTY)

    private val repository: ViewStateRepository<DashboardState, String> =
        ViewStateRepository.create({ slot.get() }, { _, state -> slot.set(state) })

    private val view: View<DashboardState, DomainEvent> =
        View.create(DashboardState.EMPTY) { state, event -> evolve(state ?: DashboardState.EMPTY, event) }

    private val materializedView: MaterializedView<DomainEvent> =
        MaterializedView.create({ _ -> KEY }, view, repository)

    // update is a read-modify-write over the single read-model slot. DCB subscription delivery is single-threaded
    // today, so this synchronization only guards the slot against a future change to the delivery threading.
    @Synchronized
    fun update(event: DomainEvent) = materializedView.update(event)

    fun courses(): List<CourseRow> = slot.get().courses.values.sortedBy { it.title }

    fun students(): List<RegisteredStudent> =
        slot.get().students.entries.map { RegisteredStudent(it.key, it.value) }.sortedBy { it.name }

    fun studentName(studentId: StudentId): String? = slot.get().students[studentId]

    private fun evolve(state: DashboardState, event: DomainEvent): DashboardState = when (event) {
        is CourseDefined -> {
            val existing = state.courses[event.courseId]
            val row = CourseRow(event.courseId, event.title, event.capacity, existing?.enrolled ?: emptySet())
            state.copy(courses = state.courses + (event.courseId to row))
        }

        is StudentRegistered -> state.copy(students = state.students + (event.studentId to event.name))

        is CourseCancelled -> state.copy(courses = state.courses - event.courseId)

        is StudentDeregistered -> state.copy(students = state.students - event.studentId)

        is StudentEnrolledInCourse -> {
            val existing = state.courses[event.courseId] ?: return state
            state.copy(courses = state.courses + (event.courseId to existing.copy(enrolled = existing.enrolled + event.studentId)))
        }

        is StudentUnenrolledFromCourse -> {
            val existing = state.courses[event.courseId] ?: return state
            state.copy(courses = state.courses + (event.courseId to existing.copy(enrolled = existing.enrolled - event.studentId)))
        }

        else -> state
    }

    companion object {
        private const val KEY = "course-dashboard"
    }
}

data class RegisteredStudent(val studentId: StudentId, val name: String)
