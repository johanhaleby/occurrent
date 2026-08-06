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

import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.atomic.AtomicReference

/** A course as shown on the dashboard. Enrolled students are a set so replay stays idempotent and order-tolerant. */
data class CourseRow(val courseId: CourseId, val title: String, val capacity: Int, val enrolled: Set<StudentId>) {
    @Suppress("unused")
    val enrolledCount: Int get() = enrolled.size // Used by thymeleaf
    @Suppress("unused")
    val seatsRemaining: Int get() = capacity - enrolled.size // Used by thymeleaf
}

data class DashboardState(val courses: Map<CourseId, CourseRow>, val students: Map<StudentId, String>) {
    companion object {
        val EMPTY = DashboardState(emptyMap(), emptyMap())
    }
}

/** The single global key the dashboard is stored under. There is no per-entity id: the whole module has one dashboard. */
const val COURSE_DASHBOARD_ID = "course-dashboard"

/**
 * An in-memory read model of all courses and students, kept current by the `course-dashboard` [org.occurrent.annotation.Projection]
 * (see [CourseDashboardProjection]). It doubles as that projection's [ViewStateRepository]: `findById`/`save`
 * read and write the same single-slot [AtomicReference] the query accessors below read from, so there is no separate store
 * to keep in sync. It is eventually consistent with the event store. For a strongly consistent read see the course-detail
 * read model in the enrollment feature.
 */
@Component
class CourseDashboard : ViewStateRepository<DashboardState, String> {

    private val slot = AtomicReference(DashboardState.EMPTY)

    // This is a single-instance store, so it only serves COURSE_DASHBOARD_ID. Rejecting any other id surfaces a
    // misconfigured projection (an id function returning the wrong key) at once rather than silently losing writes.
    override fun findById(id: String): Optional<DashboardState> =
        if (id == COURSE_DASHBOARD_ID) Optional.of(slot.get()) else Optional.empty()

    override fun save(id: String, state: DashboardState) {
        require(id == COURSE_DASHBOARD_ID) { "This dashboard store only serves '$COURSE_DASHBOARD_ID', got '$id'" }
        slot.set(state)
    }

    fun courses(): List<CourseRow> = slot.get().courses.values.sortedBy { it.title }

    fun students(): List<RegisteredStudent> =
        slot.get().students.entries.map { RegisteredStudent(it.key, it.value) }.sortedBy { it.name }

    fun studentName(studentId: StudentId): String? = slot.get().students[studentId]
}

data class RegisteredStudent(val studentId: StudentId, val name: String)
