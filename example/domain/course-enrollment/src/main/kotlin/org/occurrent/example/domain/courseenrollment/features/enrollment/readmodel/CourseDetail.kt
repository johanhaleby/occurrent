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

package org.occurrent.example.domain.courseenrollment.features.enrollment.readmodel

import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries
import org.springframework.stereotype.Component

data class EnrolledStudent(val studentId: StudentId, val name: String)

data class CourseDetailView(val courseId: CourseId, val title: String, val capacity: Int, val enrolledStudents: List<EnrolledStudent>) {
    val seatsRemaining: Int get() = capacity - enrolledStudents.size
}

/**
 * Builds the course-detail read model on demand by querying the event store, so it is strongly consistent with the last
 * write rather than eventually consistent like the dashboard. This is the [DcbDomainEventQueries] selling point.
 */
@Component
class CourseDetail(private val queries: DcbDomainEventQueries<DomainEvent>) {

    fun of(courseId: CourseId): CourseDetailView? {
        var title: String? = null
        var capacity = 0
        val enrolled = LinkedHashSet<StudentId>()
        // The course tag scopes the read to this course's own events (definition plus enrollments), not the students'.
        queries.query(CourseEnrollmentDcbQueries.courseDecisionContext(courseId)).use { events ->
            events.forEach { event ->
                when (event) {
                    is CourseDefined -> {
                        title = event.title
                        capacity = event.capacity
                    }

                    is StudentEnrolledInCourse -> enrolled.add(event.studentId)
                    is StudentUnenrolledFromCourse -> enrolled.remove(event.studentId)
                    else -> {
                    }
                }
            }
        }
        val courseTitle = title ?: return null
        // Resolve names with a consistent read per enrolled student, since names live on the student boundary.
        val students = enrolled.map { studentId -> EnrolledStudent(studentId, nameOf(studentId)) }
        return CourseDetailView(courseId, courseTitle, capacity, students)
    }

    private fun nameOf(studentId: StudentId): String =
        queries.query(CourseEnrollmentDcbQueries.studentDecisionContext(studentId)).use { events ->
            events.filter { it is StudentRegistered }.map { (it as StudentRegistered).name }.findFirst().orElse(studentId.toString())
        }
}
