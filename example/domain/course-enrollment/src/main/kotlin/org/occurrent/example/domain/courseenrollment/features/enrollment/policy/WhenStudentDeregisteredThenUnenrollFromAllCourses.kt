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

package org.occurrent.example.domain.courseenrollment.features.enrollment.policy

import org.occurrent.annotation.DcbSubscription
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.queryForSequence
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentNotEnrolledInCourseException
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.unenrollStudent
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentTags
import org.springframework.stereotype.Component

/**
 * Policy: when a student is deregistered, unenroll them from every course they are still enrolled in.
 *
 * Without this, deregistration leaves the enrollments behind: courses keep listing the student and their seats stay
 * taken. The policy is eventually consistent.
 */
@Component
class WhenStudentDeregisteredThenUnenrollFromAllCourses(
    private val applicationService: DcbApplicationService<DomainEvent>,
    private val queries: DcbDomainEventQueries<DomainEvent>
) {

    @DcbSubscription(id = "WhenStudentDeregisteredThenUnenrollFromAllCoursesPolicy")
    fun whenStudentDeregistered(event: StudentDeregistered) {
        coursesEnrolledBy(event.studentId).forEach { courseId ->
            try {
                applicationService.unenrollStudent(courseId, event.studentId)
            } catch (alreadyUnenrolled: StudentNotEnrolledInCourseException) {
                // A concurrent unenroll can win the race; the decider then rejects this one, which is the state we want.
            }
        }
    }

    private fun coursesEnrolledBy(studentId: StudentId): Set<CourseId> {
        // Scope the read to the student's enrollment events server-side (event types AND tag), so the fold sees only them.
        val enrollments = queries.criteria()
            .types<StudentEnrolledInCourse, StudentUnenrolledFromCourse>()
            .tags(StudentTags.student(studentId))
        return queries.queryForSequence(enrollments)
            .fold(emptySet<CourseId>()) { enrolled, event ->
                when (event) {
                    is StudentEnrolledInCourse -> enrolled + event.courseId
                    is StudentUnenrolledFromCourse -> enrolled - event.courseId
                    else -> enrolled
                }
            }
    }
}
