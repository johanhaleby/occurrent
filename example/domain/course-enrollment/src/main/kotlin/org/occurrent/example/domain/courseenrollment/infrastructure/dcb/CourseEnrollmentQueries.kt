/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.courseenrollment.infrastructure.dcb

import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseTags
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentTags

/**
 * The DCB queries that define the decision boundary for each command. A query is both the read filter (what the decider
 * folds its state from) and the consistency boundary (what a conditional append is checked against).
 */
internal object CourseEnrollmentQueries {

    /**
     * The boundary for enrolling or unenrolling a student in a course. It must span TWO entities at once:
     *  - the course's events, to know the capacity and how many students are already enrolled, and
     *  - the student's events, to know the student exists, is not already enrolled here, and is under the course limit.
     */
    fun enrollmentCriteria(courseId: CourseId, studentId: StudentId): DcbCriteria =
        DcbCriteria.tagsAnyOf(CourseTags.course(courseId), StudentTags.student(studentId))

    /** The boundary for defining a course (the course's own events). */
    fun courseBoundary(courseId: CourseId): DcbCriteria =
        DcbCriteria.tags(CourseTags.course(courseId))

    /** The boundary for registering a student (the student's own events). */
    fun studentCriteria(studentId: StudentId): DcbCriteria =
        DcbCriteria.tags(StudentTags.student(studentId))
}