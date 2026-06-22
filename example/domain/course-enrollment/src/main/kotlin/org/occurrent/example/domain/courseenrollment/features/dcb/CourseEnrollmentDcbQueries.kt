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

package org.occurrent.example.domain.courseenrollment.features.dcb

import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.example.domain.courseenrollment.CourseId
import org.occurrent.example.domain.courseenrollment.StudentId

/**
 * The DCB queries that define the decision boundary for each command. A query is both the read filter (what the decider
 * folds its state from) and the consistency boundary (what a conditional append is checked against).
 *
 * TODO(human): build the queries below.
 */
internal object CourseEnrollmentDcbQueries {

    /**
     * The boundary for enrolling or unenrolling a student in a course. It must span TWO entities at once:
     *  - the course's events, to know the capacity and how many students are already enrolled, and
     *  - the student's events, to know the student exists, is not already enrolled here, and is under the course limit.
     *
     * TODO(human): return a [DcbQuery] that matches events tagged course(courseId) OR student(studentId). Hint:
     *   DcbQuery.anyOf(listOf(
     *       DcbQueryItem.tagsAllOf(listOf(CourseEnrollmentDcbTags.course(courseId))),
     *       DcbQueryItem.tagsAllOf(listOf(CourseEnrollmentDcbTags.student(studentId)))
     *   ))
     * (You can narrow each item to specific event types with DcbQueryItem.typeAndTagsAllOf if you want a tighter read.)
     */
    fun enrollmentDecisionContext(courseId: CourseId, studentId: StudentId): DcbQuery =
        TODO("Match events tagged course(courseId) OR student(studentId), see the guidance above")

    /** The boundary for defining a course (the course's own events). TODO(human): match events tagged course(courseId). */
    fun courseDecisionContext(courseId: CourseId): DcbQuery =
        TODO("Match events tagged course(courseId)")

    /** The boundary for registering a student (the student's own events). TODO(human): match events tagged student(studentId). */
    fun studentDecisionContext(studentId: StudentId): DcbQuery =
        TODO("Match events tagged student(studentId)")
}
