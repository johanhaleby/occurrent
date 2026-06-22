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

import org.occurrent.application.service.blocking.dcb.TagGenerator
import org.occurrent.example.domain.courseenrollment.CourseDefined
import org.occurrent.example.domain.courseenrollment.CourseEnrollmentEvent
import org.occurrent.example.domain.courseenrollment.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.StudentRegistered
import org.occurrent.example.domain.courseenrollment.StudentUnenrolledFromCourse

/**
 * Assigns DCB tags to each event when it is appended. The tags decide which boundaries an event belongs to, and
 * therefore which events a later [CourseEnrollmentDcbQueries] query will see.
 *
 * TODO(human): implement the mapping. Guidance per event:
 *  - [CourseDefined]              -> the course boundary only:  setOf(CourseEnrollmentDcbTags.course(event.courseId))
 *  - [StudentRegistered]         -> the student boundary only: setOf(CourseEnrollmentDcbTags.student(event.studentId))
 *  - [StudentEnrolledInCourse]   -> BOTH boundaries:           setOf(course(event.courseId), student(event.studentId))
 *  - [StudentUnenrolledFromCourse] -> BOTH boundaries:         setOf(course(event.courseId), student(event.studentId))
 *
 * Tagging the enroll/unenroll events with BOTH the course and the student is the heart of the example: it is what lets a
 * single conditional append protect the course capacity AND the per-student limit in one atomic decision.
 */
internal class CourseEnrollmentEventTagGenerator : TagGenerator<CourseEnrollmentEvent> {
    override fun tags(event: CourseEnrollmentEvent): Set<String> = when (event) {
        is CourseDefined -> TODO("Tag with the course boundary, see the guidance above")
        is StudentRegistered -> TODO("Tag with the student boundary")
        is StudentEnrolledInCourse -> TODO("Tag with BOTH the course and the student boundary")
        is StudentUnenrolledFromCourse -> TODO("Tag with BOTH the course and the student boundary")
    }
}
