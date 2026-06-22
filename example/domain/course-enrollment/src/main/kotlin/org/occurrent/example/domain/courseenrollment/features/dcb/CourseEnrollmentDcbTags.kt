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

import org.occurrent.example.domain.courseenrollment.CourseId
import org.occurrent.example.domain.courseenrollment.StudentId

/**
 * The two consistency boundaries in this domain. An event can belong to one or both of them, that is what makes the
 * enrollment decision a Dynamic Consistency Boundary spanning a course and a student at once.
 */
internal object CourseEnrollmentDcbTags {
    fun course(courseId: CourseId): String = "course:$courseId"
    fun student(studentId: StudentId): String = "student:$studentId"
}
