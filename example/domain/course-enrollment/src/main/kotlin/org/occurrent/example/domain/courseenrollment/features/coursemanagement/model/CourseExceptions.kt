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

package org.occurrent.example.domain.courseenrollment.features.coursemanagement.model

import org.occurrent.example.domain.courseenrollment.common.CourseId

// Explicit exceptions for the course-management domain rules. Each extends IllegalArgumentException, which is what the
// rules threw before, so existing catches and assertions keep working. The enrollment feature reuses CourseNotDefinedException.

class CourseAlreadyDefinedException(title: String) : IllegalArgumentException("Course $title is already defined")

class CourseCancelledCannotBeRedefinedException(courseId: CourseId) : IllegalArgumentException("Course $courseId was cancelled and cannot be redefined")

class CourseNotDefinedException(courseId: CourseId) : IllegalArgumentException("Course $courseId is not defined")

class CourseAlreadyCancelledException(courseId: CourseId) : IllegalArgumentException("Course $courseId is already cancelled")
