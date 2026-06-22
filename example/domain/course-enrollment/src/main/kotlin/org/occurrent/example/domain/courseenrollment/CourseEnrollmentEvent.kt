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

package org.occurrent.example.domain.courseenrollment

import java.time.Instant
import java.util.UUID
import kotlin.reflect.KClass

typealias CourseId = UUID
typealias StudentId = UUID

/**
 * The events of the course-enrollment domain. These are fully defined, the rest of the example (tags, queries, decider,
 * use cases) is left for you to implement, see the module README and the TODOs.
 *
 * The point of the example is the enrollment decision, which must hold two invariants that live on two different
 * entities at the same time: the course capacity and the per-student course limit. That is exactly what a Dynamic
 * Consistency Boundary lets you do without a saga.
 */
sealed interface CourseEnrollmentEvent {
    val eventId: UUID
    val occurredAt: Instant
}

/** A course is offered with a fixed number of seats. Belongs to the course boundary. */
data class CourseDefined(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val courseId: CourseId,
    val title: String,
    val capacity: Int
) : CourseEnrollmentEvent

/** A student exists and may enroll in courses. Belongs to the student boundary. */
data class StudentRegistered(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val studentId: StudentId,
    val name: String
) : CourseEnrollmentEvent

/** A student took a seat in a course. Belongs to BOTH the course and the student boundary (the cross-entity move). */
data class StudentEnrolledInCourse(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val courseId: CourseId,
    val studentId: StudentId
) : CourseEnrollmentEvent

/** A student gave up their seat. Belongs to BOTH the course and the student boundary. */
data class StudentUnenrolledFromCourse(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val courseId: CourseId,
    val studentId: StudentId
) : CourseEnrollmentEvent

/** Domain policy constants. */
object EnrollmentPolicy {
    /** The maximum number of courses a single student may be enrolled in at once. */
    const val MAX_COURSES_PER_STUDENT: Int = 10
}

/**
 * The CloudEvent type string for an event class. Matches {@code ReflectionCloudEventTypeMapper.simple}, which is wired
 * in [org.occurrent.example.domain.courseenrollment.Bootstrap]. Handy when building type-scoped DCB query items.
 */
fun KClass<out CourseEnrollmentEvent>.eventType(): String =
    simpleName ?: error("Anonymous event classes are not supported")
