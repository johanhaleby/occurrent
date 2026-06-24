package org.occurrent.example.domain.courseenrollment.features.enrollment.model

import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import java.time.Instant
import java.util.*

/**
 * The events of the course-enrollment domain. These are fully defined, the rest of the example (tags, queries, decider,
 * use cases) is left for you to implement, see the module README and the TODOs.
 *
 * The point of the example is the enrollment decision, which must hold two invariants that live on two different
 * entities at the same time: the course capacity and the per-student course limit. That is exactly what a Dynamic
 * Consistency Boundary lets you do without a saga.
 */
sealed interface CourseEnrollmentEvent : DomainEvent

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