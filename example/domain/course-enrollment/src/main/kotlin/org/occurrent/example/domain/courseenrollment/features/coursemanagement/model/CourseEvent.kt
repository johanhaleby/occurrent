package org.occurrent.example.domain.courseenrollment.features.coursemanagement.model

import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.CourseId
import java.time.Instant
import java.util.*

sealed interface CourseEvent : DomainEvent

/** A course is offered with a fixed number of seats. Belongs to the course boundary. */
data class CourseDefined(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val courseId: CourseId,
    val title: String,
    val capacity: Int
) : CourseEvent

/** A course is no longer offered. Belongs to the course boundary. */
data class CourseCancelled(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val courseId: CourseId
) : CourseEvent
