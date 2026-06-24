package org.occurrent.example.domain.courseenrollment.features.studentmanagement.model

import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.common.StudentId
import java.time.Instant
import java.util.*

sealed interface StudentEvent : DomainEvent

/** A student exists and may enroll in courses. Belongs to the student boundary. */
data class StudentRegistered(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val studentId: StudentId,
    val name: String
) : StudentEvent
