package org.occurrent.example.domain.courseenrollment.infrastructure.dcb

import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseCancelled
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseDefined
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseTags
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentDeregistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentRegistered
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentTags

/**
 * Assigns DCB tags to each event when it is appended. The tags decide which boundaries an event belongs to, and
 * therefore which events a later [CourseEnrollmentDcbQueries] query will see.
 *
 * Tagging the enroll/unenroll events with BOTH the course and the student is the heart of the example: it is what lets a
 * single conditional append protect the course capacity AND the per-student limit in one atomic decision.
 */
internal class CourseEnrollmentEventTagGenerator : TagGenerator<DomainEvent> {
    override fun tags(event: DomainEvent): Set<String> = when (event) {
        is CourseDefined -> setOf(CourseTags.course(event.courseId))
        is CourseCancelled -> setOf(CourseTags.course(event.courseId))
        is StudentRegistered -> setOf(StudentTags.student(event.studentId))
        is StudentDeregistered -> setOf(StudentTags.student(event.studentId))
        is StudentEnrolledInCourse -> setOf(CourseTags.course(event.courseId), StudentTags.student(event.studentId))
        is StudentUnenrolledFromCourse -> setOf(CourseTags.course(event.courseId), StudentTags.student(event.studentId))
        else -> error("No DCB tags defined for event ${event::class.simpleName}. Every event must be tagged so the right decision boundary can find it.")
    }
}