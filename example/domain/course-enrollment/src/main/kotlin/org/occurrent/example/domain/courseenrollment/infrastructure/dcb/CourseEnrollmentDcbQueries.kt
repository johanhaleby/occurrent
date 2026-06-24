package org.occurrent.example.domain.courseenrollment.infrastructure.dcb

import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.eventstore.api.dcb.DcbQueryItem
import org.occurrent.example.domain.courseenrollment.common.CourseId
import org.occurrent.example.domain.courseenrollment.common.StudentId
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseTags
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentTags

/**
 * The DCB queries that define the decision boundary for each command. A query is both the read filter (what the decider
 * folds its state from) and the consistency boundary (what a conditional append is checked against).
 */
internal object CourseEnrollmentDcbQueries {

    /**
     * The boundary for enrolling or unenrolling a student in a course. It must span TWO entities at once:
     *  - the course's events, to know the capacity and how many students are already enrolled, and
     *  - the student's events, to know the student exists, is not already enrolled here, and is under the course limit.
     */
    fun enrollmentDecisionContext(courseId: CourseId, studentId: StudentId): DcbQuery =
        DcbQuery.anyOf(listOf(DcbQueryItem.tagsAllOf(listOf(CourseTags.course(courseId))), DcbQueryItem.tagsAllOf(listOf(StudentTags.student(studentId)))))

    /** The boundary for defining a course (the course's own events). */
    fun courseDecisionContext(courseId: CourseId): DcbQuery =
        DcbQuery.tagsAllOf(CourseTags.course(courseId))

    /** The boundary for registering a student (the student's own events). */
    fun studentDecisionContext(studentId: StudentId): DcbQuery =
        DcbQuery.tagsAllOf(StudentTags.student(studentId))
}