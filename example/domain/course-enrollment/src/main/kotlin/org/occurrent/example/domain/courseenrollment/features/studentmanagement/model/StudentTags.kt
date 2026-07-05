package org.occurrent.example.domain.courseenrollment.features.studentmanagement.model

import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.courseenrollment.common.StudentId

internal object StudentTags {
    fun student(studentId: StudentId): Tag = Tag.of("student", studentId.toString())
}