package org.occurrent.example.domain.courseenrollment.features.studentmanagement.model

import org.occurrent.example.domain.courseenrollment.common.StudentId

internal object StudentTags {
    fun student(studentId: StudentId): String = "student:$studentId"
}