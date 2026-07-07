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

package org.occurrent.dsl.dcb

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.dsl.decider.decider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag

@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbDeciderTest {

    // ---- criteria/tags from a single DcbDecider ----

    @Test
    fun criteria_is_derived_from_the_command() {
        val dcbDecider = courseDecider().toDcb(
            criteria = { command -> DcbCriteria.tags(Tag.of("course", command.courseId)) },
            tags = { event -> setOf(Tag.of("course", event.courseId)) }
        )

        val criteria = dcbDecider.criteria().apply(RegisterCourse("course-1"))

        assertThat(criteria).isEqualTo(DcbCriteria.tags(Tag.of("course", "course-1")))
    }

    @Test
    fun tags_are_applied_to_emitted_events() {
        val dcbDecider = courseDecider().toDcb(
            criteria = { command -> DcbCriteria.tags(Tag.of("course", command.courseId)) },
            tags = { event -> setOf(Tag.of("course", event.courseId)) }
        )

        val events = dcbDecider.decider().decide(RegisterCourse("course-1"), false)

        assertThat(events).containsExactly(CourseRegistered("course-1"))
        assertThat(dcbDecider.tags().tags(events.single())).containsExactly(Tag.of("course", "course-1"))
    }

    // ---- adapt ----

    @Test
    fun adapt_returns_null_criteria_and_empty_tags_for_a_foreign_command_or_event() {
        val courseDcbDecider = courseDecider().toDcb(
            criteria = { command -> DcbCriteria.tags(Tag.of("course", command.courseId)) },
            tags = { event -> setOf(Tag.of("course", event.courseId)) }
        )
        val widened: DcbDecider<SchoolCommand, Boolean, SchoolEvent> =
            DcbDecider.adapt(courseDcbDecider, RegisterCourse::class.java, CourseRegistered::class.java)

        assertThat(widened.criteria().apply(EnrollStudent("student-1"))).isNull()
        assertThat(widened.tags().tags(StudentEnrolled("student-1"))).isEmpty()

        // And it still recognizes its own command/event once widened.
        assertThat(widened.criteria().apply(RegisterCourse("course-1"))).isEqualTo(DcbCriteria.tags(Tag.of("course", "course-1")))
        assertThat(widened.tags().tags(CourseRegistered("course-1"))).containsExactly(Tag.of("course", "course-1"))
    }

    // ---- compose ----

    @Test
    fun compose_of_two_independent_deciders_routes_criteria_and_tags_to_the_recognizing_child_and_exposes_both_slices() {
        val courseDcbDecider: DcbDecider<SchoolCommand, Boolean, SchoolEvent> = DcbDecider.adapt(
            courseDecider().toDcb(
                criteria = { command -> DcbCriteria.tags(Tag.of("course", command.courseId)) },
                tags = { event -> setOf(Tag.of("course", event.courseId)) }
            ),
            RegisterCourse::class.java,
            CourseRegistered::class.java
        )
        val studentDcbDecider: DcbDecider<SchoolCommand, Boolean, SchoolEvent> = DcbDecider.adapt(
            studentDecider().toDcb(
                criteria = { command -> DcbCriteria.tags(Tag.of("student", command.studentId)) },
                tags = { event -> setOf(Tag.of("student", event.studentId)) }
            ),
            EnrollStudent::class.java,
            StudentEnrolled::class.java
        )

        val composed = DcbDecider.compose(listOf(courseDcbDecider, studentDcbDecider))

        // Criteria for a command recognized only by the course decider is that decider's own boundary.
        assertThat(composed.criteria().apply(RegisterCourse("course-1")))
            .isEqualTo(DcbCriteria.tags(Tag.of("course", "course-1")))
        // Criteria for a command recognized only by the student decider is that decider's own boundary.
        assertThat(composed.criteria().apply(EnrollStudent("student-1")))
            .isEqualTo(DcbCriteria.tags(Tag.of("student", "student-1")))

        // Tags union: an event recognized by only one child is tagged by that child alone.
        assertThat(composed.tags().tags(CourseRegistered("course-1"))).containsExactly(Tag.of("course", "course-1"))
        assertThat(composed.tags().tags(StudentEnrolled("student-1"))).containsExactly(Tag.of("student", "student-1"))

        // The composed state exposes both slices.
        val state = composed.decider().decideOnEvents(
            listOf(CourseRegistered("course-1"), StudentEnrolled("student-1")),
            emptyList<SchoolCommand>()
        ).state
        assertThat(state.slice<Boolean>(0)).isTrue()
        assertThat(state.slice<Boolean>(1)).isTrue()
    }

    // ---- fixtures ----

    private sealed interface SchoolCommand
    private sealed interface SchoolEvent

    private data class RegisterCourse(val courseId: String) : SchoolCommand
    private data class CourseRegistered(val courseId: String) : SchoolEvent

    private data class EnrollStudent(val studentId: String) : SchoolCommand
    private data class StudentEnrolled(val studentId: String) : SchoolEvent

    private fun courseDecider() = decider(
        initialState = false,
        decide = { command: RegisterCourse, _: Boolean -> listOf(CourseRegistered(command.courseId)) },
        evolve = { _: Boolean, _: CourseRegistered -> true }
    )

    private fun studentDecider() = decider(
        initialState = false,
        decide = { command: EnrollStudent, _: Boolean -> listOf(StudentEnrolled(command.studentId)) },
        evolve = { _: Boolean, _: StudentEnrolled -> true }
    )
}
