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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases.cancelCourse
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases.defineCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentPolicy.MAX_COURSES_PER_STUDENT
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.enrollStudent
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.usecases.deregisterStudent
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.usecases.registerStudent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

/**
 * Test harness for the course-enrollment example. The MongoDB replica set required for DCB transactions is started by
 * Testcontainers and wired in via {@code @ServiceConnection}, so no local setup is needed.
 *
 * The use cases are extension functions on [DcbApplicationService], so the test autowires the service and calls them
 * directly, for example {@code applicationService.defineCourse(...)}.
 */
@SpringBootTest
@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class CourseEnrollmentTest {

    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val mongoDBContainer: MongoDBContainer = MongoDBContainer("mongo:" + (System.getProperty("test.mongo.version") ?: "7.0")).withReplicaSet()
    }

    @Autowired
    lateinit var applicationService: DcbApplicationService<DomainEvent>

    @Test
    fun `a course cannot be filled beyond its capacity`() {
        // Given a course with two seats and three registered students
        val courseId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Domain-Driven Design", capacity = 2)
        val students = List(3) { UUID.randomUUID() }
        students.forEach { applicationService.registerStudent(it, name = "Student $it") }

        // When the two seats are taken
        applicationService.enrollStudent(courseId, students[0])
        applicationService.enrollStudent(courseId, students[1])

        // Then a third enrollment is rejected because the course is full
        assertThatThrownBy { applicationService.enrollStudent(courseId, students[2]) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("full")
    }

    @Test
    fun `a student cannot exceed the per-student course limit`() {
        // Given a registered student and one more course than the per-student limit allows
        val studentId = UUID.randomUUID()
        applicationService.registerStudent(studentId, name = "Jane Doe")
        val courses = List(MAX_COURSES_PER_STUDENT + 1) { UUID.randomUUID() }
        courses.forEach { applicationService.defineCourse(it, title = "Course $it", capacity = 5) }

        // When the student fills up to the limit
        courses.take(MAX_COURSES_PER_STUDENT).forEach { applicationService.enrollStudent(it, studentId) }

        // Then enrolling in one more course is rejected
        assertThatThrownBy { applicationService.enrollStudent(courses.last(), studentId) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("courses")
    }

    @Test
    fun `a cancelled course cannot be enrolled into`() {
        val courseId = UUID.randomUUID()
        val studentId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Soon Cancelled", capacity = 5)
        applicationService.registerStudent(studentId, name = "Student")
        applicationService.cancelCourse(courseId)

        assertThatThrownBy { applicationService.enrollStudent(courseId, studentId) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("cancelled")
    }

    @Test
    fun `a deregistered student cannot be enrolled`() {
        val courseId = UUID.randomUUID()
        val studentId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Open Course", capacity = 5)
        applicationService.registerStudent(studentId, name = "Leaving Student")
        applicationService.deregisterStudent(studentId)

        assertThatThrownBy { applicationService.enrollStudent(courseId, studentId) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("deregistered")
    }

    @Test
    fun `two students racing for the last seat cannot both get it`() {
        // Given a course with a single seat and two registered students
        val courseId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Event Sourcing", capacity = 1)
        val first = UUID.randomUUID().also { applicationService.registerStudent(it, name = "First") }
        val second = UUID.randomUUID().also { applicationService.registerStudent(it, name = "Second") }

        // When both try to take the single seat at the same time. This is the real Dynamic Consistency Boundary payoff:
        // the conditional append over the course boundary lets exactly one succeed even under concurrency.
        val pool = Executors.newFixedThreadPool(2)
        val startLine = CountDownLatch(1)
        val attempts = listOf(first, second).map { studentId ->
            pool.submit(Callable {
                startLine.await()
                runCatching { applicationService.enrollStudent(courseId, studentId) }
            })
        }
        startLine.countDown()
        val outcomes = attempts.map { it.get() }
        pool.shutdown()

        // Then exactly one wins and the other is rejected because the course is full
        val winners = outcomes.filter { it.isSuccess }
        val losers = outcomes.filter { it.isFailure }
        assertThat(winners).hasSize(1)
        assertThat(winners.single().getOrNull()).describedAs("the winning enrollment should produce an append result").isNotNull()
        assertThat(losers).hasSize(1)
        assertThat(losers.single().exceptionOrNull()).isInstanceOf(IllegalArgumentException::class.java)
    }
}
