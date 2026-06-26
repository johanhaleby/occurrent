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
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases.defineCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.enrollStudent
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.usecases.registerStudent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Spring MVC integration tests for the course-enrollment web layer. The MongoDB replica-set required for DCB
 * transactions is started by Testcontainers and wired via {@code @ServiceConnection}, mirroring the setup in
 * {@link CourseEnrollmentTest}.
 *
 * The dashboard is eventually consistent (fed by a DCB subscription), so dashboard assertions use Awaitility.
 * The course-detail read model is strongly consistent, so detail assertions are immediate.
 */
@SpringBootTest
@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class CourseEnrollmentWebTest {

    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val mongoDBContainer: MongoDBContainer =
            MongoDBContainer("mongo:" + (System.getProperty("test.mongo.version") ?: "7.0")).withReplicaSet()
    }

    @Autowired
    lateinit var wac: WebApplicationContext

    @Autowired
    lateinit var applicationService: DcbApplicationService<DomainEvent>

    lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setUpMockMvc() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build()
    }

    @Test
    fun `dashboard eventually reflects a seeded course with full seat count`() {
        val courseId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Event Sourcing Fundamentals", capacity = 30)

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            val body = mockMvc.get("/dashboard").andExpect { status { isOk() } }.andReturn().response.contentAsString
            assertThat(body)
                .contains("Event Sourcing Fundamentals")
                .contains("30")   // capacity column
        }
    }

    @Test
    fun `dashboard eventually lists a registered student`() {
        val studentId = UUID.randomUUID()
        applicationService.registerStudent(studentId, name = "Grace Hopper")

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            val body = mockMvc.get("/dashboard").andExpect { status { isOk() } }.andReturn().response.contentAsString
            assertThat(body).contains("Grace Hopper")
        }
    }

    @Test
    fun `GET course detail returns strongly-consistent view with title and capacity`() {
        val courseId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Domain-Driven Design", capacity = 15)

        val body = mockMvc.get("/courses/$courseId").andExpect { status { isOk() } }.andReturn().response.contentAsString
        assertThat(body)
            .contains("Domain-Driven Design")
            .contains("15")
    }

    @Test
    fun `GET course detail shows enrolled student name after seeded enrollment`() {
        val courseId = UUID.randomUUID()
        val studentId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "CQRS Workshop", capacity = 10)
        applicationService.registerStudent(studentId, name = "Alice Wonderland")
        applicationService.enrollStudent(courseId, studentId)

        val body = mockMvc.get("/courses/$courseId").andExpect { status { isOk() } }.andReturn().response.contentAsString
        assertThat(body).contains("Alice Wonderland")
    }

    @Test
    fun `POST enroll then unenroll cycles a student through the course detail fragment`() {
        val courseId = UUID.randomUUID()
        val studentId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Hexagonal Architecture", capacity = 5)
        applicationService.registerStudent(studentId, name = "Bob Builder")

        // Enroll via the web endpoint; the returned fragment is strongly consistent.
        val enrollBody = mockMvc.post("/courses/$courseId/enrollments") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("studentId", studentId.toString())
        }.andExpect { status { isOk() } }.andReturn().response.contentAsString

        assertThat(enrollBody).contains("Bob Builder")

        // Unenroll via the web endpoint; the returned fragment reflects the removal.
        val unenrollBody = mockMvc.post("/courses/$courseId/unenrollments") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("studentId", studentId.toString())
        }.andExpect { status { isOk() } }.andReturn().response.contentAsString

        // After unenrollment the enrolled-students list is empty; the template emits the "nobody enrolled" paragraph.
        // Bob's name still appears in the dropdown (registered students list), so we assert on the sentinel text.
        assertThat(unenrollBody).contains("Nobody is enrolled yet.")
    }

    @Test
    fun `POST enroll to a full course returns 200 with an inline error fragment, not 500`() {
        val courseId = UUID.randomUUID()
        val firstStudentId = UUID.randomUUID()
        val secondStudentId = UUID.randomUUID()
        applicationService.defineCourse(courseId, title = "Capacity One", capacity = 1)
        applicationService.registerStudent(firstStudentId, name = "First Student")
        applicationService.registerStudent(secondStudentId, name = "Second Student")

        // Fill the single seat via the web layer.
        mockMvc.post("/courses/$courseId/enrollments") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("studentId", firstStudentId.toString())
        }.andExpect { status { isOk() } }

        // Attempt to over-enroll; must return 200 with an error message in the fragment, not a 500.
        val body = mockMvc.post("/courses/$courseId/enrollments") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("studentId", secondStudentId.toString())
        }.andExpect { status { isOk() } }.andReturn().response.contentAsString

        // The detail template renders the error via th:if="${message}" with class "feedback error". Assert the actual
        // rejection message, not just any occurrence of "enroll" (which the surrounding fragment always contains).
        assertThat(body)
            .contains("feedback error")
            .containsIgnoringCase("full")
    }

    @Test
    fun `POST courses and POST students return 200 with a feedback fragment`() {
        val courseBody = mockMvc.post("/courses") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("title", "New Course")
            param("capacity", "20")
        }.andExpect { status { isOk() } }.andReturn().response.contentAsString

        assertThat(courseBody).contains("New Course")

        val studentBody = mockMvc.post("/students") {
            contentType = MediaType.APPLICATION_FORM_URLENCODED
            param("name", "Carol Danvers")
        }.andExpect { status { isOk() } }.andReturn().response.contentAsString

        assertThat(studentBody).contains("Carol Danvers")
    }
}
