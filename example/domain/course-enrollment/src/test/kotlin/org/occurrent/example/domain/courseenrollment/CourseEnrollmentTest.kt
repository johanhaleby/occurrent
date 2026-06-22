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

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.DefineCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.EnrollStudent
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.RegisterStudent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer

/**
 * Test harness for the course-enrollment example. The MongoDB replica set required for DCB transactions is started by
 * Testcontainers and wired in via {@code @ServiceConnection}, so no local setup is needed.
 *
 * TODO(human): once the tag generator, queries, and decider are implemented, remove the {@code @Disabled} annotations
 * and flesh out the assertions. These two tests are the ones that prove DCB is doing its job.
 */
@SpringBootTest
@Testcontainers
class CourseEnrollmentTest {

    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val mongoDBContainer: MongoDBContainer =
            MongoDBContainer("mongo:" + (System.getProperty("test.mongo.version") ?: "7.0")).withReplicaSet()
    }

    @Autowired
    lateinit var defineCourse: DefineCourse

    @Autowired
    lateinit var registerStudent: RegisterStudent

    @Autowired
    lateinit var enrollStudent: EnrollStudent

    @Test
    @Disabled("TODO(human): enable once the decider and queries are implemented")
    fun `a course cannot be filled beyond its capacity`() {
        // TODO(human): define a course with capacity N, register N+1 students, enroll N of them, then assert the
        //  (N+1)-th enrollment is rejected. For the real DCB payoff, run the last two enrollments concurrently and
        //  assert exactly one wins.
    }

    @Test
    @Disabled("TODO(human): enable once the decider and queries are implemented")
    fun `a student cannot exceed the per-student course limit`() {
        // TODO(human): register one student, define MAX_COURSES_PER_STUDENT + 1 courses, enroll the student in each,
        //  and assert the final enrollment is rejected.
    }
}
