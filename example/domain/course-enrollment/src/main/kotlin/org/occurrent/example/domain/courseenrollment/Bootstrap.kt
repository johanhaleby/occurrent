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

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.jacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.TagGenerator
import org.occurrent.dsl.decider.Decider
import org.occurrent.example.domain.courseenrollment.features.dcb.CourseEnrollmentEventTagGenerator
import org.occurrent.example.domain.courseenrollment.features.enrollment.decider.CourseEnrollmentCommand
import org.occurrent.example.domain.courseenrollment.features.enrollment.decider.CourseEnrollmentState
import org.occurrent.example.domain.courseenrollment.features.enrollment.decider.courseEnrollmentDecider
import org.occurrent.springboot.mongo.blocking.EnableOccurrent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.retry.annotation.EnableRetry
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit.MILLIS

/**
 * Spring Boot entry point.
 *
 * [EnableOccurrent] turns on the Occurrent MongoDB starter. With the DCB capability enabled (see
 * src/main/resources/application.yml), the starter auto-configures a
 * [org.occurrent.application.service.blocking.dcb.DcbApplicationService] and the DCB query DSL from the beans below.
 *
 * Everything in this file is wired for you. The domain logic lives behind the TODOs in the feature packages.
 */
@SpringBootApplication
@EnableOccurrent
@EnableRetry
class Bootstrap {

    @Bean
    fun courseEnrollmentCloudEventTypeMapper(): CloudEventTypeMapper<CourseEnrollmentEvent> =
        ReflectionCloudEventTypeMapper.simple(CourseEnrollmentEvent::class.java)

    @Bean
    fun cloudEventConverter(
        objectMapper: ObjectMapper,
        typeMapper: CloudEventTypeMapper<CourseEnrollmentEvent>
    ): CloudEventConverter<CourseEnrollmentEvent> =
        jacksonCloudEventConverter(
            objectMapper = objectMapper,
            cloudEventSource = URI.create("urn:occurrent:course-enrollment"),
            typeMapper = typeMapper,
            timeMapper = { it.occurredAt.atOffset(UTC).truncatedTo(MILLIS) },
            subjectMapper = { it.eventId.toString() }
        )

    /** Required by the starter when the DCB capability is enabled: how each event maps to its DCB tags. */
    @Bean
    fun courseEnrollmentTagGenerator(): TagGenerator<CourseEnrollmentEvent> = CourseEnrollmentEventTagGenerator()

    @Bean
    fun courseEnrollmentDeciderBean(): Decider<CourseEnrollmentCommand, CourseEnrollmentState, CourseEnrollmentEvent> =
        courseEnrollmentDecider()
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
