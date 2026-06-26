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

import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.TagGenerator
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentEventTagGenerator
import org.occurrent.springboot.mongo.blocking.EnableOccurrent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.retry.annotation.EnableRetry

/**
 * Spring Boot entry point.
 *
 * [EnableOccurrent] turns on the Occurrent MongoDB starter. With the DCB capability enabled (see
 * src/main/resources/application.yml), the starter auto-configures a
 * [org.occurrent.application.service.blocking.dcb.DcbApplicationService] and the DCB query DSL from the beans below.
 *
 */
@SpringBootApplication
@EnableOccurrent
@EnableRetry
class Bootstrap {

    @Bean
    fun courseEnrollmentCloudEventTypeMapper(): CloudEventTypeMapper<DomainEvent> = ReflectionCloudEventTypeMapper.qualified()

    /** Required by the starter when the DCB capability is enabled: how each event maps to its DCB tags. */
    @Bean
    fun courseEnrollmentTagGenerator(): TagGenerator<DomainEvent> = CourseEnrollmentEventTagGenerator()
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
