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

package org.occurrent.example.domain.hotelbooking

import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.springboot.mongo.reactor.EnableOccurrentReactive
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

/**
 * Spring Boot entry point on the reactive stack.
 *
 * [EnableOccurrentReactive] turns on the reactive Occurrent MongoDB starter. With the DCB capability enabled (see
 * src/main/resources/application.yml), the starter auto-configures a reactive
 * [org.occurrent.application.service.reactor.dcb.DcbApplicationService] and the reactive DCB query and subscription DSL
 * from the beans below.
 *
 * Unlike the blocking example there is no `@EnableRetry`: the reactive DcbApplicationService retries a lost conditional
 * append via Reactor's own retry rather than spring-retry, so spring-retry is not on the classpath at all.
 */
@SpringBootApplication
@EnableOccurrentReactive
class Bootstrap {

    @Bean
    fun hotelBookingCloudEventTypeMapper(): CloudEventTypeMapper<DomainEvent> = ReflectionCloudEventTypeMapper.qualified()
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
