/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.decidermodel.web

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.example.domain.rps.decidermodel.GameEvent
import org.occurrent.springboot.mongo.blocking.EnableOccurrent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories
import java.net.URI
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit.MILLIS


@SpringBootApplication
@EnableMongoRepositories
@EnableOccurrent
class Bootstrap {
    @Bean
    fun cloudEventConverter() = jacksonCloudEventConverter<GameEvent>(
        objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule()),
        cloudEventSource = URI.create("urn:occurrent:rps"),
        timeMapper = { e -> e.timestamp.toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC).truncatedTo(MILLIS) },
        subjectMapper = { e -> e.gameId.toString() },
        typeMapper = ReflectionCloudEventTypeMapper.simple(GameEvent::class.java)
    )
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}