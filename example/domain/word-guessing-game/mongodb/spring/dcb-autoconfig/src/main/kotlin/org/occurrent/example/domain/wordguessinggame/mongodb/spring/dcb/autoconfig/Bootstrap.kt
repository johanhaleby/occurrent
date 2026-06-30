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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.jacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.dsl.decider.Decider
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameEventTagGenerator
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameCommand
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameState
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.wordGuessingGameDecider as createWordGuessingGameDecider
import org.occurrent.springboot.mongo.blocking.EnableOccurrent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType
import org.springframework.retry.annotation.EnableRetry
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit.MILLIS

@SpringBootApplication
@ComponentScan(
    excludeFilters = [
        ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = [
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.gameplay\\.website\\..*"
            ]
        )
    ]
)
@EnableOccurrent
@EnableRetry
class Bootstrap {

    @Bean
    fun gameEventCloudEventTypeMapper(): CloudEventTypeMapper<GameEvent> = ReflectionCloudEventTypeMapper.simple(GameEvent::class.java)

    @Bean
    fun cloudEventConverter(objectMapper: ObjectMapper, typeMapper: CloudEventTypeMapper<GameEvent>): CloudEventConverter<GameEvent> =
        jacksonCloudEventConverter(
            objectMapper = objectMapper,
            cloudEventSource = URI.create("urn:occurrent:word-guessing-game"),
            typeMapper = typeMapper,
            timeMapper = { it.timestamp.toInstant().atOffset(UTC).truncatedTo(MILLIS) },
            subjectMapper = { it.gameId.toString() }
        )

    @Bean
    fun gameEventTagGenerator(): TagGenerator<GameEvent> = GameEventTagGenerator()

    // DcbDomainEventQueries is auto-configured by the starter when the DCB capability is enabled.

    @Bean
    fun wordGuessingGameDecider(): Decider<WordGuessingGameCommand, WordGuessingGameState, GameEvent> =
        createWordGuessingGameDecider()
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
