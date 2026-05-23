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
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.application.service.blocking.dcb.TagGenerator
import org.occurrent.dsl.decider.Decider
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.GameCloudEventConverter
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
import org.springframework.context.annotation.Primary
import org.springframework.retry.annotation.EnableRetry
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI

@SpringBootApplication
@ComponentScan(
    excludeFilters = [
        ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = [
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.gameplay\\.views\\..*",
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.gameplay\\.website\\..*",
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.wordhint\\..*",
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.pointawarding\\..*",
                "org\\.occurrent\\.example\\.domain\\.wordguessinggame\\.mongodb\\.spring\\.dcb\\.autoconfig\\.features\\.emailwinner\\..*"
            ]
        )
    ]
)
@EnableOccurrent
@EnableRetry
class Bootstrap {

    @Bean
    @Primary
    fun cloudEventConverter(): CloudEventConverter<GameEvent> =
        GameCloudEventConverter(
            objectMapper = jacksonObjectMapper(),
            gameSource = URI.create("urn:occurrent:word-guessing-game:game"),
            wordHintSource = URI.create("urn:occurrent:word-guessing-game:word-hint"),
            pointsSource = URI.create("urn:occurrent:word-guessing-game:points")
        )

    @Bean
    fun gameEventTagGenerator(): TagGenerator<GameEvent> = GameEventTagGenerator()

    @Bean
    fun dcbApplicationService(
        dcbEventStore: DcbEventStore,
        cloudEventConverter: CloudEventConverter<GameEvent>,
        gameEventTagGenerator: TagGenerator<GameEvent>
    ): DcbApplicationService<GameEvent> =
        GenericDcbApplicationService(dcbEventStore, cloudEventConverter, gameEventTagGenerator)

    @Bean
    fun wordGuessingGameDecider(): Decider<WordGuessingGameCommand, WordGuessingGameState, GameEvent> =
        createWordGuessingGameDecider()
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
