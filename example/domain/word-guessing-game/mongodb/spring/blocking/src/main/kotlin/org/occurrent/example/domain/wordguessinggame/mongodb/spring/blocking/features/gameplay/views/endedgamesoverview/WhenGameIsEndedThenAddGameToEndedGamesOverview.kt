/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.endedgamesoverview

import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.LostGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations

@Configuration
class WhenGameIsEndedThenAddGameToEndedGamesOverview {
    private val log = loggerFor<WhenGameIsEndedThenAddGameToEndedGamesOverview>()

    @Autowired
    private lateinit var subscriptions: Subscriptions<GameEvent>

    @Autowired
    private lateinit var mongo: MongoOperations

    @Autowired
    private lateinit var domainEventQueries: DomainEventQueries<GameEvent>

    @Bean
    fun whenGameIsEndedThenAddGameToEndedGamesOverviewPolicy() =
        subscriptions.subscribe<GameWasWon, GameWasLost>("WhenGameIsEndedThenAddGameToGameEndedOverview") { e ->
            log.info("${e::class.eventType()} - will update ended games overview")
            val gameId = e.gameId
            val gameWasStarted = domainEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))!!
            val endedGameOverview = when (e) {
                is GameWasWon -> WonGameOverview(
                    gameId,
                    gameWasStarted.category,
                    gameWasStarted.startedBy,
                    gameWasStarted.timestamp,
                    e.timestamp,
                    gameWasStarted.wordToGuess,
                    e.winnerId
                )
                is GameWasLost -> LostGameOverview(
                    gameId,
                    gameWasStarted.category,
                    gameWasStarted.startedBy,
                    gameWasStarted.timestamp,
                    e.timestamp,
                    gameWasStarted.wordToGuess
                )
                else -> throw IllegalStateException("Internal error")
            }.toDTO()
            mongo.insert(endedGameOverview)
        }
}