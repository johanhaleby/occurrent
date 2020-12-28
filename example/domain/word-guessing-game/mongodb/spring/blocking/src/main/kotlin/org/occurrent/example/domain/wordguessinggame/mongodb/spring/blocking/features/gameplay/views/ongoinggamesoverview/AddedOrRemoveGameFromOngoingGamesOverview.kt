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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.ongoinggamesoverview

import org.occurrent.application.subscription.dsl.blocking.Subscriptions
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria.where
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.remove

@Configuration
class AddedOrRemoveGameFromOngoingGamesOverview {
    private val log = loggerFor<AddedOrRemoveGameFromOngoingGamesOverview>()

    @Autowired
    lateinit var subscriptions: Subscriptions<GameEvent>

    @Autowired
    lateinit var mongo: MongoOperations

    @Bean
    fun whenGameWasStartedThenAddGameToOngoingGamesOverview() =
        subscriptions.subscribe<GameWasStarted>("WhenGameWasStartedThenAddGameToOngoingGamesOverview") { gameWasStarted ->
            log.info("Adding game ${gameWasStarted.gameId} to ongoing games view")
            val ongoingGameOverview = gameWasStarted.run {
                OngoingGameOverview(gameId, category, startedBy, timestamp).toDTO()
            }
            mongo.insert(ongoingGameOverview)
        }

    @Bean
    fun whenGameIsEndedThenRemoveGameFromOngoingGamesOverview() =
        subscriptions.subscribe<GameWasWon, GameWasLost>("WhenGameIsEndedThenRemoveGameFromOngoingGamesOverview") { e ->
            val gameId = when (e) {
                is GameWasWon -> e.gameId
                is GameWasLost -> e.gameId
                else -> throw IllegalStateException("Internal error")
            }
            log.info("Removing game $gameId from ongoing games view since ${e.type}")
            mongo.remove<OngoingGameOverviewMongoDTO>(query(where("_id").isEqualTo(gameId.toString())))
        }
}