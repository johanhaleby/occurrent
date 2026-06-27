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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.views.endedgamesoverview

import org.occurrent.annotation.StreamSubscription
import org.occurrent.dsl.dcb.blocking.dcbPosition
import org.occurrent.dsl.dcb.blocking.dcbTags
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.queryForSequence
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.support.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.LostGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class WhenGameIsEndedThenAddGameToEndedGamesOverview(
    private val mongo: MongoOperations,
    private val domainEventQueries: DcbDomainEventQueries<GameEvent>
) {
    private val log = loggerFor<WhenGameIsEndedThenAddGameToEndedGamesOverview>()

    @StreamSubscription(id = "WhenGameIsEndedThenAddGameToGameEndedOverview", eventTypes = [GameWasWon::class, GameWasLost::class])
    fun whenGameIsEndedThenAddGameToEndedGamesOverviewPolicy(e: GameEvent, metadata: EventMetadata) {
        if (!metadata.belongsToGame(e.gameId)) {
            return
        }
        requireNotNull(metadata.dcbPosition) { "Expected DCB position for ${e.type}" }
        log.info("${e::class.eventType()} - will update ended games overview")
        val gameId = e.gameId
        val gameWasStarted = domainEventQueries
            .queryForSequence(GameDcbQueries.event<GameWasStarted>(gameId))
            .filterIsInstance<GameWasStarted>()
            .first()
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
        mongo.save(endedGameOverview)
    }

    private fun EventMetadata.belongsToGame(gameId: UUID): Boolean =
        dcbTags.contains(GameDcbTags.game(gameId))
}
