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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.views.ongoinggamesoverview

import org.occurrent.annotation.Subscription
import org.occurrent.dsl.dcb.blocking.dcbPosition
import org.occurrent.dsl.dcb.blocking.dcbTags
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.support.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria.where
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.remove
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class AddedOrRemoveGameFromOngoingGamesOverview(private val mongo: MongoOperations) {
    private val log = loggerFor<AddedOrRemoveGameFromOngoingGamesOverview>()

    @Subscription(id = "WhenGameWasStartedThenAddGameToOngoingGamesOverview")
    fun whenGameWasStartedThenAddGameToOngoingGamesOverview(gameWasStarted: GameWasStarted, metadata: EventMetadata) {
        if (!metadata.belongsToGame(gameWasStarted.gameId)) {
            return
        }
        requireNotNull(metadata.dcbPosition) { "Expected DCB position for ${gameWasStarted.type}" }
        log.info("Adding game ${gameWasStarted.gameId} to ongoing games view")
        val ongoingGameOverview = gameWasStarted.run {
            OngoingGameOverview(gameId, category, startedBy, timestamp).toDTO()
        }
        mongo.save(ongoingGameOverview)
    }

    @Subscription(id = "WhenGameIsEndedThenRemoveGameFromOngoingGamesOverview", eventTypes = [GameWasWon::class, GameWasLost::class])
    fun whenGameIsEndedThenRemoveGameFromOngoingGamesOverview(e: GameEvent, metadata: EventMetadata) {
        val gameId = when (e) {
            is GameWasWon -> e.gameId
            is GameWasLost -> e.gameId
            else -> throw IllegalStateException("Internal error")
        }
        if (!metadata.belongsToGame(gameId)) {
            return
        }
        requireNotNull(metadata.dcbPosition) { "Expected DCB position for ${e.type}" }
        log.info("Removing game $gameId from ongoing games view since ${e.type}")
        mongo.remove<OngoingGameOverviewMongoDTO>(query(where("_id").isEqualTo(gameId.toString())))
    }

    private fun EventMetadata.belongsToGame(gameId: UUID): Boolean =
        dcbTags.contains(GameDcbTags.game(gameId))
}
