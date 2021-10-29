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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.pointawarding

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.writemodel.BasisForPointAwarding
import org.occurrent.example.domain.wordguessinggame.writemodel.PointAwarding
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component

@Component
class AwardPointsToPlayerThatGuessedTheRightWord(
    private val applicationService: ApplicationService<GameEvent>,
    private val domainEventQueries: DomainEventQueries<GameEvent>
) {

    @Retryable(backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(playerGuessedTheRightWord: PlayerGuessedTheRightWord) {
        val gameId = playerGuessedTheRightWord.gameId
        val playerId = playerGuessedTheRightWord.playerId
        val gameWasStarted = domainEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))
        applicationService.execute("points:$gameId") { events: Sequence<GameEvent> ->
            val eventList = events.toList()
            val totalNumberGuessesForPlayerInGame = eventList.count { event -> event is PlayerGuessedTheWrongWord && event.playerId == playerGuessedTheRightWord.playerId } + 1
            val basis = BasisForPointAwarding(gameId, gameWasStarted.startedBy, playerId, totalNumberGuessesForPlayerInGame)
            PointAwarding.awardPointsToPlayerThatGuessedTheRightWord(basis)
        }
    }
}