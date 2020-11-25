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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.GameEventQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.first
import org.occurrent.example.domain.wordguessinggame.writemodel.BasisForPointAwarding
import org.occurrent.example.domain.wordguessinggame.writemodel.PointAwarding
import org.occurrent.filter.Filter.streamId
import org.springframework.stereotype.Component

@Component
class PointAwardingPolicy(private val applicationService: ApplicationService<GameEvent>, private val gameEventQueries: GameEventQueries) {

    fun whenPlayerGuessedTheRightWordThenAwardPointsToPlayerThatGuessedTheRightWord(playerGuessedTheRightWord: PlayerGuessedTheRightWord) {
        val gameId = playerGuessedTheRightWord.gameId
        val playerId = playerGuessedTheRightWord.playerId
        val eventsInGame = gameEventQueries.query<GameEvent>(streamId(gameId.toString())).toList()
        val gameWasStarted = eventsInGame.first<GameWasStarted>()
        val totalNumberGuessesForPlayerInGame = eventsInGame.count { event -> event is PlayerGuessedTheWrongWord && event.playerId == playerGuessedTheRightWord.playerId } + 1
        applicationService.execute("points:$gameId") { _: Sequence<GameEvent> ->
            val basis = BasisForPointAwarding(gameId, gameWasStarted.startedBy, playerId, totalNumberGuessesForPlayerInGame)
            PointAwarding.awardPointsToPlayerThatGuessedTheRightWord(basis)
        }
    }
}