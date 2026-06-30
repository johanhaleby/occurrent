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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.pointawarding

import org.occurrent.annotation.StreamSubscription
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.dcbPosition
import org.occurrent.dsl.dcb.blocking.dcbTags
import org.occurrent.dsl.subscription.EventMetadata
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasNotAwardedAnyPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.writemodel.BasisForPointAwarding
import org.occurrent.example.domain.wordguessinggame.writemodel.PointAwarding
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.stream.Stream
import kotlin.streams.asStream

@Component
class AwardPointsToPlayerThatGuessedTheRightWord(
    private val applicationService: DcbApplicationService<GameEvent>
) {

    @StreamSubscription(id = "WhenPlayerGuessedTheRightWordThenAwardPointsPolicy")
    fun whenPlayerGuessedTheRightWord(playerGuessedTheRightWord: PlayerGuessedTheRightWord, metadata: EventMetadata) {
        if (!metadata.belongsToGame(playerGuessedTheRightWord.gameId)) {
            return
        }
        requireNotNull(metadata.dcbPosition) { "Expected DCB position for ${playerGuessedTheRightWord.type}" }
        invoke(playerGuessedTheRightWord)
    }

    @Retryable(backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(playerGuessedTheRightWord: PlayerGuessedTheRightWord) {
        val gameId = playerGuessedTheRightWord.gameId
        val playerId = playerGuessedTheRightWord.playerId

        applicationService.execute(GameDcbQueries.pointsBoundary(gameId)) { events: Stream<GameEvent> ->
            val eventList = events.toList()
            val gameWasStarted = eventList.filterIsInstance<GameWasStarted>().firstOrNull()
            val pointsAlreadyAwarded = eventList
                .filter { it is PlayerWasAwardedPointsForGuessingTheRightWord || it is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord }
                .any { it.gameId == gameId && playerId(it) == playerId }

            if (gameWasStarted == null || pointsAlreadyAwarded) {
                Stream.empty()
            } else {
                val totalNumberGuessesForPlayerInGame = eventList.filterIsInstance<PlayerGuessedTheWrongWord>().count { it.playerId == playerId } + 1
                val basis = BasisForPointAwarding(gameId, gameWasStarted.startedBy, playerId, totalNumberGuessesForPlayerInGame)
                PointAwarding.awardPointsToPlayerThatGuessedTheRightWord(basis).asStream()
            }
        }
    }

    private fun playerId(event: GameEvent) = when (event) {
        is PlayerWasAwardedPointsForGuessingTheRightWord -> event.playerId
        is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> event.playerId
        else -> throw IllegalArgumentException("Expected points event, got ${event::class.simpleName}")
    }

    private fun EventMetadata.belongsToGame(gameId: UUID): Boolean =
        dcbTags.contains(GameDcbTags.game(gameId))
}
