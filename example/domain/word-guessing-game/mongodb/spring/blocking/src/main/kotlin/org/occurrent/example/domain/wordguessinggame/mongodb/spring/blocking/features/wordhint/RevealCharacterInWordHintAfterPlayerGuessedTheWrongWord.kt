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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.wordhint

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.GameEventQueries
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component


@Component
class RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord(
    private val applicationService: ApplicationService<GameEvent>,
    private val gameEventQueries: GameEventQueries
) {

    @Retryable(backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(playerGuessedTheWrongWord: PlayerGuessedTheWrongWord) {
        val gameId = playerGuessedTheWrongWord.gameId
        val gameWasStarted = gameEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))
        applicationService.execute("wordhint:$gameId") { events: Sequence<GameEvent> ->
            val characterPositionsInWord = events.map { it as CharacterInWordHintWasRevealed }.map { it.characterPositionInWord }.toSet()
            val wordHintData = WordHintData(gameId, wordToGuess = gameWasStarted.wordToGuess, currentlyRevealedPositions = characterPositionsInWord)
            WordHintCharacterRevelation.revealCharacterInWordHintWhenPlayerGuessedTheWrongWord(wordHintData)
        }
    }
}