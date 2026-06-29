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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.usecases

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.dsl.decider.Decider
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameCommand
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameState
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class StartGame(
    private val applicationService: DcbApplicationService<GameEvent>,
    private val decider: Decider<WordGuessingGameCommand, WordGuessingGameState, GameEvent>
) {

    @Transactional
    @Retryable(include = [DcbAppendConditionNotFulfilledException::class, DataIntegrityViolationException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordList: WordList) {
        applicationService.execute(
            GameDcbQueries.gameplay(gameId),
            WordGuessingGameCommand.StartGame(
                eventId = UUID.randomUUID(),
                gameId = gameId,
                timestamp = startTime,
                startedBy = startedBy,
                category = wordList.category,
                wordToGuess = wordList.words.random()
            ),
            decider
        )
    }
}