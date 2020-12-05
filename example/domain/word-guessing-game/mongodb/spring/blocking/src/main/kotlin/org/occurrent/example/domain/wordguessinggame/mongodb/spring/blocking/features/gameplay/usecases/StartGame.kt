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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.usecases

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.executePolicy
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.wordhint.RevealInitialCharactersInWordHintAfterGameIsStarted
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class StartGame constructor(
    private val applicationService: ApplicationService<GameEvent>,
    private val revealInitialCharactersInWordHintAfterGameIsStarted: RevealInitialCharactersInWordHintAfterGameIsStarted
) {

    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordList: WordList) {
        applicationService.execute(gameId, { events ->
            startGame(events, gameId, startTime, startedBy, wordList, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal)
        }, executePolicy(revealInitialCharactersInWordHintAfterGameIsStarted::invoke))
    }
}