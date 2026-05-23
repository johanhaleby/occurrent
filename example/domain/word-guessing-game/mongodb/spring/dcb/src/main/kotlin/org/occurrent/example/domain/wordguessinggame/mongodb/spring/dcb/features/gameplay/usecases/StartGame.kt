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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.usecases

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.MaxNumberOfGuessesPerPlayer
import org.occurrent.example.domain.wordguessinggame.writemodel.MaxNumberOfGuessesTotal
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.occurrent.example.domain.wordguessinggame.writemodel.startGame
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

@Service
class StartGame(private val applicationService: DcbApplicationService<GameEvent>) {

    @Retryable(include = [DcbAppendConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordList: WordList) {
        applicationService.execute(GameDcbQueries.gameplay(gameId)) { events: Stream<GameEvent> ->
            startGame(events.asSequence(), gameId, startTime, startedBy, wordList, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal).asStream()
        }
    }
}
