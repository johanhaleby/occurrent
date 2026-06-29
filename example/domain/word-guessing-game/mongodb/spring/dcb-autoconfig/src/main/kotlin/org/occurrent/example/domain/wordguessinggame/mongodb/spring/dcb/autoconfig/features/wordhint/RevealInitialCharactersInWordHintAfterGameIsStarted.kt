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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.wordhint

import org.occurrent.annotation.Subscription
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.dcbPosition
import org.occurrent.dsl.dcb.blocking.dcbTags
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.stream.Stream
import kotlin.streams.asStream

@Component
class RevealInitialCharactersInWordHintAfterGameIsStarted(
    private val applicationService: DcbApplicationService<GameEvent>
) {

    @Subscription(id = "WhenGameWasStartedThenRevealInitialCharactersInWordHintPolicy")
    fun whenGameWasStarted(gameWasStarted: GameWasStarted, metadata: EventMetadata) {
        if (!metadata.belongsToGame(gameWasStarted.gameId)) {
            return
        }
        requireNotNull(metadata.dcbPosition) { "Expected DCB position for ${gameWasStarted.type}" }
        invoke(gameWasStarted)
    }

    @Retryable(backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(gameWasStarted: GameWasStarted) {
        applicationService.execute(GameDcbQueries.wordHintDecisionContext(gameWasStarted.gameId)) { events: Stream<GameEvent> ->
            if (events.toList().filterIsInstance<CharacterInWordHintWasRevealed>().isEmpty()) {
                WordHintCharacterRevelation
                    .revealInitialCharactersInWordHintWhenGameWasStarted(WordHintData(gameWasStarted.gameId, gameWasStarted.wordToGuess))
                    .asStream()
            } else {
                Stream.empty()
            }
        }
    }

    private fun EventMetadata.belongsToGame(gameId: UUID): Boolean =
        dcbTags.contains(GameDcbTags.game(gameId))
}
