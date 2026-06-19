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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.wordhint

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.queryForList
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import java.util.stream.Stream
import kotlin.math.max
import kotlin.math.min
import kotlin.streams.asStream

@Configuration
class RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord(
    private val applicationService: DcbApplicationService<GameEvent>,
    private val domainEventQueries: DcbDomainEventQueries<GameEvent>,
    private val subscriptions: Subscriptions<GameEvent>
) {

    @Bean
    fun whenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint() =
        subscriptions.subscribe<PlayerGuessedTheWrongWord>("WhenPlayerGuessedTheWrongWordThenRevealCharacterInWordHintPolicy") { wrongGuess ->
            invoke(wrongGuess)
        }

    @Retryable(backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(playerGuessedTheWrongWord: PlayerGuessedTheWrongWord) {
        val gameId = playerGuessedTheWrongWord.gameId
        val wrongGuessCount = domainEventQueries
            .queryForList(GameDcbQueries.gameplay(gameId))
            .filterIsInstance<PlayerGuessedTheWrongWord>()
            .size

        applicationService.execute(GameDcbQueries.wordHintDecisionContext(gameId)) { events: Stream<GameEvent> ->
            val eventList = events.toList()
            val gameWasStarted = eventList.filterIsInstance<GameWasStarted>().firstOrNull()
            val revealedCharacters = eventList.filterIsInstance<CharacterInWordHintWasRevealed>()

            if (gameWasStarted == null || revealedCharacters.size >= maximumNumberOfRevealedCharacters(gameWasStarted.wordToGuess, wrongGuessCount)) {
                Stream.empty()
            } else {
                val wordHintData = WordHintData(
                    gameId,
                    wordToGuess = gameWasStarted.wordToGuess,
                    currentlyRevealedPositions = revealedCharacters.map { it.characterPositionInWord }.toSet()
                )
                WordHintCharacterRevelation.revealCharacterInWordHintWhenPlayerGuessedTheWrongWord(wordHintData).asStream()
            }
        }
    }

    private fun maximumNumberOfRevealedCharacters(wordToGuess: String, wrongGuessCount: Int): Int {
        val revealableCharacters = wordToGuess.count { it != '-' }
        val maximumRevealableCharacters = max(0, revealableCharacters - minimumNumberOfObfuscatedCharactersInWordHint)
        val initialCharacters = min(minimumNumberOfRevealedCharactersInWordHint, maximumRevealableCharacters)
        return min(maximumRevealableCharacters, initialCharacters + wrongGuessCount)
    }

    private companion object {
        const val minimumNumberOfRevealedCharactersInWordHint = 2
        const val minimumNumberOfObfuscatedCharactersInWordHint = 2
    }
}
