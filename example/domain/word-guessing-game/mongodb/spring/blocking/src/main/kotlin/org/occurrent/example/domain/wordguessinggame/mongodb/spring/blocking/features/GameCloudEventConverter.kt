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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.readValue
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.data.PojoCloudEventData
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.event.ReasonForNotBeingAwardedPoints.PlayerCreatedListOfWords
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import java.net.URI
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*

internal class GameCloudEventConverter(private val objectMapper: ObjectMapper, private val gameSource: URI, private val wordHintSource: URI, private val pointsSource: URI) : CloudEventConverter<GameEvent> {

    override fun toCloudEvent(gameEvent: GameEvent): CloudEvent {
        val (source: URI, data: EventData?) = when (gameEvent) {
            is GameWasStarted -> gameSource to gameEvent.run { GameWasStartedData(startedBy, category, wordToGuess, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal) }
            is PlayerGuessedTheWrongWord -> gameSource to gameEvent.run { PlayerGuessedTheWrongWordData(playerId, guessedWord) }
            is CharacterInWordHintWasRevealed -> wordHintSource to gameEvent.run { CharacterInWordHintWasRevealedData(character, characterPositionInWord) }
            is NumberOfGuessesWasExhaustedForPlayer -> gameSource to NumberOfGuessesWasExhaustedForPlayerData(gameEvent.playerId)
            is PlayerGuessedTheRightWord -> gameSource to gameEvent.run { PlayerGuessedTheRightWordData(playerId, guessedWord) }
            is PlayerWasAwardedPointsForGuessingTheRightWord -> pointsSource to gameEvent.run { PlayerWasAwardedPointsForGuessingTheRightWordData(playerId, points) }
            is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> pointsSource to gameEvent.run { PlayerWasNotAwardedAnyPointsForGuessingTheRightWordData(playerId, reason::class.simpleName!!) }
            is GameWasWon -> gameSource to GameWasWonData(gameEvent.winnerId)
            is GameWasLost -> gameSource to null
        }

        return CloudEventBuilder.v1()
                .withId(gameEvent.eventId.toString())
                .withSubject(gameEvent.gameId.toString())
                .withSource(source)
                .withType(getCloudEventType(gameEvent))
                .withTime(gameEvent.timestamp.toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .apply {
                    if (data != null) {
                        withDataContentType("application/json")
                        withData(data.toCloudEventData())
                    }
                }
                .build()
    }

    override fun toDomainEvent(cloudEvent: CloudEvent): GameEvent {
        val eventId = UUID.fromString(cloudEvent.id)
        val gameId = UUID.fromString(cloudEvent.subject)
        val timestamp = Timestamp.from(cloudEvent.time!!.toInstant())

        return when (cloudEvent.type) {
            GameWasStarted::class.eventType() -> cloudEvent.data<GameWasStartedData>().run {
                GameWasStarted(eventId, timestamp, gameId, startedBy, category, wordToGuess, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal)
            }
            PlayerGuessedTheWrongWord::class.eventType() -> cloudEvent.data<PlayerGuessedTheWrongWordData>().run {
                PlayerGuessedTheWrongWord(eventId, timestamp, gameId, playerId, guessedWord)
            }
            NumberOfGuessesWasExhaustedForPlayer::class.eventType() -> cloudEvent.data<NumberOfGuessesWasExhaustedForPlayerData>().run {
                NumberOfGuessesWasExhaustedForPlayer(eventId, timestamp, gameId, playerId)
            }
            PlayerGuessedTheRightWord::class.eventType() -> cloudEvent.data<PlayerGuessedTheRightWordData>().run {
                PlayerGuessedTheRightWord(eventId, timestamp, gameId, playerId, guessedWord)
            }
            GameWasWon::class.eventType() -> cloudEvent.data<GameWasWonData>().run {
                GameWasWon(eventId, timestamp, gameId, winnerId)
            }
            GameWasLost::class.eventType() -> GameWasLost(eventId, timestamp, gameId)
            PlayerWasAwardedPointsForGuessingTheRightWord::class.eventType() -> cloudEvent.data<PlayerWasAwardedPointsForGuessingTheRightWordData>().run {
                PlayerWasAwardedPointsForGuessingTheRightWord(eventId, timestamp, gameId, playerId, points)
            }
            PlayerWasNotAwardedAnyPointsForGuessingTheRightWord::class.eventType() -> cloudEvent.data<PlayerWasNotAwardedAnyPointsForGuessingTheRightWordData>().run {
                val reason = if (reason == PlayerCreatedListOfWords::class.simpleName) {
                    PlayerCreatedListOfWords
                } else {
                    throw IllegalStateException("Unrecognized ${ReasonForNotBeingAwardedPoints::class.simpleName}: $reason")
                }

                PlayerWasNotAwardedAnyPointsForGuessingTheRightWord(eventId, timestamp, gameId, playerId, reason)
            }
            CharacterInWordHintWasRevealed::class.eventType() -> cloudEvent.data<CharacterInWordHintWasRevealedData>().run {
                CharacterInWordHintWasRevealed(eventId, timestamp, gameId, character, characterPositionInWord)
            }
            else -> throw IllegalStateException("Unrecognized event type: ${cloudEvent.type}")
        }
    }

    private fun EventData.toCloudEventData(): CloudEventData = PojoCloudEventData.wrap(objectMapper.convertValue<Map<String, Any>>(this), objectMapper::writeValueAsBytes)
    private inline fun <reified T : EventData> CloudEvent.data(): T =
            if (data is PojoCloudEventData<*>) {
                objectMapper.convertValue((data as PojoCloudEventData<*>).value)
            } else {
                objectMapper.readValue(data!!.toBytes())
            }

    override fun getCloudEventType(type: Class<out GameEvent>): String = type.kotlin.eventType()
}

private sealed class EventData

// Game
private data class GameWasStartedData(val startedBy: PlayerId, val category: String, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int) : EventData()
private data class PlayerGuessedTheWrongWordData(val playerId: PlayerId, val guessedWord: String) : EventData()
private data class NumberOfGuessesWasExhaustedForPlayerData(val playerId: PlayerId) : EventData()
private data class PlayerGuessedTheRightWordData(val playerId: PlayerId, val guessedWord: String) : EventData()
private data class PlayerWasAwardedPointsForGuessingTheRightWordData(val playerId: PlayerId, val points: Int) : EventData()
private data class PlayerWasNotAwardedAnyPointsForGuessingTheRightWordData(val playerId: PlayerId, val reason: String) : EventData()
private data class GameWasWonData(val winnerId: PlayerId) : EventData()

// Word Hint
private data class CharacterInWordHintWasRevealedData(val character: Char, val characterPositionInWord: Int) : EventData()