package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.occurrent.application.service.blocking.CloudEventConverter
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.event.ReasonForNotBeingAwardedPoints.PlayerCreatedListOfWords
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import java.net.URI
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*

class GameCloudEventConverter(private val objectMapper: ObjectMapper, private val gameSource: URI, private val wordHintSource: URI) : CloudEventConverter<DomainEvent> {

    override fun toCloudEvent(domainEvent: DomainEvent): CloudEvent {
        val (source: URI, data: EventData?) = when (domainEvent) {
            is GameWasStarted -> gameSource to domainEvent.run { GameWasStartedData(startedBy, category, wordToGuess, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal) }
            is PlayerGuessedTheWrongWord -> gameSource to domainEvent.run { PlayerGuessedTheWrongWordData(playerId, guessedWord) }
            is CharacterInWordHintWasRevealed -> wordHintSource to domainEvent.run { CharacterInWordHintWasRevealedData(character, characterPositionInWord) }
            is NumberOfGuessesWasExhaustedForPlayer -> gameSource to NumberOfGuessesWasExhaustedForPlayerData(domainEvent.playerId)
            is PlayerGuessedTheRightWord -> gameSource to domainEvent.run { PlayerGuessedTheRightWordData(playerId, guessedWord) }
            is PlayerWasAwardedPointsForGuessingTheRightWord -> gameSource to domainEvent.run { PlayerWasAwardedPointsForGuessingTheRightWordData(playerId, points) }
            is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> gameSource to domainEvent.run { PlayerWasNotAwardedAnyPointsForGuessingTheRightWordData(playerId, reason::class.simpleName!!) }
            is GameWasWon -> gameSource to GameWasWonData(domainEvent.winnerId)
            is GameWasLost -> gameSource to null
        }

        return CloudEventBuilder.v1()
                .withId(domainEvent.eventId.toString())
                .withSubject(domainEvent.gameId.toString())
                .withSource(source)
                .withType(domainEvent.type)
                .withTime(domainEvent.timestamp.toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .apply {
                    if (data != null) {
                        withDataContentType("application/json")
                        withData(data.toBytes())
                    }
                }
                .build()
    }

    override fun toDomainEvent(cloudEvent: CloudEvent): DomainEvent {
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

    private fun EventData.toBytes() = objectMapper.writeValueAsBytes(this)
    private inline fun <reified T : EventData> CloudEvent.data(): T = objectMapper.readValue(data!!)
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