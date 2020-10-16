package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import java.net.URI
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*

class CloudEventConverter(private val objectMapper: ObjectMapper, private val cloudEventSource: URI) {

    fun toCloudEvent(domainEvent: DomainEvent): CloudEvent {
        val data: EventData? = when (domainEvent) {
            is GameWasStarted -> domainEvent.run { GameWasStartedData(startedBy, category, wordToGuess, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal) }
            is PlayerGuessedTheWrongWord -> domainEvent.run { PlayerGuessedTheWrongWordData(playerId, guessedWord) }
            is NumberOfGuessesWasExhaustedForPlayer -> NumberOfGuessesWasExhaustedForPlayerData(domainEvent.playerId)
            is PlayerGuessedTheRightWord -> domainEvent.run { PlayerGuessedTheRightWordData(playerId, guessedWord) }
            is PlayerWasAwardedPointsForGuessingTheRightWord -> domainEvent.run { PlayerWasAwardedPointsForGuessingTheRightWordData(playerId, points) }
            is GameWasWon -> GameWasWonData(domainEvent.winnerId)
            is GameWasLost -> null
        }

        return CloudEventBuilder.v1()
                .withId(domainEvent.eventId.toString())
                .withSubject(domainEvent.gameId.toString())
                .withSource(cloudEventSource)
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

    fun toDomainEvent(cloudEvent: CloudEvent): DomainEvent {
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
            else -> throw IllegalStateException("Unrecognized event type: ${cloudEvent.type}")
        }
    }

    private fun EventData.toBytes() = objectMapper.writeValueAsBytes(this)
    private inline fun <reified T : EventData> CloudEvent.data(): T = objectMapper.readValue(data!!)
}

private sealed class EventData
private data class GameWasStartedData(val startedBy: PlayerId, val category: String, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int) : EventData()
private data class PlayerGuessedTheWrongWordData(val playerId: PlayerId, val guessedWord: String) : EventData()
private data class NumberOfGuessesWasExhaustedForPlayerData(val playerId: PlayerId) : EventData()
private data class PlayerGuessedTheRightWordData(val playerId: PlayerId, val guessedWord: String) : EventData()
private data class PlayerWasAwardedPointsForGuessingTheRightWordData(val playerId: PlayerId, val points: Int) : EventData()
private data class GameWasWonData(val winnerId: PlayerId) : EventData()