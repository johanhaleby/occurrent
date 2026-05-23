package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.NumberOfGuessesWasExhaustedForPlayer
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasNotAwardedAnyPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.ReasonForNotBeingAwardedPoints.PlayerCreatedListOfWords
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.util.Date
import java.util.UUID

@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class GameCloudEventConverterTest {

    private val gameSource = URI.create("urn:word-guessing-game:game")
    private val wordHintSource = URI.create("urn:word-guessing-game:word-hint")
    private val pointsSource = URI.create("urn:word-guessing-game:points")
    private val converter = GameCloudEventConverter(ObjectMapper(), gameSource, wordHintSource, pointsSource)
    private val eventId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val gameId = UUID.fromString("00000000-0000-0000-0000-000000000002")
    private val playerId = UUID.fromString("00000000-0000-0000-0000-000000000003")
    private val timestamp = Date(1_700_000_000_123)

    @Test
    fun `roundtrips all game event types`() {
        val events = listOf(
            GameWasStarted(eventId, timestamp, gameId, playerId, "animals", "moose", 3, 10),
            PlayerGuessedTheWrongWord(eventId, timestamp, gameId, playerId, "mouse"),
            NumberOfGuessesWasExhaustedForPlayer(eventId, timestamp, gameId, playerId),
            PlayerGuessedTheRightWord(eventId, timestamp, gameId, playerId, "moose"),
            GameWasWon(eventId, timestamp, gameId, playerId),
            GameWasLost(eventId, timestamp, gameId),
            CharacterInWordHintWasRevealed(eventId, timestamp, gameId, 'm', 0),
            PlayerWasAwardedPointsForGuessingTheRightWord(eventId, timestamp, gameId, playerId, 10),
            PlayerWasNotAwardedAnyPointsForGuessingTheRightWord(eventId, timestamp, gameId, playerId, PlayerCreatedListOfWords),
        )

        events.forEach { event ->
            val cloudEvent = converter.toCloudEvent(event)

            assertThat(converter.toDomainEvent(cloudEvent)).isEqualTo(event)
            assertThat(cloudEvent.id).isEqualTo(eventId.toString())
            assertThat(cloudEvent.subject).isEqualTo(gameId.toString())
            assertThat(cloudEvent.type).isEqualTo(event.type)
            assertThat(cloudEvent.source).isEqualTo(expectedSourceFor(event))
        }
    }

    private fun expectedSourceFor(event: GameEvent): URI =
        when (event) {
            is CharacterInWordHintWasRevealed -> wordHintSource
            is PlayerWasAwardedPointsForGuessingTheRightWord,
            is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> pointsSource
            else -> gameSource
        }
}
