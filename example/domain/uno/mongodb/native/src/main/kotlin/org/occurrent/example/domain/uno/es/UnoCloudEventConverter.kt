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

package org.occurrent.example.domain.uno.es

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.example.domain.uno.*
import org.occurrent.example.domain.uno.Card.*
import org.occurrent.example.domain.uno.Color.*
import org.occurrent.example.domain.uno.Digit.*
import java.net.URI
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit.MILLIS
import kotlin.reflect.KClass

private typealias CardDTO = Map<String, String>

class UnoCloudEventConverter(private val objectMapper: ObjectMapper) : CloudEventConverter<Event> {

    override fun toCloudEvent(event: Event): CloudEvent {
        val data = when (event) {
            is GameStarted -> GameStartedData(event.firstPlayerId, event.playerCount, event.firstCard.toDTO())
            is CardPlayed -> CardPlayedData(event.playerId, event.nextPlayerId, event.card.toDTO())
            is PlayerPlayedAtWrongTurn -> PlayerPlayedAtWrongTurnData(event.playerId, event.card.toDTO())
            is PlayerPlayedWrongCard -> PlayerPlayedWrongCardData(event.playerId, event.card.toDTO())
            is DirectionChanged -> DirectionChangedData(event.direction)
        }.toJson()

        return CloudEventBuilder.v1()
                .withId(event.eventId.toString())
                .withSource(URI.create("urn:occurrent:example:uno"))
                .withType(event.type)
                .withTime(event.timestamp.atOffset(ZoneOffset.UTC).truncatedTo(MILLIS))
                .withSubject(event.gameId.toString())
                .withDataContentType("application/json")
                .withData(data)
                .build()
    }

    override fun toDomainEvent(cloudEvent: CloudEvent): Event {
        val eventId = EventId.fromString(cloudEvent.id)
        val gameId = GameId.fromString(cloudEvent.subject)
        val timestamp = cloudEvent.time!!.toLocalDateTime()
        return when (cloudEvent.type) {
            GameStarted::class.type -> cloudEvent.data<GameStartedData> { GameStarted(eventId, gameId, timestamp, firstPlayerId, playerCount, firstCard.toDomain()) }
            CardPlayed::class.type -> cloudEvent.data<CardPlayedData> { CardPlayed(eventId, gameId, timestamp, playerId, card.toDomain(), nextPlayerId) }
            PlayerPlayedAtWrongTurn::class.type -> cloudEvent.data<PlayerPlayedAtWrongTurnData> { PlayerPlayedAtWrongTurn(eventId, gameId, timestamp, playerId, card.toDomain()) }
            PlayerPlayedWrongCard::class.type -> cloudEvent.data<PlayerPlayedWrongCardData> { PlayerPlayedAtWrongTurn(eventId, gameId, timestamp, playerId, card.toDomain()) }
            DirectionChanged::class.type -> cloudEvent.data<DirectionChangedData> { DirectionChanged(eventId, gameId, timestamp, direction) }
            else -> throw IllegalStateException("Invalid event type in database: ${cloudEvent.type}")
        }
    }

    private fun Data.toJson() = objectMapper.writeValueAsBytes(this)
    private inline fun <reified T : Data> CloudEvent.data(fn: T.() -> Event): Event {
        val data = objectMapper.readValue<T>(data!!.toBytes())
        return fn(data)
    }
}

sealed class Data
internal data class GameStartedData(val firstPlayerId: PlayerId, val playerCount: PlayerCount, val firstCard: CardDTO) : Data()
internal data class CardPlayedData(val playerId: PlayerId, val nextPlayerId: PlayerId, val card: CardDTO) : Data()
internal data class PlayerPlayedAtWrongTurnData(val playerId: PlayerId, val card: CardDTO) : Data()
internal data class PlayerPlayedWrongCardData(val playerId: PlayerId, val card: CardDTO) : Data()
internal data class DirectionChangedData(val direction: Direction) : Data()

private fun Card.toDTO(): CardDTO = when (this) {
    is DigitCard -> mapOf("type" to "Digit", "digit" to digit.string(), "color" to color.string())
    is KickBack -> mapOf("type" to "KickBack", "color" to color.string())
    is Skip -> mapOf("type" to "Skip", "color" to color.string())
}

private fun CardDTO.toDomain(): Card {
    fun digit() = (this["digit"] as String).toDigit()
    fun color() = (this["color"] as String).toColor()

    return when (val type = this["type"] as String) {
        "Digit" -> DigitCard(digit(), color())
        "KickBack" -> KickBack(color())
        "Skip" -> Skip(color())
        else -> throw IllegalStateException("Invalid card type in database: $type")
    }
}

private fun Digit.string(): String = this::class.simpleName!!
private fun Color.string(): String = this::class.simpleName!!
private fun String.toColor(): Color = when (this) {
    Red::class.simpleName!! -> Red
    Green::class.simpleName!! -> Green
    Blue::class.simpleName!! -> Blue
    Yellow::class.simpleName!! -> Yellow
    else -> throw IllegalStateException("Invalid color in database: $this")
}

private fun String.toDigit(): Digit = when (this) {
    Zero::class.simpleName!! -> Zero
    One::class.simpleName!! -> One
    Two::class.simpleName!! -> Two
    Three::class.simpleName!! -> Three
    Four::class.simpleName!! -> Four
    Five::class.simpleName!! -> Five
    Six::class.simpleName!! -> Six
    Seven::class.simpleName!! -> Seven
    Eight::class.simpleName!! -> Eight
    Nine::class.simpleName!! -> Nine
    else -> throw IllegalStateException("Invalid color in database: $this")
}

private val KClass<out Event>.type: String
    get() = simpleName!!

private val Event.type: String
    get() = this::class.type