/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.pragmatic.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.data.PojoCloudEventData
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.application.composition.command.andThen
import org.occurrent.application.composition.command.composeCommands
import org.occurrent.application.composition.command.partial
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.OffsetDateTime
import java.util.*

class ApplicationServiceDemo {

    @Test
    fun `simple cloud event converter and single command`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = simpleJacksonCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(gameId.value) { events: List<GameEvent> ->
            listOf(RPS.create(events, gameId, Timestamp.now(), GameCreatorId.random()))
        }

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(GameCreated::class.qualifiedName)
    }

    @Test
    fun `simple cloud event converter and composed commands`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = simpleJacksonCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            composeCommands(
                { events: List<GameEvent> ->
                    listOf(RPS.create(events, gameId, Timestamp.now(), GameCreatorId.random()))
                },
                { events ->
                    listOf(RPS.join(events, Timestamp.now(), PlayerId.random()))
                })
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, FirstPlayerBecameReady::class.qualifiedName
            )
    }

    @Test
    fun `simple cloud event converter and composed commands with partial function application`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = simpleJacksonCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            composeCommands(
                RPS::create.partial(gameId, Timestamp.now(), GameCreatorId.random()).toList(),
                RPS::join.partial(Timestamp.now(), PlayerId.random()).toList(),
            )
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, FirstPlayerBecameReady::class.qualifiedName
            )
    }

    @Test
    fun `simple cloud event converter and composed commands with partial function application using infix`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = simpleJacksonCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            RPS::create.partial(gameId, Timestamp.now(), GameCreatorId.random()).toList() andThen
                    RPS::join.partial(Timestamp.now(), PlayerId.random()).toList()
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, FirstPlayerBecameReady::class.qualifiedName
            )
    }

    @Test
    fun `custom production cloud event converter and composed commands with partial function application using custom extension function`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = CustomProductionCloudEventConverter(jacksonObjectMapper())
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()
        val playerId1 = PlayerId.random()
        val playerId2 = PlayerId.random()

        // When
        applicationService.execute(
            gameId.value,
            composeCommands(
                RPS::create.partial(gameId, Timestamp.now(), GameCreatorId.random()).toList(),
                RPS::join.partial(Timestamp.now(), playerId1).toList(),
                RPS::join.partial(Timestamp.now(), playerId2).toList(),
                RPS::play.partial(Timestamp.now(), playerId1, HandShape.ROCK),
                RPS::play.partial(Timestamp.now(), playerId2, HandShape.PAPER)
            )
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsExactly(
                GameCreated::class.simpleName, FirstPlayerBecameReady::class.simpleName,
                SecondPlayerBecameReady::class.simpleName, GameStarted::class.simpleName,
                HandPlayed::class.simpleName, HandPlayed::class.simpleName, GameWon::class.simpleName,
                GameEnded::class.simpleName
            )
    }
}

class CustomProductionCloudEventConverter(private val objectMapper: ObjectMapper) : CloudEventConverter<GameEvent> {

    override fun toCloudEvent(e: GameEvent): CloudEvent {
        val data = when (e) {
            is FirstPlayerBecameReady -> mapOf("player" to e.player.value.toString())
            is SecondPlayerBecameReady -> mapOf("player" to e.player.value.toString())
            is GameCreated -> mapOf("createdBy" to e.createdBy.value.toString())
            is GameEnded -> null
            is GameStarted -> null
            is GameTied -> null
            is GameWon -> mapOf("winner" to e.winner.value.toString())
            is HandPlayed -> mapOf("player" to e.player.value.toString(), "shape" to e.shape.name)
        }

        return CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withSubject(e.game.value.toString())
            .withSource(URI.create("urn:rockpaperscissors:game"))
            .withType(getCloudEventType(e))
            .withTime(OffsetDateTime.ofInstant(e.timestamp.value.toInstant(), e.timestamp.value.zone))
            .withDataContentType("application/xml")
            .withData(if (data == null) null else PojoCloudEventData.wrap(data, objectMapper::writeValueAsBytes))
            .build()
    }

    override fun toDomainEvent(cloudEvent: CloudEvent): GameEvent {
        val gameId = GameId(UUID.fromString(cloudEvent.subject))
        val timestamp = Timestamp(cloudEvent.time!!.toZonedDateTime())
        val data = cloudEvent.data.asMap()
        return when (cloudEvent.type) {
            FirstPlayerBecameReady::class.simpleName -> FirstPlayerBecameReady(gameId, timestamp, PlayerId(data.coerced("player")))
            SecondPlayerBecameReady::class.simpleName -> SecondPlayerBecameReady(gameId, timestamp, PlayerId(data.coerced("player")))
            GameCreated::class.simpleName -> GameCreated(gameId, timestamp, GameCreatorId(data.coerced("createdBy")))
            GameEnded::class.simpleName -> GameEnded(gameId, timestamp)
            GameStarted::class.simpleName -> GameStarted(gameId, timestamp)
            GameTied::class.simpleName -> GameTied(gameId, timestamp)
            GameWon::class.simpleName -> GameWon(gameId, timestamp, PlayerId(data.coerced("winner")))
            HandPlayed::class.simpleName -> HandPlayed(gameId, timestamp, PlayerId(data.coerced("player")), HandShape.valueOf(data.coerced("shape")))
            else -> throw IllegalStateException("Event type ${cloudEvent.type} is unknown")
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun CloudEventData?.asMap(): Map<String, Any> =
        if (this == null) {
            emptyMap()
        } else {
            val pojoCloudEventData = this as PojoCloudEventData<*>
            when (val value = pojoCloudEventData.value) {
                is Map<*, *> -> value as Map<String, Any>
                else -> objectMapper.readValue(pojoCloudEventData.toBytes())
            }
        }

    private inline fun <reified T> Map<String, Any>.coerced(propertyName: String): T {
        val value = this[propertyName]
        return when {
            UUID::class == T::class && value is String -> UUID.fromString(value)
            else -> value
        } as T
    }

    override fun getCloudEventType(type: Class<out GameEvent>): String = type.simpleName
}

private fun simpleJacksonCloudEventConverter() = JacksonCloudEventConverter<GameEvent>(jacksonObjectMapper().registerModule(JavaTimeModule()), URI.create("urn:rockpaperscissors:game"))

fun ((List<GameEvent>) -> GameEvent).toList(): (List<GameEvent>) -> List<GameEvent> = { events ->
    listOf(this(events))
}