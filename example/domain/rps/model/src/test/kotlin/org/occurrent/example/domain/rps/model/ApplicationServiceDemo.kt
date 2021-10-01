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

package org.occurrent.example.domain.rps.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.thoughtworks.xstream.XStream
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
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.eventstore.api.WriteResult
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.OffsetDateTime
import java.util.*

class ApplicationServiceDemo {

    @Test
    fun `simple cloud event converter and single command`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(gameId.value) { events: Sequence<GameEvent> ->
            handle(events, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE))
        }

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(GameCreated::class.qualifiedName)
    }

    @Test
    fun `simple cloud event converter and composed commands`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            composeCommands(
                { events: Sequence<GameEvent> ->
                    handle(events, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE))
                },
                { events ->
                    handle(events, PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK))
                })
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, RoundStarted::class.qualifiedName, GameStarted::class.qualifiedName,
                FirstPlayerJoinedGame::class.qualifiedName, HandPlayed::class.qualifiedName
            )
    }

    @Test
    fun `simple cloud event converter and composed commands with partial function application`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            composeCommands(
                ::handle.partial(CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE)),
                ::handle.partial(PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK))
            )
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, RoundStarted::class.qualifiedName, GameStarted::class.qualifiedName,
                FirstPlayerJoinedGame::class.qualifiedName, HandPlayed::class.qualifiedName
            )
    }

    @Test
    fun `simple cloud event converter and composed commands with partial function application using infix`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId.value,
            ::handle.partial(CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE)) andThen
                    ::handle.partial(PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK))
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, RoundStarted::class.qualifiedName, GameStarted::class.qualifiedName,
                FirstPlayerJoinedGame::class.qualifiedName, HandPlayed::class.qualifiedName
            )
    }

    @Test
    fun `simple cloud event converter and composed commands with partial function application using custom extension function`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = SimpleCloudEventConverter()
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId,
            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE),
            PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK)
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, RoundStarted::class.qualifiedName, GameStarted::class.qualifiedName,
                FirstPlayerJoinedGame::class.qualifiedName, HandPlayed::class.qualifiedName
            )
    }

    @Test
    fun `production cloud event converter and composed commands with partial function application using custom extension function`() {
        // Given
        val inMemoryEventStore = InMemoryEventStore()
        val cloudEventConvert = ProductionCloudEventConverter(jacksonObjectMapper())
        val applicationService = GenericApplicationService(inMemoryEventStore, cloudEventConvert)

        val gameId = GameId.random()

        // When
        applicationService.execute(
            gameId,
            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds.ONE),
            PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK)
        )

        // Then
        assertThat(inMemoryEventStore.read(gameId.value.toString()).map(CloudEvent::getType))
            .containsOnly(
                GameCreated::class.qualifiedName, RoundStarted::class.qualifiedName, GameStarted::class.qualifiedName,
                FirstPlayerJoinedGame::class.qualifiedName, HandPlayed::class.qualifiedName
            )
    }
}

private fun ApplicationService<GameEvent>.execute(gameId: GameId, firstCommand: Command, vararg additionalCommands: Command): WriteResult {
    val functionsToInvoke = sequenceOf(firstCommand, *additionalCommands).map { cmd ->
        ::handle.partial(cmd)
    }

    return execute(gameId.value, composeCommands(functionsToInvoke))
}

class SimpleCloudEventConverter : CloudEventConverter<GameEvent> {
    private val xstream = XStream().apply { allowTypesByWildcard(arrayOf("org.occurrent.example.domain.rps.model.**")) }

    override fun toCloudEvent(e: GameEvent): CloudEvent = CloudEventBuilder.v1()
        .withId(UUID.randomUUID().toString())
        .withSource(URI.create("urn:rockpaperscissors:game"))
        .withType(e::class.qualifiedName)
        .withTime(OffsetDateTime.ofInstant(e.timestamp.value.toInstant(), e.timestamp.value.zone))
        .withDataContentType("application/xml")
        .withData(xstream.toXML(e).toByteArray())
        .build()

    override fun toDomainEvent(cloudEvent: CloudEvent): GameEvent =
        xstream.fromXML(String(cloudEvent.data!!.toBytes())) as GameEvent
}

class ProductionCloudEventConverter(private val objectMapper: ObjectMapper) : CloudEventConverter<GameEvent> {

    override fun toCloudEvent(e: GameEvent): CloudEvent {
        val data = when (e) {
            is FirstPlayerJoinedGame -> mapOf("player" to e.player.value.toString())
            is GameCreated -> mapOf("createdBy" to e.createdBy.value.toString(), "maxNumberOfRounds" to e.maxNumberOfRounds.value)
            is GameEnded -> null
            is GameStarted -> null
            is GameTied -> null
            is GameWon -> mapOf("winner" to e.winner.value.toString())
            is HandPlayed -> mapOf("player" to e.player.value.toString(), "shape" to e.shape.name, "roundNumber" to e.roundNumber.value)
            is RoundEnded -> mapOf("roundNumber" to e.roundNumber.value)
            is RoundStarted -> mapOf("roundNumber" to e.roundNumber.value)
            is RoundTied -> mapOf("roundNumber" to e.roundNumber.value)
            is RoundWon -> mapOf("winner" to e.winner.value.toString(), "roundNumber" to e.roundNumber.value)
            is SecondPlayerJoinedGame -> mapOf("player" to e.player.value.toString())
        }

        return CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withSubject(e.game.value.toString())
            .withSource(URI.create("urn:rockpaperscissors:game"))
            .withType(e::class.qualifiedName)
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
            FirstPlayerJoinedGame::class.simpleName -> FirstPlayerJoinedGame(gameId, timestamp, PlayerId(data.coerced("player")))
            GameCreated::class.simpleName -> GameCreated(gameId, timestamp, GameCreatorId(data.coerced("createdBy")), MaxNumberOfRounds.unsafe(data.coerced("maxNumberOfRounds")))
            GameEnded::class.simpleName -> GameEnded(gameId, timestamp)
            GameStarted::class.simpleName -> GameStarted(gameId, timestamp)
            GameTied::class.simpleName -> GameTied(gameId, timestamp)
            GameWon::class.simpleName -> GameWon(gameId, timestamp, PlayerId(data.coerced("winner")))
            HandPlayed::class.simpleName -> HandPlayed(gameId, timestamp, PlayerId(data.coerced("player")), Shape.valueOf(data.coerced("shape")), RoundNumber.unsafe(data.coerced("roundNumber")))
            RoundEnded::class.simpleName -> RoundEnded(gameId, timestamp, RoundNumber.unsafe(data.coerced("roundNumber")))
            RoundStarted::class.simpleName -> RoundStarted(gameId, timestamp, RoundNumber.unsafe(data.coerced("roundNumber")))
            RoundTied::class.simpleName -> RoundTied(gameId, timestamp, RoundNumber.unsafe(data.coerced("roundNumber")))
            RoundWon::class.simpleName -> RoundWon(gameId, timestamp, RoundNumber.unsafe(data.coerced("roundNumber")), PlayerId(data.coerced("winner")))
            SecondPlayerJoinedGame::class.simpleName -> SecondPlayerJoinedGame(gameId, timestamp, PlayerId(data.coerced("playerId")))
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
}