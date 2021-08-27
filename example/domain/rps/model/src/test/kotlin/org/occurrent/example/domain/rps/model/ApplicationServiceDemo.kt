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

import com.thoughtworks.xstream.XStream
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
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
            handle(events, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))
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
                    handle(events, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))
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
                ::handle.partial(CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1))),
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
            ::handle.partial(CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1))) andThen
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
            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)),
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
    private val xstream = XStream().apply { allowTypesByWildcard(arrayOf("org.occurrent.**")) }

    override fun toCloudEvent(e: GameEvent): CloudEvent = CloudEventBuilder.v1()
        .withId(UUID.randomUUID().toString())
        .withSource(URI.create("urn:rockpaperscissors:gameplay"))
        .withType(e::class.qualifiedName)
        .withTime(OffsetDateTime.ofInstant(e.timestamp.value.toInstant(), e.timestamp.value.zone))
        .withDataContentType("application/xml")
        .withData(xstream.toXML(e).toByteArray())
        .build()

    override fun toDomainEvent(cloudEvent: CloudEvent): GameEvent =
        xstream.fromXML(String(cloudEvent.data!!.toBytes())) as GameEvent
}