/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.application.service.blocking.generic

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.occurrent.application.composition.command.composeCommands
import org.occurrent.application.composition.command.partial
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.application.service.blocking.execute
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.DomainEventConverter
import org.occurrent.domain.Name
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.time.LocalDateTime
import java.util.*

@DisplayName("generic application service - kotlin")
class GenericApplicationServiceKotlinTest {
    private lateinit var applicationService: org.occurrent.application.service.blocking.ApplicationService<DomainEvent>
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var cloudEventConverter: CloudEventConverter<DomainEvent>

    @BeforeEach
    fun `application service initialization`() {
        val domainEventConverter = DomainEventConverter(ObjectMapper())
        cloudEventConverter = GenericCloudEventConverter(
            domainEventConverter::convertToDomainEvent,
            domainEventConverter::convertToCloudEvent
        )
        eventStore = InMemoryEventStore()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
    }

    @Test
    fun returns_write_result() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()

        // When
        val writeResult = applicationService.execute(
            streamId, composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        )
        // Then
        assertAll(
            { assertThat(writeResult.streamId()).isEqualTo(streamId) },
            { assertThat(writeResult.newStreamVersion()).isEqualTo(2L) }
        )
    }

    @Test
    fun composes_commands_correctly_when_stream_already_have_events() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()

        applicationService.execute(
            streamId, composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        )

        // When
        val writeResult = applicationService.execute(
            streamId, composeCommands(
                Name::changeName.partial("eventId3", time, "name", "Hello"),
                Name::changeName.partial("eventId4", time, "name", "World")
            )
        )
        // Then
        assertAll(
            { assertThat(writeResult.streamId()).isEqualTo(streamId) },
            { assertThat(writeResult.newStreamVersion()).isEqualTo(4L) }
        )
    }

}