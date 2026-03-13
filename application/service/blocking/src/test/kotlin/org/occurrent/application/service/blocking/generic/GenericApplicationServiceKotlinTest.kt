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
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.application.service.blocking.executeList
import org.occurrent.application.service.blocking.executeSequence
import org.occurrent.application.service.blocking.filter
import org.occurrent.application.service.blocking.options
import org.occurrent.application.service.blocking.sideEffect
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.DomainEventConverter
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.api.StreamReadFilter
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.time.LocalDateTime
import java.util.UUID

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
        val writeResult = applicationService.executeSequence(streamId) {
            sequenceOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

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

        applicationService.executeSequence(streamId) {
            sequenceOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

        // When
        val writeResult = applicationService.executeSequence(streamId) {
            sequenceOf(
                NameWasChanged("eventId3", time, "name", "Hello"),
                NameWasChanged("eventId4", time, "name", "World")
            )
        }

        // Then
        assertAll(
            { assertThat(writeResult.streamId()).isEqualTo(streamId) },
            { assertThat(writeResult.newStreamVersion()).isEqualTo(4L) }
        )
    }

    @Test
    fun execute_sequence_infers_sequence_without_explicit_lambda_type() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        val sideEffects = mutableListOf<String>()

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            sideEffect(
                { event: NameDefined -> sideEffects += "defined:${event.name()}" },
                { event: NameWasChanged -> sideEffects += "changed:${event.name()}" }
            )
        ) {
            sequenceOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

        // Then
        assertAll(
            { assertThat(writeResult.newStreamVersion()).isEqualTo(2L) },
            { assertThat(sideEffects).containsExactly("defined:Some Doe", "changed:Jane Doe") }
        )
    }

    @Test
    fun execute_sequence_accepts_options_helper_without_explicit_event_type() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        val sideEffects = mutableListOf<String>()

        // When
        applicationService.executeSequence(
            streamId,
            options().sideEffect(
                { event: NameDefined -> sideEffects += "defined:${event.name()}" },
                { event: NameWasChanged -> sideEffects += "changed:${event.name()}" }
            )
        ) {
            sequenceOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

        // Then
        assertThat(sideEffects).containsExactly("defined:Some Doe", "changed:Jane Doe")
    }

    @Test
    fun execute_list_infers_list_without_explicit_lambda_type() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()

        // When
        val writeResult = applicationService.executeList(streamId) {
            listOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(2L)
    }

    @Test
    fun execute_sequence_accepts_filter_helper_with_direct_imported_side_effect() {
        // Given
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        val sideEffects = mutableListOf<String>()

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            filter(StreamReadFilter.type(NameDefined::class.java.name)).sideEffect(
                { event: NameDefined -> sideEffects += "defined:${event.name()}" },
                { event: NameWasChanged -> sideEffects += "changed:${event.name()}" }
            )
        ) {
            sequenceOf(
                NameDefined("eventId1", time, "name", "Some Doe"),
                NameWasChanged("eventId2", time, "name", "Jane Doe")
            )
        }

        // Then
        assertAll(
            { assertThat(writeResult.newStreamVersion()).isEqualTo(2L) },
            { assertThat(sideEffects).containsExactly("defined:Some Doe", "changed:Jane Doe") }
        )
    }
}
