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
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.application.service.blocking.ExecuteFilters
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
import org.occurrent.eventstore.api.StreamReadFilter.type
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.streams.asStream

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
            filter(type(NameDefined::class.java.name)).sideEffect(
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
    fun execute_sequence_accepts_typed_execute_filter_without_explicit_event_type() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            options().filter(ExecuteFilters.type<NameDefined>())
        ) {
            sequenceOf(NameWasChanged("eventId3", time, "name", "New Name"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    @Test
    fun execute_sequence_accepts_direct_type_helper_with_uuid_stream_id() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID()
        val time = LocalDateTime.now()
        eventStore.write(streamId.toString(), cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            ExecuteFilters.type<NameDefined>()
        ) {
            sequenceOf(NameDefined("eventId3", time, "name", "Filtered Input Still Writes"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    @Test
    fun execute_sequence_accepts_direct_include_types_helper() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            ExecuteFilters.includeTypes(NameDefined::class, NameWasChanged::class)
        ) {
            sequenceOf(NameWasChanged("eventId3", time, "name", "Still Writes"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    @Test
    fun execute_sequence_accepts_direct_exclude_types_helper() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeSequence(
            streamId,
            ExecuteFilters.excludeTypes(NameWasChanged::class, NameDefined::class)
        ) {
            sequenceOf(NameDefined("eventId3", time, "name", "Filtered Input Still Writes"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    @Test
    fun execute_list_accepts_options_with_typed_execute_filter() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID().toString()
        val time = LocalDateTime.now()
        eventStore.write(streamId, cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeList(
            streamId,
            options().filter(ExecuteFilters.type<NameDefined>())
        ) {
            listOf(NameWasChanged("eventId3", time, "name", "New Name"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    @Test
    fun execute_list_accepts_direct_exclude_types_helper_with_uuid_stream_id() {
        // Given
        cloudEventConverter = customCloudEventConverter()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val streamId = UUID.randomUUID()
        val time = LocalDateTime.now()
        eventStore.write(streamId.toString(), cloudEventConverter.toCloudEvents(sequenceOf(
            NameDefined("eventId1", time, "name", "Some Doe"),
            NameWasChanged("eventId2", time, "name", "Jane Doe")
        ).asStream()))

        // When
        val writeResult = applicationService.executeList(
            streamId,
            ExecuteFilters.excludeTypes(NameWasChanged::class, NameDefined::class)
        ) {
            listOf(NameDefined("eventId3", time, "name", "Filtered Input Still Writes"))
        }

        // Then
        assertThat(writeResult.newStreamVersion()).isEqualTo(3L)
    }

    private fun customCloudEventConverter(): CloudEventConverter<DomainEvent> {
        val objectMapper = ObjectMapper()
        return GenericCloudEventConverter(
            { cloudEvent: CloudEvent ->
                when (cloudEvent.type) {
                    "name-defined-v1" -> objectMapper.readValue(cloudEvent.data!!.toBytes(), NameDefined::class.java)
                    "name-was-changed-v1" -> objectMapper.readValue(cloudEvent.data!!.toBytes(), NameWasChanged::class.java)
                    else -> error("Unsupported event type ${cloudEvent.type}")
                }
            },
            { event: DomainEvent ->
                CloudEventBuilder.v1()
                    .withId(event.eventId())
                    .withSource(URI.create("http://name"))
                    .withType(customCloudEventType(event.javaClass))
                    .withTime(event.timestamp().toInstant().atOffset(ZoneOffset.UTC))
                    .withSubject(event.name())
                    .withDataContentType("application/json")
                    .withData(objectMapper.writeValueAsBytes(event))
                    .build()
            },
            ::customCloudEventType
        )
    }

    private fun customCloudEventType(eventType: Class<out DomainEvent>): String =
        when (eventType) {
            NameDefined::class.java -> "name-defined-v1"
            NameWasChanged::class.java -> "name-was-changed-v1"
            else -> eventType.name
        }
}
