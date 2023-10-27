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

package org.occurrent.dsl.query.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.application.composition.command.composeCommands
import org.occurrent.application.composition.command.partial
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.api.SortBy
import org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.filter.Filter.type
import java.net.URI
import java.time.LocalDateTime


class DomainEventQueriesKotlinTest {

    private lateinit var applicationService: ApplicationService<DomainEvent>
    private lateinit var domainEventQueries: DomainEventQueries<DomainEvent>

    @BeforeEach
    fun createInstances() {
        val cloudEventConverter: CloudEventConverter<DomainEvent> = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        val eventStore = InMemoryEventStore()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        domainEventQueries = DomainEventQueries(eventStore, cloudEventConverter)
    }

    @Test
    fun queryForSequence() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val events = domainEventQueries.queryForSequence(skip = 1).toList()

        // Then
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events.stream().findFirst()).hasValue(NameWasChanged("eventId2", time, "Jane Doe")) }
        )
    }

    @Test
    fun queryForSequenceWithSpecificType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val sequence: Sequence<NameWasChanged> = domainEventQueries.queryForSequence(type(NameWasChanged::class.qualifiedName))

        // Then
        val events: List<NameWasChanged> = sequence.toList()
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events.stream().findFirst()).hasValue(NameWasChanged("eventId2", time, "Jane Doe")) }
        )
    }

    @Test
    fun querySingleWithSpecificType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val event = domainEventQueries.queryOne<NameWasChanged>(type(NameWasChanged::class.qualifiedName))

        // Then
        assertThat(event).isEqualTo(NameWasChanged("eventId2", time, "Jane Doe"))
    }

    @Test
    fun querySingleWithSpecificReifiedKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val event = domainEventQueries.queryOne<NameWasChanged>()

        // Then
        assertThat(event).isEqualTo(NameWasChanged("eventId2", time, "Jane Doe"))
    }


    @Test
    fun querySingleWithSpecificKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val event = domainEventQueries.queryOne(NameWasChanged::class)

        // Then
        assertThat(event).isEqualTo(NameWasChanged("eventId2", time, "Jane Doe"))
    }

    @Test
    fun queryForSequenceWithKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val sequence = domainEventQueries.queryForSequence(NameWasChanged::class)

        // Then
        val events: List<NameWasChanged> = sequence.toList()
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events.stream().findFirst()).hasValue(NameWasChanged("eventId2", time, "Jane Doe")) }
        )
    }

    @Test
    fun queryForSequenceWithMultipleKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe")
            )
        )

        // When
        val sequence = domainEventQueries.queryForSequence(NameDefined::class, NameWasChanged::class)

        // Then
        val events: List<DomainEvent> = sequence.toList()
        assertAll(
            { assertThat(events).hasSize(2) },
            { assertThat(events.map { it.eventId() }).containsOnly("eventId1", "eventId2") }
        )
    }

    @Test
    fun queryOneBasedOnReifiedClassTypeAndSortBy() {
        // Given
        val time = LocalDateTime.now()
        applicationService.execute(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "Some Doe"),
                Name::changeName.partial("eventId2", time, "Jane Doe"),
                Name::changeName.partial("eventId3", time, "Jane Doe2"),
            )
        )

        // When
        val nameWasChanged = domainEventQueries.queryOne<NameWasChanged>(sortBy = SortBy.natural(DESCENDING))

        // Then
        assertThat(nameWasChanged).isEqualTo(NameWasChanged("eventId3", time, "Jane Doe2"))
    }
}