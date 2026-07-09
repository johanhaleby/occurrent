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

package org.occurrent.application.composition.command

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.time.LocalDateTime
import java.util.*

class ListCommandCompositionKotlinTest {

    private lateinit var eventStore: InMemoryEventStore
    private lateinit var applicationService: ApplicationService

    @BeforeEach
    fun `configure event store and application service`() {
        eventStore = InMemoryEventStore()
        applicationService = ApplicationService(eventStore, ObjectMapper())
    }

    @Test
    fun `compose list commands using andThen`() {
        // Given
        val eventId1 = UUID.randomUUID().toString()
        val eventId2 = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        // When
        applicationService.executeListCommand(
            "name1",
            NameWithListCommand::defineName.partial(eventId1, now, "name", "My Name 1")
                andThen NameWithListCommand::changeName.partial(eventId2, now, "name", "My Name 2")
        )

        // Then
        val domainEvents = eventStore.read("name1").map(applicationService::convertCloudEventToDomainEvent).toList()

        assertThat(domainEvents.map { event -> event.javaClass.simpleName }).containsExactly(
            NameDefined::class.java.simpleName,
            NameWasChanged::class.java.simpleName
        )
        assertThat(domainEvents.map { event -> event.name() }).containsExactly("My Name 1", "My Name 2")
    }

    @Test
    fun `compose list commands using composeCommands`() {
        // Given
        val eventId1 = UUID.randomUUID().toString()
        val eventId2 = UUID.randomUUID().toString()
        val eventId3 = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        // When
        applicationService.executeListCommand(
            "name1",
            composeCommands(
                NameWithListCommand::defineName.partial(eventId1, now, "name", "My Name 1"),
                NameWithListCommand::changeName.partial(eventId2, now, "name", "My Name 2"),
                NameWithListCommand::changeName.partial(eventId3, now, "name", "My Name 3")
            )
        )

        // Then
        val domainEvents = eventStore.read("name1").map(applicationService::convertCloudEventToDomainEvent).toList()

        assertThat(domainEvents.map { event -> event.javaClass.simpleName }).containsExactly(
            NameDefined::class.java.simpleName,
            NameWasChanged::class.java.simpleName,
            NameWasChanged::class.java.simpleName
        )
        assertThat(domainEvents.map { event -> event.name() }).containsExactly("My Name 1", "My Name 2", "My Name 3")
    }

    @Test
    fun `compose list commands using composeCommands when an event already exists`() {
        // Given
        val eventId1 = UUID.randomUUID().toString()
        val eventId2 = UUID.randomUUID().toString()
        val eventId3 = UUID.randomUUID().toString()
        val eventId4 = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        // When
        applicationService.executeListCommand(
            "name1",
            composeCommands(
                NameWithListCommand::defineName.partial(eventId1, now, "name", "My Name 1"),
                NameWithListCommand::changeName.partial(eventId2, now, "name", "My Name 2")
            )
        )

        applicationService.executeListCommand(
            "name1",
            composeCommands(
                NameWithListCommand::changeName.partial(eventId3, now, "name", "My Name 3"),
                NameWithListCommand::changeName.partial(eventId4, now, "name", "My Name 4")
            )
        )

        // Then
        val domainEvents = eventStore.read("name1").map(applicationService::convertCloudEventToDomainEvent).toList()

        assertThat(domainEvents.map { event -> event.javaClass.simpleName }).containsExactly(
            NameDefined::class.java.simpleName,
            NameWasChanged::class.java.simpleName,
            NameWasChanged::class.java.simpleName,
            NameWasChanged::class.java.simpleName
        )
        assertThat(domainEvents.map { event -> event.name() }).containsExactly(
            "My Name 1",
            "My Name 2",
            "My Name 3",
            "My Name 4"
        )
    }

    @Test
    fun `compose commands should only return new commands`() {
        val eventId1 = UUID.randomUUID().toString()
        val eventId2 = UUID.randomUUID().toString()
        val eventId3 = UUID.randomUUID().toString()
        val eventId4 = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        val events = composeCommands(
            NameWithListCommand::changeName.partial(eventId2, now, "name", "My Name 2"),
            NameWithListCommand::changeName.partial(eventId3, now, "name", "My Name 3"),
            NameWithListCommand::changeName.partial(eventId4, now, "name", "My Name 4")
        )
        val currentEvents = NameWithListCommand.defineName(emptyList(), eventId1, now, "name", "My Name 1")
        val newEvents = events(currentEvents)
        assertThat(newEvents.size).isEqualTo(3)
        assertThat(newEvents.map { event -> event.name() }).containsExactly(
            "My Name 2",
            "My Name 3",
            "My Name 4"
        )
    }
}
