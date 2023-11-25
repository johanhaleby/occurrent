/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.dsl.decider

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.command.ChangeName
import org.occurrent.command.DefineName
import org.occurrent.command.NameCommand
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.LocalDateTime
import java.util.*


class DeciderApplicationServiceExtensionsTest {

    private lateinit var decider: Decider<NameCommand, String?, DomainEvent>
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var cloudEventConverter: CloudEventConverter<DomainEvent>
    private lateinit var applicationService: ApplicationService<DomainEvent>

    @BeforeEach
    fun configureDecider() {
        decider = decider(
            initialState = null,
            decide = { cmd, state ->
                when (cmd) {
                    is DefineName -> Name.defineTheName(cmd.id, cmd.time, cmd.name)
                    is ChangeName -> Name.changeNameFromCurrent(cmd.id, cmd.time, state, cmd.newName)
                }
            },
            evolve = { _, e ->
                when (e) {
                    is NameDefined -> e.name
                    is NameWasChanged -> e.name
                }
            }
        )
    }

    @BeforeEach
    fun configureEventStoreAndApplicationService() {
        eventStore = InMemoryEventStore()
        cloudEventConverter = jacksonCloudEventConverter<DomainEvent>(ObjectMapper(), URI.create("urn:source"))
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
    }

    @AfterEach
    fun deleteAllEventsInEventStore() {
        eventStore.deleteAll()
    }

    @Test
    fun executeAndReturnDecision() {
        // Given
        val streamId = UUID.randomUUID()
        val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

        // When
        val (state, events) = applicationService.executeAndReturnDecision(streamId, command, decider)

        // Then
        assertAll(
            { assertThat(state).isEqualTo("Johan") },
            { assertThat(events).containsOnly(NameDefined(command.id, command.time, command.name)) },
            { assertThat(eventStore.allEvents()).containsOnly(NameDefined(command.id, command.time, command.name)) },
        )
    }

    private fun InMemoryEventStore.allEvents(): List<DomainEvent> = eventStore.all().map { cloudEventConverter.toDomainEvent(it) }.toList()
}