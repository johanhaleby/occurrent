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
import org.junit.jupiter.api.*
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
import org.occurrent.eventstore.api.WriteResult
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.LocalDateTime
import java.util.*
import java.util.stream.Stream


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

    @Nested
    @DisplayName("execute")
    inner class Execute {

        @Nested
        @DisplayName("when streamid is a UUID")
        inner class IsUUID {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val result = applicationService.execute(streamId, command, decider)

                // Then
                assertThat(result).isEqualTo(WriteResult(streamId.toString(), 0, 1))
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val result = applicationService.execute(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertThat(result).isEqualTo(WriteResult(streamId.toString(), 0, 2))
            }
        }

        @Nested
        @DisplayName("when streamid is a String")
        inner class IsString {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val result = applicationService.execute(streamId, command, decider)

                // Then
                assertThat(result).isEqualTo(WriteResult(streamId, 0, 1))
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val result = applicationService.execute(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertThat(result).isEqualTo(WriteResult(streamId, 0, 2))
            }
        }
    }

    @Nested
    @DisplayName("executeAndReturnDecision")
    inner class ExecuteAndReturnDecision {

        @Nested
        @DisplayName("when streamid is a UUID")
        inner class IsUUID {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val (state, events) = applicationService.executeAndReturnDecision(streamId, command, decider)

                // Then
                assertAll(
                    { assertThat(state).isEqualTo("Johan") },
                    { assertThat(events).containsOnly(NameDefined(command.id, command.time, command.name)) },
                    { assertThat(eventStore.domainEvents()).containsOnly(NameDefined(command.id, command.time, command.name)) },
                )
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val (state, events) = applicationService.executeAndReturnDecision(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertAll(
                    { assertThat(state).isEqualTo("Johan") },
                    {
                        assertThat(events).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName)
                        )
                    },
                    {
                        assertThat(eventStore.domainEvents()).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName),
                        )
                    },
                )
            }
        }

        @Nested
        @DisplayName("when streamid is a String")
        inner class IsString {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val (state, events) = applicationService.executeAndReturnDecision(streamId, command, decider)

                // Then
                assertAll(
                    { assertThat(state).isEqualTo("Johan") },
                    { assertThat(events).containsOnly(NameDefined(command.id, command.time, command.name)) },
                    { assertThat(eventStore.domainEvents()).containsOnly(NameDefined(command.id, command.time, command.name)) },
                )
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val (state, events) = applicationService.executeAndReturnDecision(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertAll(
                    { assertThat(state).isEqualTo("Johan") },
                    {
                        assertThat(events).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName)
                        )
                    },
                    {
                        assertThat(eventStore.domainEvents()).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName),
                        )
                    },
                )
            }
        }
    }


    @Nested
    @DisplayName("executeAndReturnState")
    inner class ExecuteAndReturnState {

        @Nested
        @DisplayName("when streamid is a UUID")
        inner class IsUUID {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val state = applicationService.executeAndReturnState(streamId, command, decider)

                // Then
                assertThat(state).isEqualTo("Johan")
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val state = applicationService.executeAndReturnState(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertThat(state).isEqualTo("Johan")
            }
        }

        @Nested
        @DisplayName("when streamid is a String")
        inner class IsString {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val state = applicationService.executeAndReturnState(streamId, command, decider)

                // Then
                assertThat(state).isEqualTo("Johan")
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val state = applicationService.executeAndReturnState(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertThat(state).isEqualTo("Johan")
            }
        }
    }

    @Nested
    @DisplayName("executeAndReturnEvents")
    inner class ExecuteAndReturnEvents {

        @Nested
        @DisplayName("when streamid is a UUID")
        inner class IsUUID {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val events = applicationService.executeAndReturnEvents(streamId, command, decider)

                // Then
                assertAll(
                    { assertThat(events).containsOnly(NameDefined(command.id, command.time, command.name)) },
                    { assertThat(eventStore.domainEvents()).containsOnly(NameDefined(command.id, command.time, command.name)) },
                )
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val events = applicationService.executeAndReturnEvents(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertAll(
                    {
                        assertThat(events).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName)
                        )
                    },
                    {
                        assertThat(eventStore.domainEvents()).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName),
                        )
                    },
                )
            }
        }

        @Nested
        @DisplayName("when streamid is a String")
        inner class IsString {

            @Test
            fun and_single_command() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val events = applicationService.executeAndReturnEvents(streamId, command, decider)

                // Then
                assertAll(
                    { assertThat(events).containsOnly(NameDefined(command.id, command.time, command.name)) },
                    { assertThat(eventStore.domainEvents()).containsOnly(NameDefined(command.id, command.time, command.name)) },
                )
            }

            @Test
            fun and_list_of_commands() {
                // Given
                val streamId = UUID.randomUUID().toString()
                val command1 = DefineName(UUID.randomUUID().toString(), LocalDateTime.now(), "John")
                val command2 = ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Johan")

                // When
                val events = applicationService.executeAndReturnEvents(streamId, commands = listOf(command1, command2), decider)

                // Then
                assertAll(
                    {
                        assertThat(events).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName)
                        )
                    },
                    {
                        assertThat(eventStore.domainEvents()).containsOnly(
                            NameDefined(command1.id, command1.time, command1.name),
                            NameWasChanged(command2.id, command2.time, command2.newName),
                        )
                    },
                )
            }
        }
    }

    private fun InMemoryEventStore.domainEvents(): Stream<DomainEvent> = all().map { cloudEventConverter.toDomainEvent(it) }
}