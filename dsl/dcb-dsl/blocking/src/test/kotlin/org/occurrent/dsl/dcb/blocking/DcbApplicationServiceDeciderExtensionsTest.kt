/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.dsl.dcb.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.toDcb
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI
import java.time.LocalDateTime
import java.util.stream.Stream

@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbApplicationServiceDeciderExtensionsTest {

    private lateinit var eventStore: InMemoryEventStore
    private lateinit var cloudEventConverter: CloudEventConverter<DomainEvent>
    private lateinit var applicationService: DcbApplicationService<DomainEvent>
    private lateinit var time: LocalDateTime

    @BeforeEach
    fun createInstances() {
        eventStore = InMemoryEventStore()
        cloudEventConverter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        applicationService = GenericDcbApplicationService(
            eventStore,
            cloudEventConverter,
            { event: DomainEvent -> setOf(tagFor(event)) },
            GenericDcbApplicationService.defaultRetryStrategy()
        )
        time = LocalDateTime.now()
    }

    @Test
    fun command_execution_appends_decider_produced_events() {
        val result = applicationService.execute(DefineName("Jane Doe"), nameDcbDecider())

        assertThat(result).isNotNull()
        assertThat(result!!.eventCount()).isEqualTo(1)
        assertThat(readNameEvents("name")).containsExactly(NameDefined("event-1", time, "name", "Jane Doe"))
    }

    @Test
    fun no_op_decisions_return_null() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val result = applicationService.execute(DefineName("Jane Doe"), nameDcbDecider())

        assertThat(result).isNull()
        assertThat(readNameEvents("name")).containsExactly(NameDefined("event-0", time, "name", "Jane Doe"))
    }

    @Test
    fun executeAndReturnDecision_returns_folded_state_plus_new_events() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val decision = applicationService.executeAndReturnDecision(ChangeName("John Doe"), nameDcbDecider())

        assertThat(decision.state).isEqualTo("John Doe")
        assertThat(decision.events).containsExactly(NameWasChanged("event-2", time, "name", "John Doe"))
        assertThat(readNameEvents("name")).containsExactly(
            NameDefined("event-0", time, "name", "Jane Doe"),
            NameWasChanged("event-2", time, "name", "John Doe")
        )
    }

    @Test
    fun multiple_commands_are_handled_in_order() {
        val newEvents = applicationService.executeAndReturnEvents(
            listOf(DefineName("Jane Doe"), ChangeName("John Doe")),
            nameDcbDecider()
        )

        assertThat(newEvents).containsExactly(
            NameDefined("event-1", time, "name", "Jane Doe"),
            NameWasChanged("event-2", time, "name", "John Doe")
        )
        assertThat(applicationService.executeAndReturnState(NoOp, nameDcbDecider())).isEqualTo("John Doe")
        assertThat(readNameEvents("name")).containsExactlyElementsOf(newEvents)
    }

    @Test
    fun a_batch_whose_commands_resolve_to_different_criteria_is_rejected() {
        // A decider whose read boundary depends on the command, so two commands can resolve to different boundaries.
        // A single execute appends atomically under one condition, so a mixed-boundary batch must fail loudly.
        val perCommandDecider = nameDecider().toDcb(
            criteria = { command ->
                when (command) {
                    is DefineName -> nameQuery(command.name)
                    is ChangeName -> nameQuery(command.name)
                    NoOp -> nameQuery("name")
                }
            },
            tags = { event -> setOf(tagFor(event)) }
        )

        assertThatThrownBy {
            applicationService.execute(listOf(DefineName("Jane Doe"), ChangeName("John Doe")), perCommandDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun execute_throws_on_a_command_the_decider_does_not_recognize() {
        val unrecognizingDecider = DcbDecider.from(
            nameDecider(),
            { command -> if (command is DefineName) nameQuery("name") else null },
            TagGenerator { event -> setOf(tagFor(event)) }
        )

        assertThatThrownBy {
            applicationService.execute(ChangeName("John Doe"), unrecognizingDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun executeAndReturnDecision_throws_on_a_command_the_decider_does_not_recognize() {
        val unrecognizingDecider = DcbDecider.from(
            nameDecider(),
            { command -> if (command is DefineName) nameQuery("name") else null },
            TagGenerator { event -> setOf(tagFor(event)) }
        )

        assertThatThrownBy {
            applicationService.executeAndReturnDecision(ChangeName("John Doe"), unrecognizingDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun a_batch_mixing_a_recognized_and_an_unrecognized_command_reports_the_unrecognized_command_not_a_boundary_mismatch() {
        val unrecognizingDecider = DcbDecider.from(
            nameDecider(),
            { command -> if (command is DefineName) nameQuery("name") else null },
            TagGenerator { event -> setOf(tagFor(event)) }
        )

        assertThatThrownBy {
            applicationService.execute(listOf(DefineName("Jane Doe"), ChangeName("John Doe")), unrecognizingDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("does not recognize")
    }

    @Test
    fun decider_over_a_narrower_event_type_runs_without_an_explicit_adaptEvents() {
        // Given a decider whose event type (NameDefined) is a subtype of the service's event type (DomainEvent).
        // Its state is nullable (null means "no name yet"), which toDcb and execute now accept.
        val narrowDecider: Decider<DefineName, String?, NameDefined> = decider(
            initialState = null,
            decide = { command, _ -> listOf(NameDefined("event-1", time, "name", command.name)) },
            evolve = { _, event -> event.name() }
        )
        val narrowDcbDecider = narrowDecider.toDcb(
            criteria = { nameQuery("name") },
            tags = { event -> setOf(tagFor(event)) }
        )

        // When passed straight to execute, no narrowDecider.adaptEvents() needed
        val result = applicationService.execute(DefineName("Jane Doe"), narrowDcbDecider)

        // Then
        assertThat(result).isNotNull()
        assertThat(readNameEvents("name")).containsExactly(NameDefined("event-1", time, "name", "Jane Doe"))
    }

    // executeAndReturnState returns the state directly and cannot carry null, so this decider uses a non-null String, with an empty string for "no name yet".
    private fun nameDecider(): Decider<NameCommand, String, DomainEvent> =
        decider(
            initialState = "",
            decide = { command, state ->
                when (command) {
                    is DefineName -> if (state.isEmpty()) listOf(NameDefined("event-1", time, "name", command.name)) else emptyList()
                    is ChangeName -> listOf(NameWasChanged("event-2", time, "name", command.name))
                    NoOp -> emptyList()
                }
            },
            evolve = { _, event -> event.name() }
        )

    private fun nameDcbDecider() = nameDecider().toDcb(
        criteria = { nameQuery("name") },
        tags = { event -> setOf(tagFor(event)) }
    )

    private fun append(vararg events: DomainEvent) {
        val cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(*events))
            .map { event -> DcbCloudEvents.withTags(event, setOf(Tag.of("name", "name"))) }
            .toList()
        eventStore.append(cloudEvents)
    }

    private fun readNameEvents(nameId: String): List<DomainEvent> =
        cloudEventConverter.toDomainEvents(eventStore.read(nameQuery(nameId)).stream()).toList()

    private fun nameQuery(nameId: String): DcbCriteria = DcbCriteria.tags(Tag.of("name", nameId))

    private fun tagFor(event: DomainEvent): Tag = Tag.of("name", event.userId())

    private sealed interface NameCommand
    private data class DefineName(val name: String) : NameCommand
    private data class ChangeName(val name: String) : NameCommand
    private data object NoOp : NameCommand
}
