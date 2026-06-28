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

package org.occurrent.application.service.blocking.dcb

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.DomainEventConverter
import org.occurrent.domain.NameDefined
import org.occurrent.eventstore.api.dcb.DcbQuery.tags
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.time.LocalDateTime
import java.util.UUID

@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbApplicationServiceExtensionsTest {

    private lateinit var applicationService: GenericDcbApplicationService<DomainEvent>

    @BeforeEach
    fun setup() {
        val domainEventConverter = DomainEventConverter(ObjectMapper())
        val cloudEventConverter: CloudEventConverter<DomainEvent> =
            GenericCloudEventConverter(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent)
        applicationService = GenericDcbApplicationService(InMemoryEventStore(), cloudEventConverter) { setOf("name:1") }
    }

    @Test
    fun executeList_returns_the_append_result_when_events_are_produced() {
        val result = applicationService.executeList(tags("name:1")) { listOf(nameDefined("Johan")) }

        assertThat(result).isNotNull()
        assertThat(result!!.eventCount()).isEqualTo(1)
    }

    @Test
    fun executeList_returns_null_when_no_events_are_produced() {
        val result = applicationService.executeList(tags("name:1")) { emptyList() }

        assertThat(result).isNull()
    }

    @Test
    fun executeSequence_returns_the_append_result_when_events_are_produced() {
        val result = applicationService.executeSequence(tags("name:1")) { sequenceOf(nameDefined("Ada")) }

        assertThat(result).isNotNull()
        assertThat(result!!.eventCount()).isEqualTo(1)
    }

    @Test
    fun executeList_with_options_runs_the_side_effect_with_the_written_events() {
        val observed = mutableListOf<String>()
        val options = dcbSideEffect<DomainEvent, NameDefined> { observed += it.name() }

        val result = applicationService.executeList(tags("name:1"), options) { listOf(nameDefined("Grace")) }

        assertThat(result).isNotNull()
        assertThat(observed).containsExactly("Grace")
    }

    private fun nameDefined(name: String): NameDefined =
        NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name)
}
