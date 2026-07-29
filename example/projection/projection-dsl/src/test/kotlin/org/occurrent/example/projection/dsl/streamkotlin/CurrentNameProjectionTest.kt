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

package org.occurrent.example.projection.dsl.streamkotlin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.dsl.projection.blocking.project
import org.occurrent.dsl.subscription.blocking.streamSubscriptions
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@DisplayNameGeneration(ReplaceUnderscores::class)
class CurrentNameProjectionTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var applicationService: ApplicationService<NameEvent>
    private lateinit var converter: CloudEventConverter<NameEvent>

    @BeforeEach
    fun setup() {
        subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)
        converter = JacksonCloudEventConverter.Builder<NameEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:projection-dsl"))
            .typeMapper(ReflectionCloudEventTypeMapper.simple(NameEvent::class.java))
            .idMapper { UUID.randomUUID().toString() }
            .subjectMapper { it.userId }
            .build()
        applicationService = GenericApplicationService(eventStore, converter)
    }

    @AfterEach
    fun shutdown() {
        subscriptionModel.shutdown()
    }

    @Test
    fun `stream projection subscribes and materializes the current name`() {
        val store = ConcurrentHashMap<String, CurrentName>()
        val repository = ViewStateRepository.create<CurrentName?, String>({ store[it] }, { id, state -> store[id] = state })

        streamSubscriptions(subscriptionModel, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        applicationService.execute("u1") { listOf(NameDefined("u1", "Johan")) }
        applicationService.execute("u1") { listOf(NameChanged("u1", "Johan Haleby")) }

        assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue()
        assertThat(store["u1"]).isEqualTo(CurrentName("u1", "Johan Haleby"))
    }

    @Test
    fun `an explicit subject filter scopes the projection to a single user`() {
        val store = ConcurrentHashMap<String, CurrentName>()
        val repository = ViewStateRepository.create<CurrentName?, String>({ store[it] }, { id, state -> store[id] = state })

        streamSubscriptions(subscriptionModel, converter) {
            project("current-name-u1", currentNameProjectionForUser("u1"), repository)
        }

        applicationService.execute("u1") { listOf(NameDefined("u1", "Johan")) }
        applicationService.execute("u2") { listOf(NameDefined("u2", "Eve")) }

        assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue()
        assertThat(store["u1"]).isEqualTo(CurrentName("u1", "Johan"))
        // u2's event carries a different subject, so the filtered subscription never delivered it.
        assertThat(store["u2"]).isNull()
    }
}
