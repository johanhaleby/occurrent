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

package org.occurrent.dsl.projection.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.application.service.blocking.TransactionExecutor
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel
import java.net.URI
import java.util.Date
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Proves the read-your-writes (synchronous) delivery mode: because a projection is run on the synchronous subscription
 * model (PR #327), its materialized state is updated inside `execute(...)` and is visible immediately after it returns,
 * with no await. The DSL adds no parallel API for this: the same `project(...)` runner points at the synchronous model.
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
class SynchronousProjectionTest {

    @Test
    fun synchronous_projection_is_visible_immediately_after_execute_returns() {
        val converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection"), DomainEvent::eventId)
        val eventStore = InMemoryEventStore()
        val synchronousSubscriptions = SynchronousSubscriptionModel()

        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
        val currentName = projection<String, DomainEvent, String>(initialState = "") {
            id { it.userId() }
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, e -> e.name() }
        }

        // Register the projection's subscription on the synchronous model, then wire that model as the application
        // service's synchronous dispatcher so the write dispatches to it in-line.
        ProjectionRunner.agnostic(synchronousSubscriptions, converter).project("current-name", currentName, repository)

        val applicationService = GenericApplicationService.builder(eventStore, converter)
            .synchronousSubscriptions(synchronousSubscriptions)
            .transactionExecutor(TransactionExecutor.noTransaction())
            .build()

        applicationService.execute("johan") { _ ->
            listOf(NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))
        }

        // No await: the projection was updated synchronously, within execute(...).
        assertThat(store["johan"]).isEqualTo("Johan Haleby")
    }
}
