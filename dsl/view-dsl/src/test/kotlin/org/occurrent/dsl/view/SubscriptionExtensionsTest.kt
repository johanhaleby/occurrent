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

package org.occurrent.dsl.view

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.subscription.blocking.subscriptions
import org.occurrent.eventstore.api.blocking.write
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.util.*
import java.util.concurrent.ConcurrentHashMap


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class SubscriptionExtensionsTest {

    @Test
    fun `update view subscribes to all events that are listened to be the MaterializedView`() {
        // Given
        val view = View.create<String, DomainEvent>("initial") { _, e ->
            e.name()
        }
        val state = ConcurrentHashMap<String, String>()
        val viewStateRepository = ViewStateRepository.create<String, String>(state::get, state::put)

        val materializedView = MaterializedView.create(DomainEvent::userId, view, viewStateRepository)

        val subscriptionModel = InMemorySubscriptionModel()
        val converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:name"), DomainEvent::eventId)

        subscriptions(subscriptionModel, converter) {
            updateView("names", materializedView)
        }

        val eventStore = InMemoryEventStore(subscriptionModel)

        // When
        eventStore.write(
            "johan",
            sequenceOf(
                NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"),
                NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleb"),
                NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"),
            ).map(converter::toCloudEvent)
        )

        // Then
        Thread.sleep(500)
        val viewState = await untilCallTo { viewStateRepository.fetch("johan") } matches { name -> name == "Johan Haleby" }
        assertThat(viewState).isEqualTo("Johan Haleby")
    }
}