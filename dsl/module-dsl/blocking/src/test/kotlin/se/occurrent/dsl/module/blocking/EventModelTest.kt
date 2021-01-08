/*
 * Copyright 2021 Johan Haleby
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

package se.occurrent.dsl.module.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.command.ChangeName
import org.occurrent.command.Command
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.DomainEventConverter
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import se.occurrent.dsl.module.blocking.em.eventModel
import java.util.concurrent.CopyOnWriteArrayList


class EventModelTest {

    private val log = loggerFor<EventModelTest>()

    @Test
    fun `event model example`() {
        // Given
        val domainEventConverter = DomainEventConverter(ObjectMapper())
        val cloudEventConverter = GenericCloudEventConverter(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent)
        val subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)

        val allNames = CopyOnWriteArrayList<String>()

        eventModel<Command, DomainEvent>(eventStore, cloudEventConverter) {
            slice("name") {
                wireframe("some ui").
                command(ChangeName::getId, Name::defineName).
                event<NameDefined>().
                updateView("AllNames") { nameDefined ->
                    allNames.add(nameDefined.name)
                }
            }
        }
    }
}