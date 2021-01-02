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

package se.occurrent.application.module.dsl.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.command.ChangeName
import org.occurrent.command.Command
import org.occurrent.command.DefineName
import org.occurrent.domain.*
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.concurrent.CopyOnWriteArrayList

inline fun <reified T : Any> loggerFor(): Logger = LoggerFactory.getLogger(T::class.java)

class ModuleTest {

    private val log = loggerFor<ModuleTest>()

    @Test
    fun `module configuration`() {
        // Given
        val domainEventConverter = DomainEventConverter(ObjectMapper())
        val cloudEventConverter = GenericCloudEventConverter(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent)
        val subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)

        val allEvents = CopyOnWriteArrayList<DomainEvent>()

        // Module Configuration
        val module = module<Command, DomainEvent>(cloudEventConverter, eventStore, { e -> e.qualifiedName!! }) {
            commands {
                command(DefineName::getId, Name::defineName)
                command(ChangeName::getId, Name::changeName)
            }
            subscriptions(subscriptionModel) {
                subscribe<NameDefined>("nameDefined") { e ->
                    log.info("Hello ${e.name}")
                }
                subscribe<NameWasChanged>("nameChanged") { e ->
                    log.info("Changed name to ${e.name}")
                }
                subscribe("everything") { e ->
                    allEvents.add(e)
                }
            }
        }

        // When
        repeat(10) { count ->
            val streamId = count.toString()
            module.dispatch(
                DefineName(streamId, LocalDateTime.now(), "Johan:$streamId"),
                ChangeName(streamId, LocalDateTime.now(), "Eric:$streamId")
            )
        }

        // Then
        assertAll(
            { assertThat(allEvents).hasSize(20) },
            { assertThat(allEvents.map { e -> e.name.substringAfter(":").toInt() }).isSubsetOf(0 until 10) }
        )
    }
}