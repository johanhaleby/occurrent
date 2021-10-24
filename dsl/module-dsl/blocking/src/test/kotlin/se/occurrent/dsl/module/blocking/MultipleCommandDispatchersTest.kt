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
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.Test
import org.occurrent.application.composition.command.partial
import org.occurrent.application.converter.generic.GenericCloudEventConverter
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.command.ChangeName
import org.occurrent.command.Command
import org.occurrent.command.DefineName
import org.occurrent.domain.*
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import se.occurrent.dsl.module.blocking.ApplicationServiceCommandDispatcher.Companion.dispatchTo
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.MILLIS
import java.util.concurrent.CopyOnWriteArrayList

class MultipleCommandDispatchersTest {

    private val log = loggerFor<ModuleConfigurationTest>()

    @Test
    fun `multiple command dispatchers`() {
        // Given
        val domainEventConverter = DomainEventConverter(ObjectMapper())
        val cloudEventConverter = GenericCloudEventConverter(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent)
        val subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)
        val applicationService = GenericApplicationService(eventStore, cloudEventConverter)

        val allEvents = CopyOnWriteArrayList<DomainEvent>()

        // Module Configuration
        val module = module<Command, DomainEvent>(cloudEventConverter, eventNameFromType = { e -> e.qualifiedName!! }) {
            commands(dispatchTo(applicationService)) {
                command(DefineName::getId, Name::defineNameFromCommand)
            }
            commands { cmd ->
                when (cmd) {
                    is ChangeName -> applicationService.execute(cmd.id, Name::changeNameFromCommand.partial(cmd))
                }
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
        await withPollInterval Duration.of(10, MILLIS) untilAsserted {
            assertThat(allEvents).hasSize(20)
        }
        assertThat(allEvents.map { e -> e.name.substringAfter(":").toInt() }).isSubsetOf(0 until 10)
    }
}