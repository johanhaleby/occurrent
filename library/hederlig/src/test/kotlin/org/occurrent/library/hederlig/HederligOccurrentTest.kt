/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.library.hederlig

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.Simple
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.command.ChangeName
import org.occurrent.command.Command
import org.occurrent.command.DefineName
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.library.hederlig.domain.AllNames
import org.occurrent.library.hederlig.domain.PersonNamed
import org.occurrent.library.hederlig.domain.DomainQuery
import org.occurrent.library.hederlig.initialization.occurrent.OccurrentHederligModuleInitializer
import org.occurrent.library.hederlig.model.Delay
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.time.LocalDateTime
import java.time.Year
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.util.*


@DisplayNameGeneration(Simple::class)
class HederligOccurrentTest {

    // TODO Idea: Introduce "bootstrap" as a function in ModuleDefiniton. Bootstrap can be an interface, thus we can provide a OccurrentBootStrapper that takes care of wiring everything together.
    // There can also be a Spring Starter project that creates a bean, "hederligOccurrentBootstraper", that one can inject when creating the module.
    @Test
    fun `example with occurrent`() {
        val cloudEventConverter: CloudEventConverter<DomainEvent> = JacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent"))
        val subscriptionModel = InMemorySubscriptionModel()
        val eventStore = InMemoryEventStore(subscriptionModel)
        val applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        val subscriptions = Subscriptions(subscriptionModel, cloudEventConverter)
        val queries = DomainEventQueries(eventStore, cloudEventConverter)
        val initializer = OccurrentHederligModuleInitializer<Command, DomainEvent, DomainQuery<out Any>>(applicationService, subscriptions, queries)

        val module = module<Command, DomainEvent, DomainQuery<out Any>> {
            feature("manage name") {
                commands {
                    command(DefineName::getId, Name::defineNameFromCommand)
                    command(ChangeName::getId, Name::changeNameFromCommand)
                }
                // Alternative 1
                commands(Command::getId) {
                    command(Name::defineNameFromCommand)
                    command(Name::changeNameFromCommand)
                }

                // Alternative 2 - When domain model doesn't use commands!
                commands {
                    command(DefineName::getId) { e, cmd ->
                        Name.defineName(e, cmd.id, cmd.time, cmd.name)
                    }
                    command<ChangeName>({ changeName -> changeName.id }) { e, cmd ->
                        Name.defineName(e, cmd.id, cmd.time, cmd.newName)
                    }
                }

                subscriptions {
                    on<NameDefined> { event ->
                        println("Name defined: ${event.name}")
                    }
                    on<NameWasChanged> { event, ctx ->
                        when (event.name) {
                            "John Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Forbidden Name"))
                            "Jane Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Mrs ${event.name}"), Delay.ofMinutes(10))
                            "Ikk Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "Hohoho"), Delay.until(ZonedDateTime.of(Year.now().value, 12, 25, 15, 0, 0, 0, UTC)))
                            "Baby Doe" -> println("Baby detected!")
                        }
                    }
                }
                queries {
                    query<AllNames> { ctx ->
                        ctx.queryForSequence<NameDefined>().map { e -> e.name }
                    }
                    query<PersonNamed> { (name), ctx ->
                        ctx.queryForSequence<NameDefined>().filter { e -> e.name == name }
                    }
                }
            }
        }.initialize(initializer)

        val id = UUID.randomUUID().toString()
        module.publish(DefineName(id, LocalDateTime.now(), "Some Doe"))
        module.publish(ChangeName(id, LocalDateTime.now(), "Baby Doe"))

        val names = module.query(AllNames)
        names.forEach(::println)
    }
}