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

import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.Simple
import org.junit.jupiter.api.Test
import org.occurrent.command.ChangeName
import org.occurrent.command.DefineName
import org.occurrent.command.NameCommand
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.library.hederlig.domain.AllNames
import org.occurrent.library.hederlig.domain.DomainQuery
import org.occurrent.library.hederlig.domain.PersonNamed
import org.occurrent.library.hederlig.model.Delay
import java.time.LocalDateTime
import java.time.Year
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.util.*


@DisplayNameGeneration(Simple::class)
class HederligTest {

    // TODO Idea: Introduce "bootstrap" as a function in ModuleDefiniton. Bootstrap can be an interface, thus we can provide a OccurrentBootStrapper that takes care of wiring everything together.
    // There can also be a Spring Starter project that creates a bean, "hederligOccurrentBootstraper", that one can inject when creating the module.
    @Test
    fun `example`() {
        module<NameCommand, DomainEvent, DomainQuery<out Any>> {
            feature("manage name") {
                commands {
                    command(DefineName::userId, Name::defineNameFromCommand)
                    command(ChangeName::userId, Name::changeNameFromCommand)
                }
                // Alternative 1
                commands(NameCommand::userId) {
                    command(Name::defineNameFromCommand)
                    command(Name::changeNameFromCommand)
                }

                // Alternative 2 - When domain model doesn't use commands!
                commands {
                    command(DefineName::commandId) { e, cmd ->
                        Name.defineName(e, cmd.commandId, cmd.time, cmd.userId, cmd.name)
                    }
                    command<ChangeName>({ changeName -> changeName.userId }) { e, cmd ->
                        Name.defineName(e, cmd.commandId, cmd.time(), cmd.userId, cmd.newName)
                    }
                }

                subscriptions {
                    on<NameDefined> { event ->
                        println("Name defined: ${event.name()}")
                    }
                    on<NameWasChanged> { event, ctx ->
                        when (event.name()) {
                            "John Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "johndoe", "Forbidden Name"))
                            "Jane Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "jandoe", "Mrs ${event.name()}"), Delay.ofMinutes(10))
                            "Ikk Doe" -> ctx.publish(ChangeName(UUID.randomUUID().toString(), LocalDateTime.now(), "santa", "Hohoho"), Delay.until(ZonedDateTime.of(Year.now().value, 12, 25, 15, 0, 0, 0, UTC)))
                            "Baby Doe" -> println("Baby detected!")
                        }
                    }
                }
                queries {
                    query<AllNames> { ctx ->
                        ctx.queryForSequence<NameDefined>().map { e -> e.name() }
                    }
                    query<PersonNamed> { (name), ctx ->
                        ctx.queryForSequence<NameDefined>().filter { e -> e.name() == name }
                    }
                }
            }
        }
    }
}