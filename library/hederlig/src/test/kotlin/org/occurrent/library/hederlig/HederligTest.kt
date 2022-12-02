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
import org.occurrent.command.Command
import org.occurrent.command.DefineName
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.library.hederlig.model.Delay
import org.occurrent.library.hederlig.domain.NameQuery
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
        module<Command, DomainEvent, NameQuery> {
            feature("manage name") {
                commands {
                    command(DefineName::getId, Name::defineNameFromCommand)
                    command(ChangeName::getId, Name::changeNameFromCommand)
                }
                // Alternative
                commands(Command::getId) {
                    command(Name::defineNameFromCommand)
                    command(Name::changeNameFromCommand)
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
                    query<AllNames> { q ->
                        
                    }
                }
            }
        }

    }
}