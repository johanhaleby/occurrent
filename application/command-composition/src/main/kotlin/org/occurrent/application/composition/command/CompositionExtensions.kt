/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.application.composition.command

import java.util.function.Function


/**
 * Compose two commands using infix notation. The resulting command will be executed atomically in the event store.
 * For example:
 * ```kotlin
 * val cmd1 : (Sequence<DomainEvent>) -> Sequence<DomainEvent> = ..
 * val cmd2 : (Sequence<DomainEvent>) -> Sequence<DomainEvent> = ..
 * applicationService.execute("streamId", cmd1 andThen cmd2)
 * ```
 *
 * @param anotherCommand The other command to run after this one.
 */
infix fun <T> ((Sequence<T>) -> Sequence<T>).andThen(anotherCommand: (Sequence<T>) -> Sequence<T>): (Sequence<T>) -> Sequence<T> =
    composeCommands(this, anotherCommand)

fun <T> composeCommands(
    firstCommand: (Sequence<T>) -> Sequence<T>,
    secondCommand: (Sequence<T>) -> Sequence<T>,
    vararg additionalCommands: (Sequence<T>) -> Sequence<T>
): ((Sequence<T>) -> Sequence<T>) {
    val commands = sequenceOf(firstCommand, secondCommand, *additionalCommands).map { it.toMutableListCommand() }.toList()
    return { events ->
        ListCommandComposition.composeCommands(commands).apply(events.toList()).asSequence()
    }
}

private fun <T> ((Sequence<T>) -> Sequence<T>).toMutableListCommand(): Function<MutableList<T>, MutableList<T>> =
    Function<MutableList<T>, MutableList<T>> { events ->
        this(events.asSequence()).toMutableList()
    }