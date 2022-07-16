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

package org.occurrent.application.composition.command

import java.util.function.Function

// Sequence Composition

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

@JvmName("andThenDifferentTypes")
infix fun <A, B, R> ((A) -> B).andThen(anotherCommand: (B) -> R): (A) -> R = { a ->
    anotherCommand(this(a))
}

infix fun <T> ((Sequence<T>) -> Sequence<T>).andThen(anotherCommand: (Sequence<T>) -> Sequence<T>): (Sequence<T>) -> Sequence<T> =
    composeCommands(this, anotherCommand)

/**
 * Compose multiple commands
 */
fun <T> composeCommands(
    firstCommand: (Sequence<T>) -> Sequence<T>,
    secondCommand: (Sequence<T>) -> Sequence<T>,
    vararg additionalCommands: (Sequence<T>) -> Sequence<T>
): ((Sequence<T>) -> Sequence<T>) {
    return composeCommands(sequenceOf(firstCommand, secondCommand, *additionalCommands))
}

/**
 * Compose a sequence of commands
 */
fun <T> composeCommands(commands: Sequence<(Sequence<T>) -> Sequence<T>>): (Sequence<T>) -> Sequence<T> {
    val listCommands = commands.map { it.toMutableListCommandFromSequence() }.toList()
    return { events ->
        ListCommandComposition.composeCommands(listCommands).apply(events.toList()).asSequence()
    }
}

// List Composition
/**
 * Compose two commands using infix notation. The resulting command will be executed atomically in the event store.
 * For example:
 * ```kotlin
 * val cmd1 : (List<DomainEvent>) -> List<DomainEvent> = ..
 * val cmd2 : (List<DomainEvent>) -> List<DomainEvent> = ..
 * applicationService.execute("streamId", cmd1 andThen cmd2)
 * ```
 *
 * @param anotherCommand The other command to run after this one.
 */
@JvmName("andThenList")
infix fun <T> ((List<T>) -> List<T>).andThen(anotherCommand: (List<T>) -> List<T>): (List<T>) -> List<T> =
    composeCommands(this, anotherCommand)

/**
 * Compose multiple commands
 */
@JvmName("commandsListCommands")
fun <T> composeCommands(
    firstCommand: (List<T>) -> List<T>,
    secondCommand: (List<T>) -> List<T>,
    vararg additionalCommands: (List<T>) -> List<T>
): ((List<T>) -> List<T>) {
    return composeCommands(listOf(firstCommand, secondCommand, *additionalCommands))
}

/**
 * Compose a sequence of commands
 */
fun <T> composeCommands(commands: List<(List<T>) -> List<T>>): (List<T>) -> List<T> {
    val listCommands = commands.map { it.toMutableListCommandFromList() }
    return { events ->
        ListCommandComposition.composeCommands(listCommands).apply(events.toList())
    }
}

// Helpers
private fun <T> ((Sequence<T>) -> Sequence<T>).toMutableListCommandFromSequence(): Function<MutableList<T>, MutableList<T>> =
    Function<MutableList<T>, MutableList<T>> { events ->
        this(events.asSequence()).toMutableList()
    }

private fun <T> ((List<T>) -> List<T>).toMutableListCommandFromList(): Function<MutableList<T>, MutableList<T>> =
    Function<MutableList<T>, MutableList<T>> { events ->
        this(events).toMutableList()
    }