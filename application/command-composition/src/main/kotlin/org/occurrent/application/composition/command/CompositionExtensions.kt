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

/**
 * Backward compose two commands using infix notation. The resulting command will be executed atomically in the event store.
 * For example:
 * ```kotlin
 * val cmd1 : (Sequence<DomainEvent>) -> Sequence<DomainEvent> = ..
 * val cmd2 : (Sequence<DomainEvent>) -> Sequence<DomainEvent> = ..
 * applicationService.execute("streamId", cmd1 andThen cmd2)
 * ```
 *
 * @param anotherCommand The other command to run after this one.
 */
infix fun <A, B, R> ((A) -> B).andThen(anotherCommand: (B) -> R): (A) -> R = { a ->
    anotherCommand(this(a))
}

/**
 * Compose multiple commands
 */
fun <T> composeCommands(
    firstCommand: (T) -> T,
    secondCommand: (T) -> T,
    vararg additionalCommands: (T) -> T
): ((T) -> T) {
    return composeCommands(sequenceOf(firstCommand, secondCommand, *additionalCommands))
}

/**
 * Compose a sequence of commands
 */
fun <T> composeCommands(commands: Sequence<(T) -> T>): (T) -> T {
    return { initial ->
        commands.fold(initial) { acc, cmd ->
            cmd(acc)
        }
    }
}