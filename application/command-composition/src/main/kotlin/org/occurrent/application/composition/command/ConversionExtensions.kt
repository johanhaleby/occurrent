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

import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream


fun <T> ((Sequence<T>) -> Sequence<T>).toListCommand(): ((List<T>) -> List<T>) = { events ->
    this(events.asSequence()).toList()
}

fun <T> ((Sequence<T>) -> Sequence<T>).toStreamCommand(): ((Stream<T>) -> Stream<T>) = { events ->
    this(events.asSequence()).asStream()
}

@JvmName("toSequenceCommandFromStream")
inline fun <reified T> ((Stream<T>) -> Stream<T>).toSequenceCommand(): ((Sequence<T>) -> Sequence<T>) = { events ->
    this(events.asStream()).asSequence()
}

@JvmName("toSequenceCommandFromList")
inline fun <reified T> ((List<T>) -> List<T>).toSequenceCommand(): ((Sequence<T>) -> Sequence<T>) = { events ->
    this(events.toList()).asSequence()
}