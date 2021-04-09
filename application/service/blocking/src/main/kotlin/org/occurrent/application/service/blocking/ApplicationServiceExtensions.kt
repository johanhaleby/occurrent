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

package org.occurrent.application.service.blocking

import org.occurrent.eventstore.api.WriteResult
import java.util.*
import java.util.function.Function
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>): WriteResult =
    execute(streamId, functionThatCallsDomainModel, null)

/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(streamId: String, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>): WriteResult =
    execute(streamId, functionThatCallsDomainModel, null)

/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(
    streamId: UUID, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>,
    sideEffects: ((Sequence<T>) -> Unit)? = null
): WriteResult =
    execute(streamId.toString(), functionThatCallsDomainModel, sideEffects)

/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(streamId: String, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>, sideEffects: ((Sequence<T>) -> Unit)? = null): WriteResult =
    execute(streamId, { streamOfEvents ->
        functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
    }, sideEffects?.toStreamSideEffect())


/**
 * Extension function to [ApplicationService] that allows working with [List]
 */
@JvmName("executeList")
fun <T> ApplicationService<T>.execute(streamId: String, functionThatCallsDomainModel: (List<T>) -> List<T>) : WriteResult {
    val f = Function<Stream<T>, Stream<T>> { eventStream: Stream<T> ->
        val list: List<T> = eventStream.asSequence().toList()
        val s: Stream<T> = functionThatCallsDomainModel.invoke(list).stream()
        s
    }
    return execute(streamId, f)
}

private fun <T> ((Sequence<T>) -> Unit).toStreamSideEffect(): (Stream<T>) -> Unit {
    return { streamOfEvents -> this(streamOfEvents.asSequence()) }
}