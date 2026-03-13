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
import java.util.UUID
import java.util.function.Function
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 * Execute a domain function that works with Kotlin [Sequence] instead of Java [Stream].
 *
 * This is the preferred Kotlin API when the domain model naturally consumes and
 * returns lazy event sequences.
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: String, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId, ExecuteOptions.empty(), functionThatCallsDomainModel)

/**
 * Variant of [executeSequence] that accepts a [UUID] stream identifier.
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: UUID, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId.toString(), functionThatCallsDomainModel)

/**
 * Execute a domain function that works with Kotlin [Sequence] and additional [ExecuteOptions].
 *
 * This is the preferred Kotlin entrypoint when command execution needs a
 * [org.occurrent.eventstore.api.StreamReadFilter], post-write side effects, or both.
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: String, options: ExecuteOptions<E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    execute(streamId, options) { streamOfEvents ->
        functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
    }

/**
 * Variant of [executeSequence] that accepts a [UUID] stream identifier and [ExecuteOptions].
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: UUID, options: ExecuteOptions<E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId.toString(), options, functionThatCallsDomainModel)

/**
 * Execute a domain function that works with Kotlin [List] instead of Java [Stream].
 *
 * Use this when the domain model expects all previously stored events to be
 * materialized eagerly before making decisions.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, ExecuteOptions.empty(), functionThatCallsDomainModel)

/**
 * Variant of [executeList] that accepts a [UUID] stream identifier.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId.toString(), functionThatCallsDomainModel)

/**
 * Execute a domain function that works with Kotlin [List] and additional [ExecuteOptions].
 *
 * The current stream is fully materialized into a list before
 * [functionThatCallsDomainModel] is invoked.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, options: ExecuteOptions<E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult {
    val f = Function<Stream<E>, Stream<E>> { eventStream: Stream<E> ->
        val currentEvents: List<E> = eventStream.toList()
        functionThatCallsDomainModel.invoke(currentEvents).stream()
    }
    return execute(streamId, options, f)
}

/**
 * Variant of [executeList] that accepts a [UUID] stream identifier and [ExecuteOptions].
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, options: ExecuteOptions<E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId.toString(), options, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old sequence-based `execute(...)` extension.
 *
 * Use [executeSequence] instead to avoid ambiguity with Java's stream-based
 * `ApplicationService.execute(...)` members.
 */
@Deprecated(
    message = "Use executeSequence(streamId, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeSequence(streamId, functionThatCallsDomainModel)")
)
fun <E : Any> ApplicationService<E>.execute(streamId: String, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old sequence-based `execute(...)` extension that accepts a [UUID].
 */
@Deprecated(
    message = "Use executeSequence(streamId, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeSequence(streamId, functionThatCallsDomainModel)")
)
fun <E : Any> ApplicationService<E>.execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old sequence-based `execute(...)` extension with [ExecuteOptions].
 */
@Deprecated(
    message = "Use executeSequence(streamId, options, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeSequence(streamId, options, functionThatCallsDomainModel)")
)
fun <E : Any> ApplicationService<E>.execute(streamId: String, options: ExecuteOptions<E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId, options, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old sequence-based `execute(...)` extension with [UUID] and [ExecuteOptions].
 */
@Deprecated(
    message = "Use executeSequence(streamId, options, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeSequence(streamId, options, functionThatCallsDomainModel)")
)
fun <E : Any> ApplicationService<E>.execute(streamId: UUID, options: ExecuteOptions<E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId, options, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old list-based `execute(...)` extension.
 */
@Deprecated(
    message = "Use executeList(streamId, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeList(streamId, functionThatCallsDomainModel)")
)
@JvmName("deprecatedExecuteList")
fun <E : Any> ApplicationService<E>.execute(streamId: String, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old list-based `execute(...)` extension that accepts a [UUID].
 */
@Deprecated(
    message = "Use executeList(streamId, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeList(streamId, functionThatCallsDomainModel)")
)
@JvmName("deprecatedExecuteList")
fun <E : Any> ApplicationService<E>.execute(streamId: UUID, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old list-based `execute(...)` extension with [ExecuteOptions].
 */
@Deprecated(
    message = "Use executeList(streamId, options, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeList(streamId, options, functionThatCallsDomainModel)")
)
@JvmName("deprecatedExecuteListWithOptions")
fun <E : Any> ApplicationService<E>.execute(streamId: String, options: ExecuteOptions<E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, options, functionThatCallsDomainModel)

/**
 * Deprecated Kotlin alias for the old list-based `execute(...)` extension with [UUID] and [ExecuteOptions].
 */
@Deprecated(
    message = "Use executeList(streamId, options, functionThatCallsDomainModel) to avoid ambiguity with Java Stream-based execute.",
    replaceWith = ReplaceWith("this.executeList(streamId, options, functionThatCallsDomainModel)")
)
@JvmName("deprecatedExecuteListWithOptions")
fun <E : Any> ApplicationService<E>.execute(streamId: UUID, options: ExecuteOptions<E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, options, functionThatCallsDomainModel)
