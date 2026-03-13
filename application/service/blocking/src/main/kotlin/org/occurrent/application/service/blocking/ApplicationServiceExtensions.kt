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
    executeSequence(streamId, ExecuteOptions.empty<E>(), functionThatCallsDomainModel)

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
@Suppress("UNCHECKED_CAST")
fun <E : Any> ApplicationService<E>.executeSequence(streamId: String, options: ExecuteOptions<*>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    // Kotlin callers often start from `options()` before the concrete event type is known.
    // `ApplicationService<E>` and the domain-model lambda establish `E`, after which we can
    // safely bridge the star-projected Kotlin options chain to the typed Java API.
    execute(streamId, options as ExecuteOptions<E>) { streamOfEvents ->
        functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
    }

/**
 * Execute a domain function that works with Kotlin [Sequence] and an [ExecuteFilter].
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: String, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    execute(streamId, executeFilter) { streamOfEvents ->
        functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
    }

/**
 * Variant of [executeSequence] that accepts a [UUID] stream identifier and [ExecuteOptions].
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: UUID, options: ExecuteOptions<*>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId.toString(), options, functionThatCallsDomainModel)

/**
 * Variant of [executeSequence] that accepts a [UUID] stream identifier and an [ExecuteFilter].
 */
fun <E : Any> ApplicationService<E>.executeSequence(streamId: UUID, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): WriteResult =
    executeSequence(streamId.toString(), executeFilter, functionThatCallsDomainModel)

/**
 * Execute a domain function that works with Kotlin [List] instead of Java [Stream].
 *
 * Use this when the domain model expects all previously stored events to be
 * materialized eagerly before making decisions.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, ExecuteOptions.empty<E>(), functionThatCallsDomainModel)

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
@Suppress("UNCHECKED_CAST")
fun <E : Any> ApplicationService<E>.executeList(streamId: String, options: ExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult {
    // As in executeSequence, Kotlin may still be carrying an untyped `options()` chain here.
    // The surrounding `ApplicationService<E>` and list-based domain lambda establish `E`
    // before we delegate to the typed Java API.
    val f = Function<Stream<E>, Stream<E>> { eventStream: Stream<E> ->
        val currentEvents: List<E> = eventStream.toList()
        functionThatCallsDomainModel.invoke(currentEvents).stream()
    }
    return execute(streamId, options as ExecuteOptions<E>, f)
}

/**
 * Execute a domain function that works with Kotlin [List] and an [ExecuteFilter].
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult {
    val f = Function<Stream<E>, Stream<E>> { eventStream: Stream<E> ->
        val currentEvents: List<E> = eventStream.toList()
        functionThatCallsDomainModel.invoke(currentEvents).stream()
    }
    return execute(streamId, executeFilter, f)
}

/**
 * Variant of [executeList] that accepts a [UUID] stream identifier and [ExecuteOptions].
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, options: ExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId.toString(), options, functionThatCallsDomainModel)

/**
 * Variant of [executeList] that accepts a [UUID] stream identifier and an [ExecuteFilter].
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId.toString(), executeFilter, functionThatCallsDomainModel)
