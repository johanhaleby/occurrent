/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.application.service.blocking.dcb

import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbQuery
import java.util.function.Function
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 * Kotlin-friendly counterparts to [DcbApplicationService.execute] that take a [Sequence] or [List] domain function and
 * return a nullable [DcbAppendResult] instead of the Java `Optional<DcbAppendResult>`.
 *
 * The result is `null` when the domain function produced no new events (a no-op command), mirroring the empty
 * `Optional` the Java API returns. The naming follows the stream `ApplicationService` extensions
 * (`executeSequence` / `executeList`) so there is no overload clash with the Java `execute` taking a `Function`.
 */

/**
 * Execute a domain function for the events selected by [query], working with lazy [Sequence]s.
 */
fun <E : Any> DcbApplicationService<E>.executeSequence(query: DcbQuery, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): DcbAppendResult? =
    execute(query) { streamOfEvents: Stream<E> -> functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream() }.orElse(null)

/**
 * Execute a domain function for the events selected by [query], with the supplied [DcbExecuteOptions], working with lazy [Sequence]s.
 */
@Suppress("UNCHECKED_CAST")
fun <E : Any> DcbApplicationService<E>.executeSequence(query: DcbQuery, options: DcbExecuteOptions<*>, functionThatCallsDomainModel: (Sequence<E>) -> Sequence<E>): DcbAppendResult? =
    execute(query, options as DcbExecuteOptions<E>) { streamOfEvents: Stream<E> -> functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream() }.orElse(null)

/**
 * Execute a domain function for the events selected by [query], working with eager [List]s.
 */
fun <E : Any> DcbApplicationService<E>.executeList(query: DcbQuery, functionThatCallsDomainModel: (List<E>) -> List<E>): DcbAppendResult? =
    execute(query) { eventStream: Stream<E> -> functionThatCallsDomainModel(eventStream.toList()).stream() }.orElse(null)

/**
 * Execute a domain function for the events selected by [query], with the supplied [DcbExecuteOptions], working with eager [List]s.
 */
@Suppress("UNCHECKED_CAST")
fun <E : Any> DcbApplicationService<E>.executeList(query: DcbQuery, options: DcbExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): DcbAppendResult? {
    val f = Function<Stream<E>, Stream<E>> { eventStream: Stream<E> -> functionThatCallsDomainModel(eventStream.toList()).stream() }
    return execute(query, options as DcbExecuteOptions<E>, f).orElse(null)
}
