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

package org.occurrent.application.service.reactor

import org.occurrent.application.service.ExecuteFilter
import org.occurrent.eventstore.api.WriteResult
import reactor.core.publisher.Mono
import java.util.UUID

/** Execute a domain function over a Kotlin [List], returning a [Mono] of the write result. */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    executeList(streamId, ExecuteOptions.empty<E>(), functionThatCallsDomainModel)

/** [executeList] variant accepting a [UUID] stream id. */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    executeList(streamId.toString(), functionThatCallsDomainModel)

/** Execute a domain function over a Kotlin [List] with [ExecuteOptions]. */
@Suppress("UNCHECKED_CAST")
fun <E : Any> ApplicationService<E>.executeList(streamId: String, options: ExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    execute(streamId, options as ExecuteOptions<E>) { events -> functionThatCallsDomainModel(events) }

/** Execute a domain function over a Kotlin [List] with an [ExecuteFilter]. */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    execute(streamId, executeFilter) { events -> functionThatCallsDomainModel(events) }

/** [executeList] variant accepting a [UUID] stream id and [ExecuteOptions]. */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, options: ExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    executeList(streamId.toString(), options, functionThatCallsDomainModel)

/** [executeList] variant accepting a [UUID] stream id and an [ExecuteFilter]. */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (List<E>) -> List<E>): Mono<WriteResult> =
    executeList(streamId.toString(), executeFilter, functionThatCallsDomainModel)
