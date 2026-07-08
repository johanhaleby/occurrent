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

import org.occurrent.application.service.ExecuteFilter
import org.occurrent.eventstore.api.WriteResult
import java.util.UUID

/**
 * Execute a domain function that works with a Kotlin [List] of events.
 *
 * This reads naturally from Kotlin now that the application service works with [List] directly.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId, ExecuteOptions.empty<E>(), functionThatCallsDomainModel)

/**
 * Variant of [executeList] that accepts a [UUID] stream identifier.
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: UUID, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    executeList(streamId.toString(), functionThatCallsDomainModel)

/**
 * Execute a domain function that works with a Kotlin [List] and additional [ExecuteOptions].
 */
@Suppress("UNCHECKED_CAST")
fun <E : Any> ApplicationService<E>.executeList(streamId: String, options: ExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    // Kotlin callers often start from `options()` before the concrete event type is known.
    // `ApplicationService<E>` and the domain-model lambda establish `E`, after which we can
    // safely bridge the star-projected Kotlin options chain to the typed Java API.
    execute(streamId, options as ExecuteOptions<E>) { events -> functionThatCallsDomainModel(events) }

/**
 * Execute a domain function that works with a Kotlin [List] and an [ExecuteFilter].
 */
fun <E : Any> ApplicationService<E>.executeList(streamId: String, executeFilter: ExecuteFilter<out E>, functionThatCallsDomainModel: (List<E>) -> List<E>): WriteResult =
    execute(streamId, executeFilter) { events -> functionThatCallsDomainModel(events) }

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
