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
import org.occurrent.eventstore.api.dcb.DcbCriteria

/**
 * Kotlin-friendly counterparts to [DcbApplicationService.execute] that return a nullable [DcbAppendResult] instead of
 * the Java `Optional<DcbAppendResult>`. The result is `null` when the domain function produced no new events (a no-op
 * command), mirroring the empty `Optional` the Java API returns.
 */

/**
 * Execute a domain function for the events selected by [query].
 */
fun <E : Any> DcbApplicationService<E>.executeOrNull(query: DcbCriteria, functionThatCallsDomainModel: (List<E>) -> List<E>): DcbAppendResult? =
    execute(query) { events -> functionThatCallsDomainModel(events) }.orElse(null)

/**
 * Execute a domain function for the events selected by [query], with the supplied [DcbExecuteOptions].
 */
@Suppress("UNCHECKED_CAST")
fun <E : Any> DcbApplicationService<E>.executeOrNull(query: DcbCriteria, options: DcbExecuteOptions<*>, functionThatCallsDomainModel: (List<E>) -> List<E>): DcbAppendResult? =
    execute(query, options as DcbExecuteOptions<E>) { events -> functionThatCallsDomainModel(events) }.orElse(null)
