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

package org.occurrent.dsl.dcb.reactor

import org.occurrent.eventstore.api.dcb.DcbConsistencyToken
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import reactor.core.publisher.Mono

/**
 * Query that collects the matching domain events into a [Mono] of a [List].
 *
 * The [query] family already returns a [reactor.core.publisher.Flux], which is the idiomatic reactive shape, so this is
 * only a convenience for callers that want the whole result as one list.
 *
 * @see DcbDomainEventQueries.query
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForList(
    query: DcbQuery,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Mono<List<T>> =
    this.query(query, options).collectList()

/**
 * Query that returns the matching domain events as a [List] together with the observed DCB sequence position and the
 * consistency token for a later conditional append.
 *
 * @see DcbDomainEventQueries.queryWithPosition
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForListWithPosition(
    query: DcbQuery,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Mono<Triple<List<T>, Long, DcbConsistencyToken>> =
    this.queryWithPosition(query, options).map { Triple(it.events(), it.lastSequencePosition(), it.consistencyToken()) }
