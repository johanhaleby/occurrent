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

package org.occurrent.dsl.dcb.blocking

import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import kotlin.streams.asSequence

/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 *
 * @see DcbDomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<T>.queryForSequence(
    query: DcbQuery,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Sequence<T> =
    DcbDomainEventQueries.query(this, query, options).asSequence()

/**
 * Query that returns a [List] instead of a [java.util.stream.Stream].
 *
 * @see DcbDomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<T>.queryForList(
    query: DcbQuery,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): List<T> =
    DcbDomainEventQueries.query(this, query, options).toList()

/**
 * Query and keep the DCB sequence position returned by the read.
 *
 * @see DcbDomainEventQueries.queryWithPosition
 */
fun <T : Any> DomainEventQueries<T>.queryWithPosition(
    query: DcbQuery,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): DcbDomainEventStream<T> =
    DcbDomainEventQueries.queryWithPosition(this, query, options)
