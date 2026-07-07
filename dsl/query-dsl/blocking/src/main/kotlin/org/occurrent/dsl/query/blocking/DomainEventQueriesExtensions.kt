/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.query.blocking

import org.occurrent.eventstore.api.PositionRange
import org.occurrent.eventstore.api.SortBy
import org.occurrent.filter.Filter
import kotlin.reflect.KClass
import kotlin.streams.asSequence

// Naming convention: the blocking query DSL names its extensions after the return type they produce
// (queryForSequence, queryForList) because the underlying Java query returns a java.util.stream.Stream, so the verb
// signals the conversion. The reactor query DSL keeps the bare query name since a Flux is already the idiomatic
// reactive result and needs no conversion.

/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForSequence(
    filter: Filter = Filter.all(),
    sortBy: SortBy = SortBy.unsorted()
): Sequence<T> =
    query<T>(filter, sortBy)
        .map { it as T }
        .asSequence()


/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForSequence(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Sequence<T> =
    query<T>(filter, skip, limit, sortBy)
        .map { it as T }
        .asSequence()

/**
 * Query by type of domain event ([T]).
 *
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForSequence(
    type: KClass<T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Sequence<T> =
    query(type.java, skip, limit, sortBy).asSequence()

/**
 * Query by type of domain event ([T]).
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<T>.queryForSequence(
    type: KClass<out T>,
    vararg additionalTypes: KClass<out T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Sequence<T> = (if (additionalTypes.isEmpty()) {
    query(type.java, skip, limit, sortBy)
} else {
    val typeList = mutableListOf(type.java, *additionalTypes.map { it.java }.toTypedArray())
    query(typeList, skip, limit, sortBy)
}).asSequence()

/**
 * Query that returns a [List] instead of a [java.util.stream.Stream].
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForList(
    filter: Filter = Filter.all(),
    sortBy: SortBy = SortBy.unsorted()
): List<T> =
    query<T>(filter, sortBy)
        .map { it as T }
        .toList()

/**
 * Query that returns a [List] instead of a [java.util.stream.Stream].
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForList(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): List<T> =
    query<T>(filter, skip, limit, sortBy)
        .map { it as T }
        .toList()

/**
 * Query by type of domain event ([T]).
 *
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForList(
    type: KClass<T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): List<T> = query(type.java, skip, limit, sortBy).toList()

/**
 * Query by type of domain event ([T]).
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<T>.queryForList(
    type: KClass<out T>,
    vararg additionalTypes: KClass<out T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): List<T> = (if (additionalTypes.isEmpty()) {
    query(type.java, skip, limit, sortBy)
} else {
    val typeList = mutableListOf(type.java, *additionalTypes.map { it.java }.toTypedArray())
    query(typeList, skip, limit, sortBy)
}).toList()

/**
 * Query for a single event (Kotlin equivalent to [DomainEventQueries.queryOne]).
 */
inline fun <reified T : Any> DomainEventQueries<in T>.queryOne(
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): T? = queryOne(T::class.java, skip, limit, sortBy)

/**
 * Query for a single event (Kotlin equivalent to [DomainEventQueries.queryOne]).
 */
fun <T : Any> DomainEventQueries<in T>.queryOne(
    type: KClass<T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): T? = queryOne(type.java, skip, limit, sortBy)

/**
 * Reads domain events strictly after the global sequence [position] as a [Sequence], in ascending position order.
 * @see DomainEventQueries.afterPosition
 */
fun <T : Any> DomainEventQueries<T>.afterPositionAsSequence(position: Long): Sequence<T> =
    afterPosition(position).asSequence()

/**
 * Reads domain events matching [filter] within [range] as a [Sequence], in ascending position order.
 * @see DomainEventQueries.readInPositionOrder
 */
fun <T : Any> DomainEventQueries<T>.readInPositionOrderAsSequence(
    filter: Filter = Filter.all(),
    range: PositionRange
): Sequence<T> = readInPositionOrder(filter, range).asSequence()