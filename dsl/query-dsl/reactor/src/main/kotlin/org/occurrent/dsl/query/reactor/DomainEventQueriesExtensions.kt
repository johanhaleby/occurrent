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

package org.occurrent.dsl.query.reactor

import org.occurrent.eventstore.api.SortBy
import org.occurrent.filter.Filter
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.reflect.KClass

/**
 * Query by type of domain event ([T]).
 *
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.query(
    type: KClass<T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Flux<T> = query(type.java, skip, limit, sortBy)

/**
 * Query by type of domain event ([T]).
 * @see DomainEventQueries.query
 */
@Suppress("UNCHECKED_CAST")
fun <T : Any> DomainEventQueries<T>.query(
    type: KClass<out T>,
    vararg additionalTypes: KClass<out T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Flux<T> = (if (additionalTypes.isEmpty()) {
    query(type.java, skip, limit, sortBy)
} else {
    val typeList = mutableListOf(type.java, *additionalTypes.map { it.java }.toTypedArray())
    query(typeList, skip, limit, sortBy)
}) as Flux<T>

/**
 * Query for a single event (Kotlin equivalent to [DomainEventQueries.queryOne]).
 */
inline fun <reified T : Any> DomainEventQueries<in T>.queryOne(
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Mono<T> = queryOne(T::class.java, skip, limit, sortBy)

/**
 * Query for a single event (Kotlin equivalent to [DomainEventQueries.queryOne]).
 */
fun <T : Any> DomainEventQueries<in T>.queryOne(
    type: KClass<T>,
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Mono<T> = queryOne(type.java, skip, limit, sortBy)

/**
 * Query that collects the matching domain events into a [Mono] of a [List].
 *
 * The [query] family already returns a [Flux], which is the idiomatic reactive shape, so this is only a
 * convenience for callers that want the whole result as one list.
 *
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForList(
    filter: Filter = Filter.all(),
    sortBy: SortBy = SortBy.unsorted()
): Mono<List<T>> =
    query<T>(filter, sortBy)
        .map { it as T }
        .collectList()

/**
 * Query that collects the matching domain events into a [Mono] of a [List].
 * @see DomainEventQueries.query
 */
fun <T : Any> DomainEventQueries<in T>.queryForList(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Mono<List<T>> =
    query<T>(filter, skip, limit, sortBy)
        .map { it as T }
        .collectList()
