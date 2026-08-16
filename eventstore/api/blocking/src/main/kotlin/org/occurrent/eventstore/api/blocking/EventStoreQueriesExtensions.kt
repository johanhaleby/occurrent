/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.api.blocking

import io.cloudevents.CloudEvent
import org.occurrent.eventstore.api.SortBy
import org.occurrent.filter.Filter
import kotlin.streams.asSequence

/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 *
 * If you only want the first few elements, pass [limit] here rather than calling `.take(n)` on the result.
 * `limit` is a parameter the query itself carries, where `.take(n)` only trims what a fully iterated [Sequence]
 * already produced.
 *
 * A [Sequence] cannot be closed, and the underlying read may hold a database resource, so consume this to the end.
 * If you stop early, read through [EventStoreQueries.query] instead and close the stream yourself.
 *
 * @see EventStoreQueries.query
 */
fun EventStoreQueries.queryForSequence(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): Sequence<CloudEvent> = query(filter, skip, limit, sortBy).asSequence()

/**
 * Query that returns a [List] instead of a [java.util.stream.Stream].
 * @see EventStoreQueries.query
 */
fun EventStoreQueries.queryForList(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.unsorted()
): List<CloudEvent> = query(filter, skip, limit, sortBy).toList()