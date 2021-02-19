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
import org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING
import org.occurrent.filter.Filter
import kotlin.streams.asSequence

/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 * @see EventStoreQueries.query
 */
fun EventStoreQueries.queryForSequence(
    filter: Filter = Filter.all(),
    skip: Int = 0,
    limit: Int = Int.MAX_VALUE,
    sortBy: SortBy = SortBy.natural(ASCENDING)
): Sequence<CloudEvent> = query(filter, skip, limit, sortBy).asSequence()