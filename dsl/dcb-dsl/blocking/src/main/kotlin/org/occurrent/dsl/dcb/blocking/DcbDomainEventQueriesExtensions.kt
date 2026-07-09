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

import org.occurrent.eventstore.api.dcb.DcbConsistencyToken
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import org.occurrent.eventstore.api.dcb.Tag
import kotlin.streams.asSequence

/**
 * Query that returns a [Sequence] instead of a [java.util.stream.Stream].
 *
 * @see DcbDomainEventQueries.query
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForSequence(
    criteria: DcbCriteria,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Sequence<T> =
    this.query(criteria, options).asSequence()

/**
 * Query that returns a [List] instead of a [java.util.stream.Stream].
 *
 * @see DcbDomainEventQueries.query
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForList(
    criteria: DcbCriteria,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): List<T> =
    this.query(criteria, options).toList()

/**
 * Query that returns the matching domain events as a [List] together with the observed DCB sequence position and the
 * consistency token for a later conditional append.
 *
 * @see DcbDomainEventQueries.queryWithPosition
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForListWithPosition(
    criteria: DcbCriteria,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Triple<List<T>, Long, DcbConsistencyToken> =
    this.queryWithPosition(criteria, options).let { Triple(it.events(), it.lastSequencePosition(), it.consistencyToken()) }

/**
 * Query that returns the matching domain events as a [Sequence] together with the observed DCB sequence position and the
 * consistency token for a later conditional append.
 *
 * @see DcbDomainEventQueries.queryWithPosition
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForSequenceWithPosition(
    criteria: DcbCriteria,
    options: DcbReadOptions = DcbReadOptions.fromBeginning()
): Triple<Sequence<T>, Long, DcbConsistencyToken> =
    this.queryWithPosition(criteria, options).let { Triple(it.events().asSequence(), it.lastSequencePosition(), it.consistencyToken()) }

/**
 * Queries DCB events of the reified type [T] as a [List].
 */
inline fun <reified T : Any> DcbDomainEventQueries<in T>.queryForList(): List<T> =
    types(T::class.java).toList()

/**
 * Queries DCB events of the reified type [T] as a [Sequence].
 */
inline fun <reified T : Any> DcbDomainEventQueries<in T>.queryForSequence(): Sequence<T> =
    types(T::class.java).asSequence()

/**
 * Queries DCB events tagged with all the given tags, as a [List].
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForList(firstTag: Tag, vararg moreTags: Tag): List<T> =
    tags(firstTag, *moreTags).toList()

/**
 * Queries DCB events tagged with all the given tags (each parsed from `"key:value"`), as a [List].
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForList(firstTag: String, vararg moreTags: String): List<T> =
    tags(firstTag, *moreTags).toList()

/**
 * Queries DCB events tagged with any of the given tags, as a [List].
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForListAnyOf(firstTag: Tag, vararg moreTags: Tag): List<T> =
    tagsAnyOf(firstTag, *moreTags).toList()

/**
 * Queries DCB events tagged with any of the given tags (each parsed from `"key:value"`), as a [List].
 */
fun <T : Any> DcbDomainEventQueries<T>.queryForListAnyOf(firstTag: String, vararg moreTags: String): List<T> =
    tagsAnyOf(firstTag, *moreTags).toList()
