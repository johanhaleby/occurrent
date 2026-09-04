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

package org.occurrent.dsl.snapshot

import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.view.View
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.filter.Filter
import java.util.function.BiFunction

/**
 * Builds a [SnapshotView] with a type-safe, per-event-type handler block, for example:
 *
 * ```
 * val balance = snapshotView<Balance, LedgerEvent>(initialState = Balance.ZERO) {
 *     schemaVersion(1)
 *     on<MoneyDeposited> { state, e -> state.plus(e.amount) }
 *     on<MoneyWithdrawn> { state, e -> state.minus(e.amount) }
 * }
 * ```
 *
 * The registered event types become the view's [SnapshotView.eventTypes]. Add an explicit [filter] to select on more
 * than event type.
 */
fun <S, E : Any> snapshotView(initialState: S, block: SnapshotViewBuilder<S, E>.() -> Unit): SnapshotView<S, E> {
    val builder = SnapshotViewBuilder<S, E>(initialState)
    builder.block()
    return builder.build()
}

/**
 * Builds a [SnapshotView] whose fold begins from no state. The state stays `null` until a handler replaces it. See
 * [snapshotView] for the handler block.
 */
@Suppress("UNCHECKED_CAST")
fun <S, E : Any> snapshotView(block: SnapshotViewBuilder<S, E>.() -> Unit): SnapshotView<S, E> =
    snapshotView(null as S, block)

/**
 * Receiver for the [snapshotView] block. Delegates to the Java [SnapshotView.Builder] so the Java and Kotlin surfaces
 * produce the same descriptor from one dispatch implementation.
 */
class SnapshotViewBuilder<S, E : Any> @PublishedApi internal constructor(initialState: S) {
    @PublishedApi
    internal val delegate: SnapshotView.Builder<S, E> = SnapshotView.builder(initialState)

    /** Registers the fold for event type [T]. */
    inline fun <reified T : E> on(noinline handler: (S, T) -> S) {
        delegate.on(T::class.java, BiFunction { s, e -> handler(s, e) })
    }

    /**
     * Registers a metadata-aware fold for event type [T]: the fold sees the event's [EventMetadata] (stream id and
     * version, global position, DCB tags, CloudEvent extensions) as well as the event. A rebuild from a query/replay
     * that folds without metadata sees [EventMetadata.empty].
     */
    inline fun <reified T : E> on(noinline handler: (S, EventMetadata, T) -> S) {
        delegate.on(T::class.java, View.Fold { s, metadata, e -> handler(s, metadata, e) })
    }

    /** Sets the schema version tagging the state this fold produces; bump it when the state shape changes. */
    fun schemaVersion(schemaVersion: Int) {
        delegate.schemaVersion(schemaVersion)
    }

    /** Sets an explicit selector overriding the event-type-derived one. */
    fun filter(filter: Filter) {
        delegate.filter(filter)
    }

    @PublishedApi
    internal fun build(): SnapshotView<S, E> = delegate.build()
}

/**
 * Builds a [DcbSnapshotView] with a type-safe handler block plus a DCB read boundary, for example:
 *
 * ```
 * val balance = dcbSnapshotView<Balance, LedgerEvent>(initialState = Balance.ZERO) {
 *     schemaVersion(1)
 *     tags("account:1")
 *     on<MoneyDeposited> { state, e -> state.plus(e.amount) }
 * }
 * ```
 *
 * Set the boundary with [DcbSnapshotViewBuilder.tags] (a tag filter) or an explicit [DcbSnapshotViewBuilder.criteria].
 * With neither, the boundary defaults to [DcbCriteria.all].
 */
fun <S, E : Any> dcbSnapshotView(initialState: S, block: DcbSnapshotViewBuilder<S, E>.() -> Unit): DcbSnapshotView<S, E> {
    val builder = DcbSnapshotViewBuilder<S, E>(initialState)
    builder.block()
    return builder.build()
}

/**
 * Receiver for the [dcbSnapshotView] block: the [SnapshotViewBuilder] surface plus the DCB read boundary.
 */
class DcbSnapshotViewBuilder<S, E : Any> @PublishedApi internal constructor(initialState: S) {
    @PublishedApi
    internal val viewBuilder: SnapshotViewBuilder<S, E> = SnapshotViewBuilder(initialState)

    @PublishedApi
    internal val tags: MutableList<Tag> = mutableListOf()

    @PublishedApi
    internal var explicitCriteria: DcbCriteria? = null

    /** Registers the fold for event type [T]. */
    inline fun <reified T : E> on(noinline handler: (S, T) -> S) = viewBuilder.on<T>(handler)

    /**
     * Registers a metadata-aware fold for event type [T]: the fold sees the event's [EventMetadata] as well as the
     * event.
     */
    inline fun <reified T : E> on(noinline handler: (S, EventMetadata, T) -> S) = viewBuilder.on<T>(handler)

    /** Sets the schema version tagging the state this fold produces; bump it when the state shape changes. */
    fun schemaVersion(schemaVersion: Int) = viewBuilder.schemaVersion(schemaVersion)

    /** Adds DCB tags to the read boundary, matched all-of. Each string is parsed with [Tag.parse]. */
    fun tags(vararg tag: String) {
        tag.forEach { tags.add(Tag.parse(it)) }
    }

    /** Adds DCB tags to the read boundary, matched all-of. */
    fun tags(vararg tag: Tag) {
        tags.addAll(tag)
    }

    /** Sets the DCB read boundary explicitly, overriding any [tags]. Can be set only once. */
    fun criteria(criteria: DcbCriteria) {
        check(explicitCriteria == null) { "criteria(...) has already been set and can only be set once" }
        this.explicitCriteria = criteria
    }

    @PublishedApi
    internal fun build(): DcbSnapshotView<S, E> {
        val snapshotView = viewBuilder.build()
        val criteria = explicitCriteria ?: if (tags.isNotEmpty()) DcbCriteria.tags(tags) else DcbCriteria.all()
        return DcbSnapshotView(snapshotView, criteria)
    }
}
