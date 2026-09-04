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

package org.occurrent.dsl.projection

import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.view.View
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.filter.Filter
import java.util.function.BiFunction
import java.util.function.Function

/**
 * Builds a capability-agnostic [Projection] with a type-safe, per-event-type handler block, for example:
 *
 * ```
 * val nameProjection = projection<NameState?, DomainEvent, String>(initialState = null) {
 *     id { event -> event.userId() }
 *     on<NameDefined> { _, e -> NameState(e.userId(), e.name()) }
 *     on<NameWasChanged> { state, e -> state?.copy(name = e.name()) }
 * }
 * ```
 *
 * The registered event types become the projection's [Projection.eventTypes] (its default subscription selector). Add
 * an explicit [filter] to select on more than event type.
 */
fun <S, E : Any, ID : Any> projection(initialState: S, block: ProjectionBuilder<S, E, ID>.() -> Unit): Projection<S, E, ID> {
    val builder = ProjectionBuilder<S, E, ID>(initialState)
    builder.block()
    return builder.build()
}

/**
 * Builds a [Projection] whose fold begins from no state. The block and the returned `Projection` see `S?` rather
 * than `S`, since the fold starts from `null` until a handler replaces it. See [projection] for the handler block.
 */
fun <S, E : Any, ID : Any> projection(block: ProjectionBuilder<S?, E, ID>.() -> Unit): Projection<S?, E, ID> =
    projection(null, block)

/**
 * Builds a keyed [DcbProjection] with the same handler block as [projection], plus a DCB read boundary. Supply the
 * boundary with [DcbProjectionBuilder.tags] (a tag filter such as `tags("kind:account")`) or an explicit
 * [DcbProjectionBuilder.criteria]. With neither, the boundary defaults to [DcbCriteria.all]. For a single view over all
 * matching events use [dcbSingletonProjection] instead. For example, one instance per account keyed by account id:
 *
 * ```
 * fun accountStatus() =
 *     dcbProjection<Status, AccountEvent, String>(initialState = Status.NEW) {
 *         tags("kind:account")
 *         id { it.accountId }
 *         on<AccountRegistered> { _, _ -> Status.ACTIVE }
 *         on<AccountClosed> { _, _ -> Status.CLOSED }
 *     }
 * ```
 */
fun <S, E : Any, ID : Any> dcbProjection(initialState: S, block: DcbProjectionBuilder<S, E, ID>.() -> Unit): DcbProjection<S, E, ID> {
    val builder = DcbProjectionBuilder<S, E, ID>(initialState)
    builder.block()
    return builder.build()
}

/**
 * Builds a single-instance capability-agnostic [Projection]: it holds one view state rather than one per key, so no
 * `id { }` is needed. The runtime keys the single slot by the projection's own identity (the subscription id when run
 * through a runner, or the `@Projection` id). Use [projection] with an `id { }` block for a keyed, multi-instance read
 * model. For example:
 *
 * ```
 * val claimed = singletonProjection<Boolean, AccountEvent>(initialState = false) {
 *     on<AccountRegistered> { _, _ -> true }
 *     on<AccountClosed> { _, _ -> false }
 * }
 * ```
 */
fun <S, E : Any> singletonProjection(initialState: S, block: ProjectionBuilder<S, E, String>.() -> Unit): Projection<S, E, String> {
    val builder = ProjectionBuilder<S, E, String>(initialState)
    builder.singleton()
    builder.block()
    return builder.build()
}

/**
 * Builds a single-instance [Projection] whose fold begins from no state. The block and the returned `Projection`
 * see `S?` rather than `S`, since the fold starts from `null` until a handler replaces it. See [singletonProjection]
 * for what single-instance means.
 */
fun <S, E : Any> singletonProjection(block: ProjectionBuilder<S?, E, String>.() -> Unit): Projection<S?, E, String> =
    singletonProjection(null, block)

/**
 * Builds a single-instance [DcbProjection] (see [singletonProjection]) with a DCB read boundary. Supply the boundary
 * with [DcbProjectionBuilder.tags] or an explicit [DcbProjectionBuilder.criteria]. With neither it defaults to
 * [DcbCriteria.all].
 */
fun <S, E : Any> dcbSingletonProjection(initialState: S, block: DcbProjectionBuilder<S, E, String>.() -> Unit): DcbProjection<S, E, String> {
    val builder = DcbProjectionBuilder<S, E, String>(initialState)
    builder.singleton()
    builder.block()
    return builder.build()
}

/**
 * Receiver for the [projection] block. Delegates to the Java [Projection.Builder] so the Java and Kotlin surfaces
 * produce the same descriptor from one dispatch implementation.
 */
class ProjectionBuilder<S, E : Any, ID : Any> @PublishedApi internal constructor(initialState: S) {
    @PublishedApi
    internal val delegate: Projection.Builder<S, E, ID> = Projection.builder(initialState)

    /** Sets the function deriving the view-instance id from an event; return `null` to skip the event. Required unless [singleton]. */
    fun id(fn: (E) -> ID?) {
        delegate.id(Function { e -> fn(e) })
    }

    /**
     * Sets the function deriving the view-instance id from the event's [EventMetadata] and the event, so a projection
     * can be keyed by metadata such as the stream id (`id { metadata, _ -> metadata.streamId }`). Return `null` to skip
     * the event. Required unless [singleton]. The metadata-less on-demand query/replay path folds with
     * [EventMetadata.empty], so a metadata-keyed projection cannot be read that way.
     */
    fun id(fn: (EventMetadata, E) -> ID?) {
        delegate.id(BiFunction { metadata, e -> fn(metadata, e) })
    }

    /** Internal: use the top-level [singletonProjection] builder, which fixes the id type to `String`. */
    internal fun singleton() {
        delegate.singleton()
    }

    /** Registers the fold for event type [T]. */
    inline fun <reified T : E> on(noinline handler: (S, T) -> S) {
        delegate.on(T::class.java, BiFunction { s, e -> handler(s, e) })
    }

    /**
     * Registers a metadata-aware fold for event type [T]: the fold sees the event's [EventMetadata] (stream id and
     * version, global position, DCB tags, CloudEvent extensions) as well as the event. On the metadata-less query/replay
     * path the fold sees [EventMetadata.empty].
     */
    inline fun <reified T : E> on(noinline handler: (S, EventMetadata, T) -> S) {
        delegate.on(T::class.java, View.Fold { s, metadata, e -> handler(s, metadata, e) })
    }

    /** Sets an explicit selector overriding the event-type-derived one. */
    fun filter(filter: Filter) {
        delegate.filter(filter)
    }

    @PublishedApi
    internal fun build(): Projection<S, E, ID> = delegate.build()
}

/**
 * Receiver for the [dcbProjection] block: the [ProjectionBuilder] surface plus the DCB read boundary.
 */
class DcbProjectionBuilder<S, E : Any, ID : Any> @PublishedApi internal constructor(initialState: S) {
    @PublishedApi
    internal val projectionBuilder: ProjectionBuilder<S, E, ID> = ProjectionBuilder(initialState)

    @PublishedApi
    internal val tags: MutableList<Tag> = mutableListOf()

    @PublishedApi
    internal var explicitCriteria: DcbCriteria? = null

    /** Sets the function deriving the view-instance id from an event; return `null` to skip the event. Required unless [singleton]. */
    fun id(fn: (E) -> ID?) = projectionBuilder.id(fn)

    /**
     * Sets the metadata-aware function deriving the view-instance id from the event's [EventMetadata] and the event, so
     * a projection can be keyed by metadata such as the stream id. Return `null` to skip the event. Required unless
     * [singleton].
     */
    fun id(fn: (EventMetadata, E) -> ID?) = projectionBuilder.id(fn)

    /** Internal: use the top-level [dcbSingletonProjection] builder, which fixes the id type to `String`. */
    internal fun singleton() = projectionBuilder.singleton()

    /** Registers the fold for event type [T]. */
    inline fun <reified T : E> on(noinline handler: (S, T) -> S) = projectionBuilder.on<T>(handler)

    /**
     * Registers a metadata-aware fold for event type [T]: the fold sees the event's [EventMetadata] as well as the
     * event.
     */
    inline fun <reified T : E> on(noinline handler: (S, EventMetadata, T) -> S) = projectionBuilder.on<T>(handler)

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
    internal fun build(): DcbProjection<S, E, ID> {
        val projection = projectionBuilder.build()
        val criteria = explicitCriteria ?: if (tags.isNotEmpty()) DcbCriteria.tags(tags) else DcbCriteria.all()
        return DcbProjection(projection, criteria)
    }
}
