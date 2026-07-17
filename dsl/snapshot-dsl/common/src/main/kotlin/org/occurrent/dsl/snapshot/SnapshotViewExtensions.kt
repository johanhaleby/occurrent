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
