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

package org.occurrent.dsl.saga

import org.occurrent.cloudevents.EventMetadata
import java.time.Duration
import java.time.Instant
import java.util.function.BiFunction
import java.util.function.Function
import java.util.function.Predicate

/**
 * Builds a [Saga] (an event-driven process manager) with a type-safe, per-event-type block, for example:
 *
 * ```
 * val orderFulfillment = saga<OrderEvent, OrderState?, OrderCommand>(initialState = null) {
 *     correlateAll { it.orderId }
 *     startsOn<OrderPlaced>()
 *     evolve<OrderPlaced> { _, e -> AwaitingPayment(e.orderId) }
 *     react<OrderPlaced> { _, e ->
 *         issue(ReservePayment(e.orderId, e.amount))
 *         startTimeout("payment", Duration.ofMinutes(30))
 *     }
 *     evolveOnTimeout("payment") { s, _ -> Cancelled((s as AwaitingPayment).orderId) }
 *     reactOnTimeout("payment") { s, _ -> issue(CancelOrder((s as Cancelled).orderId)) }
 *     isTerminal { it is Cancelled }
 * }
 * ```
 *
 * The registered event types become the saga's [Saga.eventTypes] (its default subscription selector). Every handled
 * event type needs a correlation, from [SagaBuilder.correlate] or a [SagaBuilder.correlateAll] fallback, or [build]
 * throws.
 */
fun <E : Any, S, C : Any> saga(initialState: S, block: SagaBuilder<E, S, C>.() -> Unit): Saga<E, S, C> {
    val builder = SagaBuilder<E, S, C>(initialState)
    builder.block()
    return builder.build()
}

/**
 * Receiver for the [saga] block. Delegates to the Java [Saga.Builder] so the Java and Kotlin surfaces produce the same
 * descriptor from one dispatch implementation.
 */
class SagaBuilder<E : Any, S, C : Any> @PublishedApi internal constructor(initialState: S) {
    @PublishedApi
    internal val delegate: Saga.Builder<E, S, C> = Saga.builder(initialState)

    /** Registers how to derive the correlation id from an event of type [T]. Return `null` to skip the event. */
    inline fun <reified T : E> correlate(noinline id: (T) -> String?) {
        delegate.correlate(T::class.java, Function { e -> id(e) })
    }

    /** Registers a fallback correlation function for any event type without its own [correlate]. Can be set only once. */
    fun correlateAll(id: (E) -> String?) {
        delegate.correlateAll(Function { e -> id(e) })
    }

    /** Marks event type [T] as instance-creating. At least one is required. */
    inline fun <reified T : E> startsOn() {
        delegate.startsOn(T::class.java)
    }

    /** Registers the fold for event type [T]. */
    inline fun <reified T : E> evolve(noinline fold: (S, T) -> S) {
        delegate.evolve(T::class.java, BiFunction { s, e -> fold(s, e) })
    }

    /** Registers the metadata-carrying fold for event type [T]: the fold also receives the event's delivery [EventMetadata]. */
    inline fun <reified T : E> evolve(noinline fold: (S, EventMetadata, T) -> S) {
        delegate.evolve(T::class.java, Saga.EventEvolver<S, T> { s, m, e -> fold(s, m, e) })
    }

    /** Registers the reaction for event type [T], given the post-evolve state. */
    inline fun <reified T : E> react(noinline react: SagaEffects<C>.(S, T) -> SagaEffects<C>) {
        delegate.react(T::class.java, BiFunction { s, e -> SagaEffects<C>().apply { react(s, e) }.build() })
    }

    /** Registers the metadata-carrying reaction for event type [T]: the reaction also receives the event's delivery [EventMetadata]. */
    inline fun <reified T : E> react(noinline react: SagaEffects<C>.(S, EventMetadata, T) -> SagaEffects<C>) {
        delegate.react(T::class.java, Saga.EventReactor<S, T, C> { s, m, e -> SagaEffects<C>().apply { react(s, m, e) }.build() })
    }

    /** Registers the fold for the timer named [timerName]. */
    fun evolveOnTimeout(timerName: String, fold: (S, SagaTimeout) -> S) {
        delegate.evolveOnTimeout(timerName, BiFunction { s, t -> fold(s, t) })
    }

    /** Registers the reaction for the timer named [timerName], given the post-evolve state. */
    fun reactOnTimeout(timerName: String, react: SagaEffects<C>.(S, SagaTimeout) -> SagaEffects<C>) {
        delegate.reactOnTimeout(timerName, BiFunction { s, t -> SagaEffects<C>().apply { react(s, t) }.build() })
    }

    /** Effects to run once when a start event creates the instance. Can be set only once. */
    fun onStart(react: SagaEffects<C>.(S, E) -> SagaEffects<C>) {
        delegate.onStart(BiFunction { s, e -> SagaEffects<C>().apply { react(s, e) }.build() })
    }

    /**
     * Effects to run once when a start event creates the instance, with the start event's delivery [EventMetadata]. Can be
     * set only once.
     */
    fun onStart(react: SagaEffects<C>.(S, EventMetadata, E) -> SagaEffects<C>) {
        delegate.onStart(Saga.EventReactor<S, E, C> { s, m, e -> SagaEffects<C>().apply { react(s, m, e) }.build() })
    }

    /** The terminal predicate. Can be set only once. */
    fun isTerminal(predicate: (S) -> Boolean) {
        delegate.isTerminal(Predicate { s -> predicate(s) })
    }

    @PublishedApi
    internal fun build(): Saga<E, S, C> = delegate.build()
}

/**
 * The receiver of a [SagaBuilder] reaction block: collects the [SagaEffect]s a reaction produces, in call order.
 */
class SagaEffects<C : Any> @PublishedApi internal constructor() {
    @PublishedApi
    internal val effects: MutableList<SagaEffect<C>> = mutableListOf()

    /** Issue [command]. */
    fun issue(command: C): SagaEffects<C> {
        effects += SagaEffect.issue(command)
        return this
    }

    /** Start (or restart) the timer named [timerName] to fire once [after] has elapsed. */
    fun startTimeout(timerName: String, after: Duration): SagaEffects<C> {
        effects += SagaEffect.startTimeout(timerName, after)
        return this
    }

    /** Start (or restart) the timer named [timerName] to fire at [at]. */
    fun startTimeoutAt(timerName: String, at: Instant): SagaEffects<C> {
        effects += SagaEffect.startTimeoutAt(timerName, at)
        return this
    }

    /** Cancel the timer named [timerName]. */
    fun cancelTimeout(timerName: String): SagaEffects<C> {
        effects += SagaEffect.cancelTimeout(timerName)
        return this
    }

    /**
     * Ends a reaction whose last statement is not one of the effect calls, for example a body that finishes on a
     * conditional. A trailing `if` without an `else` has type `Unit` and would not compile as the last expression.
     */
    val noMore: SagaEffects<C>
        get() = this

    @PublishedApi
    internal fun build(): List<SagaEffect<C>> = effects.toList()
}
