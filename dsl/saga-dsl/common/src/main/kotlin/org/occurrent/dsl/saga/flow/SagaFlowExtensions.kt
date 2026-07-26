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

package org.occurrent.dsl.saga.flow

import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.saga.Saga
import java.time.Duration
import java.time.Instant
import java.util.function.BiFunction
import java.util.function.Function

/**
 * Marks the flow-saga builder receivers so that, inside a nested `step { }` block, a member of an outer scope (such as
 * `correlate`) does not resolve implicitly. This is a deliberate deviation from the non-nesting projection/core DSLs,
 * which do not use a `@DslMarker`.
 */
@DslMarker
annotation class SagaDsl

/**
 * Builds a flow [Saga] with a linear, declarative step block, for example:
 *
 * ```
 * val orderFulfillment = saga<OrderEvent, OrderCommand> {
 *     correlateAll { it.orderId }
 *     startsOn<OrderPlaced> { order -> issue(ReservePayment(order.orderId, order.amount)) }
 *     step("awaiting-payment") {
 *         on<PaymentReserved>(then = end) { payment -> issue(ShipOrder(payment.orderId)) }
 *         on<PaymentFailed>(then = end) { failure -> issue(CancelOrder(failure.orderId)) }
 *         timeout(after = Duration.ofMinutes(30), then = end) { received ->
 *             issue(CancelOrder(received.initiating<OrderPlaced>().orderId))
 *         }
 *     }
 * }
 * ```
 *
 * It compiles to a `Saga<E, FlowState<E>, C>`, so the executor only ever runs one descriptor type.
 */
fun <E : Any, C : Any> saga(block: FlowSagaBuilder<E, C>.() -> Unit): Saga<E, FlowState<E>, C> {
    val builder = FlowSagaBuilder<E, C>()
    builder.block()
    return builder.build()
}

/** Receiver for the flow [saga] block. Delegates to the Java [FlowSaga.Builder]. */
@SagaDsl
class FlowSagaBuilder<E : Any, C : Any> @PublishedApi internal constructor() {
    @PublishedApi
    internal val delegate: FlowSaga.Builder<E, C> = FlowSaga.builder()

    /**
     * Sets how many received events are retained behind the current step's entry, and so how far back a guard, a join
     * reaction, or a timeout reaction can read history through [ReceivedEvents]. The initiating event and the current
     * step's own events are always retained on top of this, so this only bounds the earlier history. Raise it for a
     * guard that counts far across a self-looping step, and lower it to trim the persisted state of a long-running
     * instance. Must be at least zero.
     */
    fun historyWindow(events: Int) {
        delegate.historyWindow(events)
    }

    /**
     * Declares the event type [T] that starts an instance, plus the commands to issue on start. Correlate [T] with
     * [correlate] or [correlateAll] like any other event type.
     */
    inline fun <reified T : E> startsOn(noinline onStart: FlowReactions<C>.(T) -> FlowReactions<C> = { noMore }) {
        delegate.startsOn(T::class.java) { event -> FlowReactions<C>().apply { onStart(event) }.build() }
    }

    /** As [startsOn], but the start reaction also sees the starting event's [EventMetadata]. */
    inline fun <reified T : E> startsOn(noinline onStart: FlowReactions<C>.(EventMetadata, T) -> FlowReactions<C>) {
        delegate.startsOn(T::class.java, BiFunction { metadata, event: T ->
            FlowReactions<C>().apply { onStart(metadata, event) }.build()
        })
    }

    /** Registers how to correlate an event of type [T] to a saga instance. */
    inline fun <reified T : E> correlate(noinline correlatedBy: (T) -> String) {
        delegate.correlate(T::class.java) { correlatedBy(it) }
    }

    /**
     * Registers a fallback correlation for any event type without its own [correlate]. The common case is a sealed
     * event hierarchy exposing a shared id, for example `correlateAll { it.orderId }`.
     */
    fun correlateAll(correlatedBy: (E) -> String) {
        delegate.correlateAll { correlatedBy(it) }
    }

    /** Adds a step named [name]. */
    fun step(name: String, block: StepScope<E, C>.() -> Unit) {
        delegate.step(name) { stepBuilder -> StepScope(stepBuilder).block() }
    }

    @PublishedApi
    internal fun build(): Saga<E, FlowState<E>, C> = delegate.build()
}

/** Receiver for a flow [FlowSagaBuilder.step] block. */
@SagaDsl
class StepScope<E : Any, C : Any> @PublishedApi internal constructor(@PublishedApi internal val delegate: StepBuilder<E, C>) {

    /** Continuation: advance to the next declared step (or complete if there is none). */
    val next: Continuation get() = Continuation.next()

    /** Continuation: complete the saga. */
    val end: Continuation get() = Continuation.end()

    /** Continuation: jump to the named step (a back-edge models a loop or retry). */
    fun transitionTo(step: String): Continuation = Continuation.transitionTo(step)

    /** An expectation of [count] events of type [T], for a [join]. */
    inline fun <reified T : E> expect(count: Int = 1): Expectation<E> = Expectation(T::class.java, count)

    /**
     * A branch: on an event of type [T] (optionally only when [onlyIf] is true), issue commands and follow [then].
     * Omit the reaction entirely for a branch that issues nothing and only advances the flow.
     *
     * The reaction defaults here rather than living on a separate no-reaction overload, because a two-parameter trailing
     * lambda would then be ambiguous between that overload's [onlyIf] and the metadata-carrying reaction below.
     */
    inline fun <reified T : E> on(
        then: Continuation,
        noinline onlyIf: ((T, ReceivedEvents<E>) -> Boolean)? = null,
        noinline commands: FlowReactions<C>.(T) -> FlowReactions<C> = { noMore }
    ) {
        val commandFn = Function<T, List<C>> { event -> FlowReactions<C>().apply { commands(event) }.build() }
        if (onlyIf == null) {
            delegate.on(T::class.java, then, commandFn)
        } else {
            delegate.on(T::class.java, { e, received -> onlyIf(e, received) }, then, commandFn)
        }
    }

    /**
     * A branch whose commands also receive the triggering event's delivery [EventMetadata] (stream id and version, global
     * position, CloudEvent extensions). The metadata-first sibling of [on].
     */
    inline fun <reified T : E> on(
        then: Continuation,
        noinline onlyIf: ((T, ReceivedEvents<E>) -> Boolean)? = null,
        noinline commands: FlowReactions<C>.(EventMetadata, T) -> FlowReactions<C>
    ) {
        val commandFn = BiFunction<EventMetadata, T, List<C>> { metadata, event -> FlowReactions<C>().apply { commands(metadata, event) }.build() }
        if (onlyIf == null) {
            delegate.on(T::class.java, then, commandFn)
        } else {
            delegate.on(T::class.java, { e, received -> onlyIf(e, received) }, then, commandFn)
        }
    }

    /** A join: wait until all [expecting] are met (counted since the step was entered), then issue commands and follow [then]. */
    fun join(expecting: Expectation<E>, vararg more: Expectation<E>, then: Continuation, whenFulfilled: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { noMore }) {
        delegate.join(listOf(expecting, *more), then) { received -> FlowReactions<C>().apply { whenFulfilled(received) }.build() }
    }

    /** A relative timeout: if it fires before the step completes, issue commands and follow [then]. */
    fun timeout(after: Duration, then: Continuation, onExpiry: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { noMore }) {
        delegate.timeout(after, then) { received -> FlowReactions<C>().apply { onExpiry(received) }.build() }
    }

    /** An absolute, data-derived timeout: [at] is computed from the events received when the step is entered. */
    fun timeout(at: (ReceivedEvents<E>) -> Instant, then: Continuation, onExpiry: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { noMore }) {
        delegate.timeout({ received -> at(received) }, then, { received -> FlowReactions<C>().apply { onExpiry(received) }.build() })
    }
}

/**
 * Collects the commands a flow reaction issues, in call order. Flow steps manage timeouts declaratively, so this only
 * issues commands.
 *
 * [issue] returns the receiver, which is what a reaction lambda must return. That is what makes a produced but
 * discarded command a compile error rather than a saga that silently does nothing.
 */
class FlowReactions<C : Any> @PublishedApi internal constructor() {
    @PublishedApi
    internal val commands: MutableList<C> = mutableListOf()

    /** Issue [command]. */
    fun issue(command: C): FlowReactions<C> {
        commands += command
        return this
    }

    /**
     * Ends a reaction whose last statement is not an [issue], typically one finishing on an `if` without an `else`,
     * which has type `Unit` and so cannot end the lambda on its own. A branch that issues nothing at all takes no
     * reaction lambda instead.
     */
    val noMore: FlowReactions<C> get() = this

    @PublishedApi
    internal fun build(): List<C> = commands.toList()
}
