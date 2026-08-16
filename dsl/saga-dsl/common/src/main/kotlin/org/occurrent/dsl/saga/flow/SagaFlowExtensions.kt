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
import org.occurrent.dsl.saga.TimerName
import org.occurrent.filter.Filter
import java.time.Duration
import java.time.Instant
import java.util.function.BiFunction
import java.util.function.Function
import java.util.function.Predicate
import kotlin.reflect.KClass

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

/**
 * The name of the timer that the step called [stepName] arms. Fire it in a test with
 * `SagaInput.timeout("game-1", stepTimer("awaiting-players"))` and assert on it with
 * `SagaEffect.cancelTimeout(stepTimer("awaiting-players"))`.
 */
fun stepTimer(stepName: String): TimerName = FlowSaga.stepTimer(stepName)

/**
 * Wraps a Kotlin function as a [Predicate] with equals/hashCode over [delegate]'s own identity, since a bare SAM
 * conversion allocates a fresh object on every call and two leaves built from the very same function value would
 * then never be recognized as counting the same events, or as sharing an [allOf] child's requirement (see
 * [StepScope.event]). Identity rather than [delegate]'s own equals, so a bound method reference such as
 * `thing::test` is only interchangeable with a call passing that exact reference again, never with a second
 * `thing::test` over an equal but distinct receiver whose equals happens to ignore a field the method reads. Parity
 * only. Two separately-declared lambdas, even functionally identical ones, still compare unequal here, exactly as
 * two separately-declared Java lambdas do.
 */
private class FunctionPredicate<T>(private val delegate: (T) -> Boolean) : Predicate<T> {
    override fun test(candidate: T): Boolean = delegate(candidate)
    override fun equals(other: Any?): Boolean = other is FunctionPredicate<*> && delegate === other.delegate
    override fun hashCode(): Int = System.identityHashCode(delegate)
}

/** Constructs [FunctionPredicate], kept out of the two inline [StepScope.event] bodies so the class itself can stay private. */
@PublishedApi
internal fun <T> wrapPredicate(predicate: (T) -> Boolean): Predicate<T> = FunctionPredicate(predicate)

/** Receiver for the flow [saga] block. Delegates to the Java [FlowSaga.Builder]. */
@SagaDsl
class FlowSagaBuilder<E : Any, C : Any> @PublishedApi internal constructor() {
    @PublishedApi
    internal val delegate: FlowSaga.Builder<E, C> = FlowSaga.builder()

    /**
     * Sets how many received events are retained behind the current step's entry, and so how far back a guard, a
     * window-condition reaction, or a timeout reaction can read history through [ReceivedEvents]. The initiating event
     * and the current step's own events are always retained on top of this, so this only bounds the earlier history.
     * Raise it for a guard that counts far across a self-looping step, and lower it to trim the persisted state of a
     * long-running instance. Must be at least zero.
     */
    fun historyWindow(events: Int) {
        delegate.historyWindow(events)
    }

    /**
     * Sets how many of the current step's own received events are kept, which limits what the instance stores rather than
     * what a step condition counts. Unbounded by default, so a step keeps every event it receives unless this is set.
     * A condition's counts are carried in the instance's state, so a step still completes on the same event it would have
     * without this set, while a guard, a window-condition reaction and a timeout reaction read only the events still kept.
     * Must be at least 1.
     */
    fun stepWindow(events: Int) {
        delegate.stepWindow(events)
    }

    /**
     * Declares the event type [T] that starts an instance, and optionally the commands to issue on start. Correlate [T] with
     * [correlate] or [correlateAll] like any other event type.
     */
    inline fun <reified T : E> startsOn(noinline onStart: FlowReactions<C>.(T) -> FlowReactions<C> = { nothing }) {
        delegate.startsOn(T::class.java) { event -> FlowReactions<C>().onStart(event).build() }
    }

    /** As [startsOn], but the start reaction also sees the starting event's [EventMetadata]. */
    inline fun <reified T : E> startsOn(noinline onStart: FlowReactions<C>.(EventMetadata, T) -> FlowReactions<C>) {
        delegate.startsOn(T::class.java, BiFunction { metadata, event: T ->
            FlowReactions<C>().onStart(metadata, event).build()
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

    /**
     * Adds a condition every selected event must also match, on top of the selector derived from the event types the
     * flow names. See [Saga.narrowingFilter] for what it leaves you responsible for. Can be set only once.
     */
    fun narrowingFilter(narrowingFilter: Filter) {
        delegate.narrowingFilter(narrowingFilter)
    }

    /**
     * Sets an explicit selector used instead of deriving one from the event types the flow names. See
     * [Saga.replacementFilter] for what it leaves you responsible for. Can be set only once.
     */
    fun replacementFilter(replacementFilter: Filter) {
        delegate.replacementFilter(replacementFilter)
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

    /**
     * A leaf [StepCondition] matching [count] events of type [T], optionally also satisfying [predicate]. Combine leaves
     * with [allOf]/[anyOf] and hand the tree to [on].
     *
     * [predicate] goes through [wrapPredicate] rather than a bare SAM conversion, so [allOf] can tell two leaves built
     * from the same Kotlin function value apart from two that happen to test the same thing independently, the same
     * way it already can for two leaves sharing a Java [Predicate] instance.
     */
    inline fun <reified T : E> event(count: Int = 1, noinline predicate: ((T) -> Boolean)? = null): StepCondition<E> {
        val javaPredicate: Predicate<T>? = predicate?.let { wrapPredicate(it) }
        return StepCondition.event<E, T>(T::class.java, count, javaPredicate)
    }

    /**
     * A leaf [StepCondition] matching [count] events of type [T] that also satisfy [predicate], with [predicateId] naming
     * that predicate so a saga can keep the leaf's count in its state instead of counting the step's events again. Naming a
     * predicate is what makes `stepWindow` usable on the step. Change the name whenever the predicate's meaning changes,
     * since keeping the name while changing the test is the one thing this cannot detect.
     *
     * Wraps [predicate] with [wrapPredicate] rather than a bare SAM conversion, so two leaves built from the same
     * Kotlin function value compare equal the way two leaves sharing a Java [Predicate] instance do. A fresh SAM
     * object has identity equality and would never match another one, even one wrapping the exact same function.
     */
    inline fun <reified T : E> event(count: Int = 1, predicateId: String, noinline predicate: (T) -> Boolean): StepCondition<E> =
        StepCondition.event<E, T>(T::class.java, count, predicateId, wrapPredicate(predicate))

    /** [allOf] over an existing [StepCondition] tree, fulfilled once every one of [first] plus [rest] is. */
    fun allOf(first: StepCondition<out E>, vararg rest: StepCondition<out E>): StepCondition<E> =
        StepCondition.allOf<E>(listOf(first) + rest)

    /** [anyOf] over an existing [StepCondition] tree, fulfilled once any one of [first] plus [rest] is. */
    fun anyOf(first: StepCondition<out E>, vararg rest: StepCondition<out E>): StepCondition<E> =
        StepCondition.anyOf<E>(listOf(first) + rest)

    /** [allOf] shorthand, [first] and [rest] each become a predicate-less, count-one [event] leaf. */
    fun allOf(first: KClass<out E>, vararg rest: KClass<out E>): StepCondition<E> =
        StepCondition.allOf<E>(classLeaves(first, rest))

    /** [anyOf] shorthand, [first] and [rest] each become a predicate-less, count-one [event] leaf. */
    fun anyOf(first: KClass<out E>, vararg rest: KClass<out E>): StepCondition<E> =
        StepCondition.anyOf<E>(classLeaves(first, rest))

    private fun classLeaves(first: KClass<out E>, rest: Array<out KClass<out E>>): List<StepCondition<E>> =
        (listOf(first) + rest).map { type -> classLeaf(type.java) }

    private fun <T : E> classLeaf(type: Class<T>): StepCondition<E> = StepCondition.event<E, T>(type)

    // The @JvmName pair on each arity disambiguates the JVM signature between the two overloads below (both erase to a
    // zero-argument method), the same reason DcbCriteriaBuilder.types<E1,E2>()/types<E1,E2,E3>() names both types2/types3.

    /** [allOf] over [A] and [B], each a predicate-less, count-one leaf. Beyond three types, use the [KClass] or leaf spelling. */
    @JvmName("allOfTwo")
    inline fun <reified A : E, reified B : E> allOf(): StepCondition<E> =
        StepCondition.allOf<E>(event<A>(), event<B>())

    /** As the two-type [allOf], for three leaf types. */
    @JvmName("allOfThree")
    inline fun <reified A : E, reified B : E, reified C : E> allOf(): StepCondition<E> =
        StepCondition.allOf<E>(event<A>(), event<B>(), event<C>())

    /** [anyOf] over [A] and [B], each a predicate-less, count-one leaf. Beyond three types, use the [KClass] or leaf spelling. */
    @JvmName("anyOfTwo")
    inline fun <reified A : E, reified B : E> anyOf(): StepCondition<E> =
        StepCondition.anyOf<E>(event<A>(), event<B>())

    /** As the two-type [anyOf], for three leaf types. */
    @JvmName("anyOfThree")
    inline fun <reified A : E, reified B : E, reified C : E> anyOf(): StepCondition<E> =
        StepCondition.anyOf<E>(event<A>(), event<B>(), event<C>())

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
        noinline commands: FlowReactions<C>.(T) -> FlowReactions<C> = { nothing }
    ) {
        val commandFn = Function<T, List<C>> { event -> FlowReactions<C>().commands(event).build() }
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
        val commandFn = BiFunction<EventMetadata, T, List<C>> { metadata, event -> FlowReactions<C>().commands(metadata, event).build() }
        if (onlyIf == null) {
            delegate.on(T::class.java, then, commandFn)
        } else {
            delegate.on(T::class.java, { e, received -> onlyIf(e, received) }, then, commandFn)
        }
    }

    /**
     * A branch that issues commands and follows [then] once the events received since the step was entered satisfy
     * [condition]. [whenFulfilled] reads [ReceivedEvents], not a single triggering event. Whichever leaf tipped
     * [condition] over, the tipping event is always `received.asList().last()`.
     *
     * A [StepCondition] first argument cannot bind to the reified `on<T>` overloads above, so there is no resolution
     * collision between a classic branch and a window-condition one.
     *
     * [condition] takes `StepCondition<out E>`, matching the Java side's `StepCondition<? extends E>`, so a leaf built
     * over a narrower event type can be passed to a step declared over a broader one.
     */
    fun on(
        condition: StepCondition<out E>,
        then: Continuation,
        whenFulfilled: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { nothing }
    ) {
        delegate.on(condition, then) { received -> FlowReactions<C>().whenFulfilled(received).build() }
    }

    /** A relative timeout: if it fires before the step completes, issue commands and follow [then]. */
    fun timeout(after: Duration, then: Continuation, onExpiry: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { nothing }) {
        delegate.timeout(after, then) { received -> FlowReactions<C>().onExpiry(received).build() }
    }

    /** An absolute, data-derived timeout: [at] is computed from the events received when the step is entered. */
    fun timeout(at: (ReceivedEvents<E>) -> Instant, then: Continuation, onExpiry: FlowReactions<C>.(ReceivedEvents<E>) -> FlowReactions<C> = { nothing }) {
        delegate.timeout({ received -> at(received) }, then, { received -> FlowReactions<C>().onExpiry(received).build() })
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
    fun issue(command: C): FlowReactions<C> = apply { commands += command }

    /** Closes a reaction that does not end on an [issue], such as one finishing on an `if` without an `else`. */
    val nothing: FlowReactions<C> get() = this

    @PublishedApi
    internal fun build(): List<C> = commands.toList()
}
