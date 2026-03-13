package org.occurrent.application.service.blocking

import org.occurrent.eventstore.api.StreamReadFilter
import java.util.function.Consumer
import java.util.stream.Stream
import kotlin.streams.asSequence

/**
 * Create empty [ExecuteOptions] for Kotlin call sites.
 *
 * This helper exists so Kotlin code can start an options chain with `options()`
 * and let the event type be inferred later from chained `sideEffect(...)` or
 * the surrounding `executeSequence(...)` or `executeList(...)` call.
 * Standalone assignments may still require explicit type context.
 */
fun options(): ExecuteOptions<Any> = ExecuteOptions.options()

/**
 * Create [ExecuteOptions] containing the supplied [StreamReadFilter].
 *
 * This helper exists so Kotlin code can start directly with `filter(...)`
 * instead of `options().filter(...)` when that reads better.
 *
 * Type inference is expected to come from the surrounding expression, typically
 * an `executeSequence(...)` or `executeList(...)` call.
 */
fun filter(filter: StreamReadFilter): ExecuteOptions<Any> = options().filter(filter)

/**
 * Create [ExecuteOptions] with a typed [sideEffect] that is invoked for events
 * matching [E].
 *
 * Type inference typically comes from the surrounding `executeSequence(...)`
 * or `executeList(...)` call.
 */
inline fun <T : Any, reified E : T> sideEffect(noinline sideEffect: (E) -> Unit): ExecuteOptions<T> =
    typedOptions<T>().addTypedSideEffect(E::class.java, sideEffect)

/**
 * Create [ExecuteOptions] with two typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2)

/**
 * Create [ExecuteOptions] with three typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3)

/**
 * Create [ExecuteOptions] with four typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4)

/**
 * Create [ExecuteOptions] with five typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5)

/**
 * Create [ExecuteOptions] with six typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5, sideEffect6)

/**
 * Create [ExecuteOptions] with seven typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit,
    noinline sideEffect7: (E7) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5, sideEffect6, sideEffect7)

/**
 * Create [ExecuteOptions] with eight typed side effects.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T, reified E8 : T> sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit,
    noinline sideEffect7: (E7) -> Unit,
    noinline sideEffect8: (E8) -> Unit
): ExecuteOptions<T> =
    typedOptions<T>().sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5, sideEffect6, sideEffect7, sideEffect8)

/**
 * Return new [ExecuteOptions] that invoke [sideEffect] with matching events as a list.
 */
@JvmName("sideEffectOnList")
inline fun <E : Any, reified E_SPECIFIC : E> ExecuteOptions<E>.sideEffectOnList(
    noinline sideEffect: (List<E_SPECIFIC>) -> Unit
): ExecuteOptions<E> =
    addSideEffect(Consumer { stream -> sideEffect(stream.toList().filterIsInstance<E_SPECIFIC>()) })

/**
 * Return new [ExecuteOptions] that invoke [sideEffect] with matching events as a sequence.
 */
@JvmName("sideEffectOnSequence")
inline fun <E : Any, reified E_SPECIFIC : E> ExecuteOptions<E>.sideEffectOnSequence(
    noinline sideEffect: (Sequence<E_SPECIFIC>) -> Unit
): ExecuteOptions<E> =
    addSideEffect(Consumer { stream -> sideEffect(stream.asSequence().filterIsInstance<E_SPECIFIC>()) })

/**
 * Deprecated alias for [sideEffectOnList].
 */
@Deprecated(
    message = "Use sideEffectOnList(sideEffect) to avoid ambiguity with Java Stream-based sideEffect.",
    replaceWith = ReplaceWith("this.sideEffectOnList(sideEffect)")
)
@JvmName("deprecatedSideEffectList")
inline fun <E : Any, reified E_SPECIFIC : E> ExecuteOptions<E>.sideEffect(
    noinline sideEffect: (List<E_SPECIFIC>) -> Unit
): ExecuteOptions<E> =
    sideEffectOnList(sideEffect)

/**
 * Deprecated alias for [sideEffectOnSequence].
 */
@Deprecated(
    message = "Use sideEffectOnSequence(sideEffect) to avoid ambiguity with Java Stream-based sideEffect.",
    replaceWith = ReplaceWith("this.sideEffectOnSequence(sideEffect)")
)
@JvmName("deprecatedSideEffectSequence")
inline fun <E : Any, reified E_SPECIFIC : E> ExecuteOptions<E>.sideEffect(
    noinline sideEffect: (Sequence<E_SPECIFIC>) -> Unit
): ExecuteOptions<E> =
    sideEffectOnSequence(sideEffect)

/**
 * Append two typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit
): ExecuteOptions<T> =
    addTypedSideEffect(E1::class.java, sideEffect1)
        .addTypedSideEffect(E2::class.java, sideEffect2)

/**
 * Append three typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2)
        .addTypedSideEffect(E3::class.java, sideEffect3)

/**
 * Append four typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2, sideEffect3)
        .addTypedSideEffect(E4::class.java, sideEffect4)

/**
 * Append five typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4)
        .addTypedSideEffect(E5::class.java, sideEffect5)

/**
 * Append six typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5)
        .addTypedSideEffect(E6::class.java, sideEffect6)

/**
 * Append seven typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit,
    noinline sideEffect7: (E7) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5, sideEffect6)
        .addTypedSideEffect(E7::class.java, sideEffect7)

/**
 * Append eight typed side effects to this [ExecuteOptions].
 */
inline fun <T : Any, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T, reified E8 : T> ExecuteOptions<*>.sideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit,
    noinline sideEffect3: (E3) -> Unit,
    noinline sideEffect4: (E4) -> Unit,
    noinline sideEffect5: (E5) -> Unit,
    noinline sideEffect6: (E6) -> Unit,
    noinline sideEffect7: (E7) -> Unit,
    noinline sideEffect8: (E8) -> Unit
): ExecuteOptions<T> =
    sideEffect(sideEffect1, sideEffect2, sideEffect3, sideEffect4, sideEffect5, sideEffect6, sideEffect7)
        .addTypedSideEffect(E8::class.java, sideEffect8)

/**
 * Compose [sideEffect] with any existing Java stream-based side effect already present in this [ExecuteOptions].
 */
@PublishedApi
@Suppress("UNCHECKED_CAST")
internal fun <E : Any> ExecuteOptions<*>.addSideEffect(sideEffect: Consumer<Stream<E>>): ExecuteOptions<E> {
    val existingSideEffect = this.sideEffect() as Consumer<Stream<E>>?
    val composedSideEffect = if (existingSideEffect == null) {
        sideEffect
    } else {
        Consumer<Stream<E>> { stream ->
            val events = stream.toList()
            existingSideEffect.accept(events.stream())
            sideEffect.accept(events.stream())
        }
    }

    val filter = filter()
    return if (filter == null) {
        ExecuteOptions.options<E>().sideEffect<E>(composedSideEffect)
    } else {
        ExecuteOptions.options<E>().filter<E>(filter).sideEffect<E>(composedSideEffect)
    }
}

/**
 * Compose a typed [sideEffect] that only receives events assignable to [eventType].
 */
@PublishedApi
internal fun <T : Any, E_SPECIFIC : T> ExecuteOptions<*>.addTypedSideEffect(
    eventType: Class<E_SPECIFIC>,
    sideEffect: (E_SPECIFIC) -> Unit
): ExecuteOptions<T> =
    addSideEffect(PolicySideEffect.executePolicy(eventType, Consumer(sideEffect)))

/**
 * Create typed empty [ExecuteOptions] for internal helper composition.
 */
@PublishedApi
internal fun <E : Any> typedOptions(): ExecuteOptions<E> = ExecuteOptions.options()
