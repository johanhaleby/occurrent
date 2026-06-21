package org.occurrent.application.service.blocking.dcb

import org.occurrent.application.service.blocking.PolicySideEffect

/**
 * Create empty [DcbExecuteOptions] for Kotlin call sites.
 *
 * The event type is normally inferred from a chained [sideEffect] or from the surrounding `execute(...)` call.
 */
fun <E : Any> dcbExecuteOptions(): DcbExecuteOptions<E> = DcbExecuteOptions.empty()

/**
 * Create [DcbExecuteOptions] with a typed policy side-effect that is invoked for events matching [E] after the
 * produced events have been appended successfully.
 */
inline fun <T : Any, reified E : T> dcbSideEffect(noinline policy: (E) -> Unit): DcbExecuteOptions<T> =
    DcbExecuteOptions.empty<T>().sideEffect(PolicySideEffect.executePolicy<T, E>(E::class.java, policy))

/**
 * Create [DcbExecuteOptions] with two typed policy side-effects, composed and invoked after a successful append.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T> dcbSideEffect(
    noinline policy1: (E1) -> Unit,
    noinline policy2: (E2) -> Unit
): DcbExecuteOptions<T> =
    DcbExecuteOptions.empty<T>().sideEffect(
        PolicySideEffect.executePolicy<T, E1>(E1::class.java, policy1).andThenExecuteAnotherPolicy<E2>(E2::class.java, policy2)
    )
