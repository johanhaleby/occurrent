package org.occurrent.application.service.blocking.dcb

import org.occurrent.application.service.blocking.SideEffect

/**
 * Create empty [DcbExecuteOptions] for Kotlin call sites, mirroring the stream `options()` helper.
 *
 * Returns `DcbExecuteOptions<Any>` so it works as a chain starter without a call-site type argument. A chained
 * [DcbExecuteOptions.sideEffect] narrows the event type from its side-effect.
 */
fun dcbExecuteOptions(): DcbExecuteOptions<Any> = DcbExecuteOptions.empty()

/**
 * Create [DcbExecuteOptions] with a typed side-effect that is invoked for events matching [E] after the
 * produced events have been appended successfully.
 */
inline fun <T : Any, reified E : T> dcbSideEffect(noinline sideEffect: (E) -> Unit): DcbExecuteOptions<T> =
    DcbExecuteOptions.empty<T>().sideEffect(SideEffect.executeSideEffect<T, E>(E::class.java, sideEffect))

/**
 * Create [DcbExecuteOptions] with two typed side-effects, composed and invoked after a successful append.
 */
inline fun <T : Any, reified E1 : T, reified E2 : T> dcbSideEffect(
    noinline sideEffect1: (E1) -> Unit,
    noinline sideEffect2: (E2) -> Unit
): DcbExecuteOptions<T> =
    DcbExecuteOptions.empty<T>().sideEffect(
        SideEffect.executeSideEffect<T, E1>(E1::class.java, sideEffect1).andThenExecuteAnotherSideEffect<E2>(E2::class.java, sideEffect2)
    )
