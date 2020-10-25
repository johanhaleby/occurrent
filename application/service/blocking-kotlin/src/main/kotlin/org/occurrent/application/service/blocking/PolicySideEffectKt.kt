package org.occurrent.application.service.blocking


/**
 * Execute policy as a side effect after events are written to the event store
 */
inline fun <T, reified E : T> PolicySideEffect<T>.executePolicy(noinline policy: (E) -> Unit): PolicySideEffect<T> {
    return PolicySideEffect.executePolicy(E::class.java, policy)
}