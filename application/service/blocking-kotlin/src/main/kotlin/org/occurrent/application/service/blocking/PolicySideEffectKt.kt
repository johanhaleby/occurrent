package org.occurrent.application.service.blocking

import kotlin.streams.asStream

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2)(sequence)
}

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                               noinline policy3: (E3) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3)(sequence)
}

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                                               noinline policy3: (E3) -> Unit, noinline policy4: (E4) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3).andThenExecutePolicy<T, E4>(policy4)(sequence)
}

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                                                               noinline policy3: (E3) -> Unit, noinline policy4: (E4) -> Unit,
                                                                                                               noinline policy5: (E5) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3).andThenExecutePolicy<T, E4>(policy4)
            .andThenExecutePolicy<T, E5>(policy5)(sequence)
}

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                                                                               noinline policy3: (E3) -> Unit, noinline policy4: (E4) -> Unit,
                                                                                                                               noinline policy5: (E5) -> Unit, noinline policy6: (E6) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3).andThenExecutePolicy<T, E4>(policy4)
            .andThenExecutePolicy<T, E5>(policy5).andThenExecutePolicy<T, E6>(policy6)(sequence)
}


/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                                                                                               noinline policy3: (E3) -> Unit, noinline policy4: (E4) -> Unit,
                                                                                                                                               noinline policy5: (E5) -> Unit, noinline policy6: (E6) -> Unit,
                                                                                                                                               noinline policy7: (E7) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3).andThenExecutePolicy<T, E4>(policy4)
            .andThenExecutePolicy<T, E5>(policy5).andThenExecutePolicy<T, E6>(policy6).andThenExecutePolicy<T, E7>(policy7)(sequence)
}

/**
 * Execute multiple policies sequentially from left to right
 */
inline fun <T, reified E1 : T, reified E2 : T, reified E3 : T, reified E4 : T, reified E5 : T, reified E6 : T, reified E7 : T, reified E8 : T> executePolicies(noinline policy1: (E1) -> Unit, noinline policy2: (E2) -> Unit,
                                                                                                                                                               noinline policy3: (E3) -> Unit, noinline policy4: (E4) -> Unit,
                                                                                                                                                               noinline policy5: (E5) -> Unit, noinline policy6: (E6) -> Unit,
                                                                                                                                                               noinline policy7: (E7) -> Unit, noinline policy8: (E8) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    executePolicy<T, E1>(policy1).andThenExecutePolicy<T, E2>(policy2).andThenExecutePolicy<T, E3>(policy3).andThenExecutePolicy<T, E4>(policy4)
            .andThenExecutePolicy<T, E5>(policy5).andThenExecutePolicy<T, E6>(policy6).andThenExecutePolicy<T, E7>(policy7).andThenExecutePolicy<T, E8>(policy8)(sequence)
}

/**
 * Execute policy as a side effect after events are written to the event store
 */
inline fun <T, reified E : T> executePolicy(noinline policy: (E) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    PolicySideEffect.executePolicy<T, E>(E::class.java, policy).accept(sequence.asStream())
}

/**
 * Compose with another policy
 */
inline fun <T, reified E : T> ((Sequence<T>) -> Unit).andThenExecutePolicy(noinline policy: (E) -> Unit): (Sequence<T>) -> Unit = { sequence: Sequence<T> ->
    val list = sequence.toList()
    this(list.asSequence())
    PolicySideEffect.executePolicy<T, E>(E::class.java, policy).accept(list.asSequence().asStream())
}