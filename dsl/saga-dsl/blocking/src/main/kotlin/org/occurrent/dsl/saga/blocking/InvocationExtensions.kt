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

package org.occurrent.dsl.saga.blocking

import org.occurrent.command.Invocation
import org.occurrent.dsl.saga.SagaEffects
import org.occurrent.dsl.saga.flow.FlowReactions
import java.util.function.Function

/**
 * Issues the domain function itself instead of a command object, for a saga whose command type is [Invocation].
 * These live here rather than in `occurrent-saga-dsl-common`, which knows nothing about command dispatch (ADR 81).
 *
 * Both mirror the one-argument `issue(command)` they delegate to, including returning the receiver, which is what makes
 * a produced-but-discarded reaction a compile error.
 */

/**
 * Issue an [Invocation] that runs [decision] against the stream [streamId], the two-argument sibling of
 * `issue(command)`:
 *
 * ```kotlin
 * react<OrderPlaced> { _, e ->
 *     issue(e.orderId) { events -> reservePayment(events, e.amount) }
 *     startTimeout("payment", Duration.ofMinutes(30))
 * }
 * ```
 *
 * [E] is the event type of the stream being written to, which need not be the type the saga subscribes to.
 */
fun <E : Any> SagaEffects<Invocation<E>>.issue(streamId: String, decision: (List<E>) -> List<E>): SagaEffects<Invocation<E>> =
    issue(Invocation(streamId, Function { events -> decision(events) }))

/**
 * Issue an [Invocation] that runs [decision] against the stream [streamId], for a flow saga. Available in every flow
 * reaction, since `startsOn`, `on` and `timeout` all share this receiver:
 *
 * ```kotlin
 * on<PaymentReserved>(then = end) { issue(it.orderId) { events -> ship(events) } }
 * ```
 *
 * [E] is the event type of the stream being written to, which need not be the type the saga subscribes to.
 */
fun <E : Any> FlowReactions<Invocation<E>>.issue(streamId: String, decision: (List<E>) -> List<E>): FlowReactions<Invocation<E>> =
    issue(Invocation(streamId, Function { events -> decision(events) }))
