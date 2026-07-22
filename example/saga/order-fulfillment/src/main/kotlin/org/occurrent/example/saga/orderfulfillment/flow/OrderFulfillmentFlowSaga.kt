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

package org.occurrent.example.saga.orderfulfillment.flow

import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.flow.FlowState
import org.occurrent.dsl.saga.flow.initiating
import org.occurrent.dsl.saga.flow.saga
import org.occurrent.example.saga.orderfulfillment.CancelOrder
import org.occurrent.example.saga.orderfulfillment.OrderCommand
import org.occurrent.example.saga.orderfulfillment.OrderEvent
import org.occurrent.example.saga.orderfulfillment.OrderPlaced
import org.occurrent.example.saga.orderfulfillment.PaymentFailed
import org.occurrent.example.saga.orderfulfillment.PaymentReserved
import org.occurrent.example.saga.orderfulfillment.ReservePayment
import org.occurrent.example.saga.orderfulfillment.ShipOrder
import java.time.Duration

/**
 * The same order-fulfillment process as [org.occurrent.example.saga.orderfulfillment.machine.OrderFulfillmentSaga],
 * expressed instead with the declarative Kotlin flow `saga { }` block: one step, two branches and a timeout, rather than
 * an explicit per-event-type fold and reaction.
 */
fun orderFulfillmentFlow(paymentTimeout: Duration): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
    saga {
        correlateAll { it.orderId }
        startsOn<OrderPlaced> { order ->
            issue(ReservePayment(order.orderId, order.amount))
        }
        step("awaiting-payment") {
            on<PaymentReserved>(then = end) { payment -> issue(ShipOrder(payment.orderId)) }
            on<PaymentFailed>(then = end) { failure -> issue(CancelOrder(failure.orderId, failure.reason)) }
            timeout(after = paymentTimeout, then = end) { received ->
                issue(CancelOrder(received.initiating<OrderPlaced>().orderId, "payment timeout"))
            }
        }
    }
