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

package org.occurrent.example.saga.orderfulfillment.machine;

import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.example.saga.orderfulfillment.CancelOrder;
import org.occurrent.example.saga.orderfulfillment.OrderCommand;
import org.occurrent.example.saga.orderfulfillment.OrderEvent;
import org.occurrent.example.saga.orderfulfillment.OrderPlaced;
import org.occurrent.example.saga.orderfulfillment.PaymentFailed;
import org.occurrent.example.saga.orderfulfillment.PaymentReserved;
import org.occurrent.example.saga.orderfulfillment.ReservePayment;
import org.occurrent.example.saga.orderfulfillment.ShipOrder;

import java.time.Duration;
import java.util.List;

/**
 * The order-fulfillment process expressed with the machine-core {@link Saga} builder: an explicit, per-event-type fold
 * and reaction over {@link OrderSagaState}. See {@code org.occurrent.example.saga.orderfulfillment.flow} for the same
 * process expressed with the declarative flow DSL instead.
 * <p>
 * The process: {@code OrderPlaced} reserves payment and arms a payment timeout; {@code PaymentReserved} ships the order
 * and clears the timeout; {@code PaymentFailed} cancels the order and clears the timeout; the payment timeout firing
 * (nobody having reserved or failed the payment in time) also cancels the order.
 */
public final class OrderFulfillmentSaga {

    public static final String PAYMENT_TIMER = "payment";

    private OrderFulfillmentSaga() {
    }

    public static Saga<OrderEvent, OrderSagaState, OrderCommand> orderFulfillment(Duration paymentTimeout) {
        return Saga.<OrderEvent, OrderSagaState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(
                        SagaEffect.issue(new ReservePayment(e.orderId(), e.amount())),
                        SagaEffect.startTimeout(PAYMENT_TIMER, paymentTimeout)))
                .evolve(PaymentReserved.class, (state, e) -> new Completed(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> List.of(
                        SagaEffect.issue(new ShipOrder(e.orderId())),
                        SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolve(PaymentFailed.class, (state, e) -> new Cancelled(e.orderId(), e.reason()))
                .react(PaymentFailed.class, (state, e) -> List.of(
                        SagaEffect.issue(new CancelOrder(e.orderId(), e.reason())),
                        SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolveOnTimeout(PAYMENT_TIMER, (state, t) -> new Cancelled(t.sagaId(), "payment timeout"))
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId(), "payment timeout"))))
                .isTerminal(state -> state instanceof Completed || state instanceof Cancelled)
                .build();
    }
}
