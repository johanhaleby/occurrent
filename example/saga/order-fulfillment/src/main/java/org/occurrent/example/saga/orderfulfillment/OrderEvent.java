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

package org.occurrent.example.saga.orderfulfillment;

/**
 * The order-fulfillment domain's events. Every permitted type is top-level (not nested), so the reflection-based
 * CloudEvent type mapper can resolve each one from its simple name. {@link OrderShipped} and {@link OrderCancelled} are
 * never written by the saga itself, they are written by the command dispatchers in this example, standing in for the
 * shipping and cancellation services that would react to {@link org.occurrent.example.saga.orderfulfillment.ShipOrder}
 * and {@link org.occurrent.example.saga.orderfulfillment.CancelOrder} in a real system.
 */
public sealed interface OrderEvent permits OrderPlaced, PaymentReserved, PaymentFailed, PaymentReservationRequested, OrderShipped, OrderCancelled {
    String orderId();

    /**
     * A JavaBean getter delegating to {@link #orderId()}, so Kotlin reads the id as a property
     * ({@code event.orderId}) on the interface type, matching the record-component access on the concrete events.
     */
    default String getOrderId() {
        return orderId();
    }
}
