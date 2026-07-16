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

package org.occurrent.example.projection.dsl.streamjava;

import org.occurrent.dsl.projection.Projection;

/**
 * A stream/agnostic read model built with the Java handler builder. Each order becomes one {@link OrderStatusView} keyed
 * by its id; the registered handler types are what the runner subscribes to, so there is no separate event-type list to
 * keep in sync.
 */
public final class OrderStatusProjection {

    private OrderStatusProjection() {
    }

    public static Projection<OrderStatusView, OrderEvent, String> orderStatusProjection() {
        return Projection.<OrderStatusView, OrderEvent, String>builder(null)
                .id(OrderEvent::orderId)
                .on(OrderPlaced.class, (state, event) -> new OrderStatusView(event.orderId(), event.product(), "PLACED"))
                .on(OrderShipped.class, (state, event) -> state.withStatus("SHIPPED"))
                .on(OrderCancelled.class, (state, event) -> state.withStatus("CANCELLED"))
                .build();
    }

    /** The materialized read model: an order and its current status. */
    public record OrderStatusView(String orderId, String product, String status) {
        OrderStatusView withStatus(String newStatus) {
            return new OrderStatusView(orderId, product, newStatus);
        }
    }
}
