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

package org.occurrent.example.broker.rabbitmq;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.projection.Projection;

/**
 * A read model kept up to date by a {@code @Projection(source = PUSH)} fed from RabbitMQ, at the CloudEvent level
 * through {@code PushSubscriptionModel} or at the domain level through {@code DomainEventFeed}. Both levels
 * register this same descriptor, so what differs between the two example tests is only how the event reaches the
 * fold, never the fold itself.
 * <p>
 * Every fold here is idempotent under redelivery, on purpose, including out of order. A catch-up replay and a live
 * broker redelivery can both reach the same stored event, and at-least-once delivery permits {@code OrderPlaced} to
 * arrive again after {@code OrderShipped} already landed, so neither handler trusts the event alone. {@code OrderPlaced}
 * only creates the view, it never overwrites an existing one, and {@code OrderShipped} only moves a view already at
 * {@code PLACED} to {@code SHIPPED}. Redelivering either event, in either order, leaves the view exactly where it was.
 * <p>
 * {@code OrderPlaced} is folded through the metadata-aware handler so the domain-level example can prove that the
 * stream id, stream version and global position survive the round trip through RabbitMQ's message headers, the
 * same {@link org.occurrent.cloudevents.EventMetadata} a catch-up replay would have handed it.
 */
public final class OrderStatusProjection {

    private OrderStatusProjection() {
    }

    public static Projection<OrderStatusView, OrderEvent, String> orderStatusProjection() {
        return Projection.<OrderStatusView, OrderEvent, String>builder(null)
                .id(OrderEvent::orderId)
                .on(OrderPlaced.class, (state, metadata, event) -> state == null
                        ? new OrderStatusView(event.orderId(), event.product(), "PLACED",
                                metadata.getStreamId(), metadata.getStreamVersion(), metadata.getPosition())
                        : state)
                .on(OrderShipped.class, (state, event) -> state != null && state.status().equals("PLACED")
                        ? state.withStatus("SHIPPED")
                        : state)
                .build();
    }

    /**
     * The materialized read model, holding an order, its current status, and the
     * {@link org.occurrent.cloudevents.EventMetadata} that {@code OrderPlaced} arrived with, so a test can assert
     * the round trip directly off the view instead of reaching into the bridge.
     */
    public record OrderStatusView(String orderId, String product, String status,
                                   @Nullable String streamId, long streamVersion, @Nullable Long position) {
        OrderStatusView withStatus(String newStatus) {
            return new OrderStatusView(orderId, product, newStatus, streamId, streamVersion, position);
        }
    }
}
