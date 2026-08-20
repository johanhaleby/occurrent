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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.view.View;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fold itself, without a broker or a store in the loop, so a redelivery order the two example tests cannot
 * force through RabbitMQ can still be exercised directly. At-least-once delivery permits {@code OrderPlaced} to
 * arrive again after {@code OrderShipped} already landed, and permits {@code OrderShipped} to arrive before
 * {@code OrderPlaced} ever does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OrderStatusProjectionTest {

    private final View<OrderStatusProjection.OrderStatusView, OrderEvent> view = OrderStatusProjection.orderStatusProjection().view();

    @Test
    void redelivering_order_placed_after_order_shipped_does_not_regress_the_status() {
        String orderId = "order-" + UUID.randomUUID();
        OrderPlaced placed = new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget");
        OrderShipped shipped = new OrderShipped(UUID.randomUUID().toString(), orderId);
        EventMetadata metadata = metadataFor("stream-1", 0L, 10L);

        OrderStatusProjection.OrderStatusView afterPlaced = view.evolve(null, metadata, placed);
        OrderStatusProjection.OrderStatusView afterShipped = view.evolve(afterPlaced, EventMetadata.empty(), shipped);
        OrderStatusProjection.OrderStatusView afterRedeliveredPlaced = view.evolve(afterShipped, metadata, placed);

        assertThat(afterRedeliveredPlaced.status()).isEqualTo("SHIPPED");
    }

    @Test
    void order_shipped_arriving_before_order_placed_produces_no_view() {
        OrderShipped shipped = new OrderShipped(UUID.randomUUID().toString(), "order-" + UUID.randomUUID());

        OrderStatusProjection.OrderStatusView result = view.evolve(null, EventMetadata.empty(), shipped);

        assertThat(result).isNull();
    }

    private static EventMetadata metadataFor(String streamId, long streamVersion, long position) {
        return new EventMetadata(Map.of(
                OccurrentCloudEventExtension.STREAM_ID, streamId,
                OccurrentCloudEventExtension.STREAM_VERSION, streamVersion,
                OccurrentCloudEventExtension.POSITION, position));
    }
}
