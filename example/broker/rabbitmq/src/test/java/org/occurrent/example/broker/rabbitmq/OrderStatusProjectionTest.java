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
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fold itself, without a broker or a store in the loop, so a redelivery order the two example tests cannot
 * force through RabbitMQ can still be exercised directly. At-least-once delivery permits {@code OrderPlaced} to
 * arrive again after {@code OrderShipped} already landed, and permits {@code OrderShipped} to arrive before
 * {@code OrderPlaced} ever does.
 * <p>
 * {@link #order_shipped_arriving_before_order_placed_is_saved_and_then_filled_in_without_crashing()} drives the
 * same reordering through {@link Projections#materializedView(org.occurrent.dsl.projection.Projection, ViewStateRepository)}
 * and a real {@link ViewStateRepository}, not just {@link View#evolve}. A fold that answered {@code null} for
 * {@code OrderShipped} would fail {@link #order_shipped_arriving_before_order_placed_produces_a_shipped_view_with_no_product_yet()}
 * too, since that test dereferences the fold's result immediately. This test exists to exercise the real
 * production path instead, where a real {@link ViewStateRepository} enforces that {@code save} is never called
 * with {@code null}.
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
    void order_shipped_arriving_before_order_placed_produces_a_shipped_view_with_no_product_yet() {
        OrderShipped shipped = new OrderShipped(UUID.randomUUID().toString(), "order-" + UUID.randomUUID());

        OrderStatusProjection.OrderStatusView result = view.evolve(null, metadataFor("stream-1", 0L, 10L), shipped);

        assertThat(result.status()).isEqualTo("SHIPPED");
        assertThat(result.product()).isNull();
    }

    /**
     * {@link #order_shipped_arriving_before_order_placed_produces_a_shipped_view_with_no_product_yet()} already
     * fails if the fold answers {@code null} for {@code OrderShipped}, since it dereferences the result immediately.
     * This test exists for a different reason. It drives the identical reordering through
     * {@link Projections#materializedView} and a real {@link ViewStateRepository} instead, to prove {@code save} is
     * never called with {@code null}, the contract a real deployment depends on.
     */
    @Test
    void order_shipped_arriving_before_order_placed_is_saved_and_then_filled_in_without_crashing() {
        String orderId = "order-" + UUID.randomUUID();
        OrderShipped shipped = new OrderShipped(UUID.randomUUID().toString(), orderId);
        OrderPlaced placed = new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget");

        Map<String, OrderStatusProjection.OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);
        MaterializedView<OrderEvent> materializedView = Projections.materializedView(OrderStatusProjection.orderStatusProjection(), repository);

        materializedView.update(metadataFor("stream-1", 0L, 10L), shipped);
        assertThat(store.get(orderId).status()).isEqualTo("SHIPPED");
        assertThat(store.get(orderId).product()).isNull();

        materializedView.update(metadataFor("stream-1", 1L, 11L), placed);
        assertThat(store.get(orderId).status()).isEqualTo("SHIPPED");
        assertThat(store.get(orderId).product()).isEqualTo("Widget");
    }

    @Test
    void redelivering_order_shipped_after_it_already_shipped_changes_nothing() {
        String orderId = "order-" + UUID.randomUUID();
        OrderPlaced placed = new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget");
        OrderShipped shipped = new OrderShipped(UUID.randomUUID().toString(), orderId);
        EventMetadata metadata = metadataFor("stream-1", 0L, 10L);

        OrderStatusProjection.OrderStatusView afterPlaced = view.evolve(null, metadata, placed);
        OrderStatusProjection.OrderStatusView afterShipped = view.evolve(afterPlaced, EventMetadata.empty(), shipped);
        OrderStatusProjection.OrderStatusView afterRedeliveredShipped = view.evolve(afterShipped, EventMetadata.empty(), shipped);

        assertThat(afterRedeliveredShipped).isEqualTo(afterShipped);
    }

    /**
     * A live broker queue delivers events for whichever order reaches it first, not grouped one order at a time,
     * so two orders placed before either ships is the ordinary case, not an edge one. Drives it through
     * {@link Projections#materializedView}, keyed by {@code orderId}, so each order's view comes from the
     * repository under its own id rather than from folding two independent {@link View#evolve} calls that could
     * never have shared state to begin with.
     */
    @Test
    void two_orders_interleaved_through_the_same_materialized_view_do_not_cross_contaminate() {
        String orderIdA = "order-" + UUID.randomUUID();
        String orderIdB = "order-" + UUID.randomUUID();
        OrderPlaced placedA = new OrderPlaced(UUID.randomUUID().toString(), orderIdA, "Widget");
        OrderPlaced placedB = new OrderPlaced(UUID.randomUUID().toString(), orderIdB, "Gadget");
        OrderShipped shippedA = new OrderShipped(UUID.randomUUID().toString(), orderIdA);
        OrderShipped shippedB = new OrderShipped(UUID.randomUUID().toString(), orderIdB);

        Map<String, OrderStatusProjection.OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);
        MaterializedView<OrderEvent> materializedView = Projections.materializedView(OrderStatusProjection.orderStatusProjection(), repository);

        // Interleaved: A placed, B placed, A shipped, B shipped, rather than each order finishing before the next starts.
        materializedView.update(metadataFor("stream-a", 0L, 10L), placedA);
        materializedView.update(metadataFor("stream-b", 0L, 11L), placedB);
        materializedView.update(EventMetadata.empty(), shippedA);
        materializedView.update(EventMetadata.empty(), shippedB);

        assertThat(store.get(orderIdA).status()).isEqualTo("SHIPPED");
        assertThat(store.get(orderIdA).product()).isEqualTo("Widget");
        assertThat(store.get(orderIdB).status()).isEqualTo("SHIPPED");
        assertThat(store.get(orderIdB).product()).isEqualTo("Gadget");
    }

    private static EventMetadata metadataFor(String streamId, long streamVersion, long position) {
        return new EventMetadata(Map.of(
                OccurrentCloudEventExtension.STREAM_ID, streamId,
                OccurrentCloudEventExtension.STREAM_VERSION, streamVersion,
                OccurrentCloudEventExtension.POSITION, position));
    }
}
