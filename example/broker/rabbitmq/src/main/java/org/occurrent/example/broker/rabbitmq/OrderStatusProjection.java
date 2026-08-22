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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;

/**
 * A read model kept up to date by a {@code @Projection(source = PUSH)} fed from RabbitMQ, at the CloudEvent level
 * through {@code PushSubscriptionModel} or at the domain level through {@code DomainEventFeed}. Both levels
 * register this same descriptor, so what differs between the two example tests is only how the event reaches the
 * fold, never the fold itself.
 * <p>
 * Every fold here is idempotent under redelivery, on purpose, including out of order. A catch-up replay and a live
 * broker redelivery can both reach the same stored event, and at-least-once delivery permits {@code OrderShipped}
 * to arrive before {@code OrderPlaced} does, not only after it. Neither handler ever returns {@code null}.
 * A {@link org.occurrent.dsl.view.ViewStateRepository} does not accept a {@code null} state, so a handler that
 * answered {@code null} for an event it wanted to ignore would fail the save instead, which redelivers the same
 * event forever and, at the bridge's default prefetch of one, blocks every later delivery behind it. {@code
 * OrderShipped} with no view yet therefore records the shipment as its own view, {@code product} left {@code null}
 * until {@code OrderPlaced} fills it in, rather than declining to answer. {@code OrderPlaced} always writes its own
 * fields, including the metadata, but never regresses a status the view already reached. Redelivering either
 * event, in either order, leaves the view at the same status it already had.
 * <p>
 * {@code OrderPlaced} is folded through the metadata-aware handler so the domain-level example can prove that the
 * stream id, stream version and global position survive the round trip through RabbitMQ's message headers, the
 * same {@link org.occurrent.cloudevents.EventMetadata} a catch-up replay would have handed it. {@code OrderShipped}
 * is folded through it too, for the same reason on the one path where it reaches the view first. Neither handler
 * assumes that metadata is actually there: ADR 133 decision 7 says an event published through {@code publish(E)}
 * carries no stream identity at all, and a consume bridge has no guarantee the source it is fed from is even an
 * Occurrent application. Both handlers read the stream id and version defensively rather than unconditionally, so
 * such an event still folds a view instead of throwing and, at the bridge's default {@code REDELIVER} policy and
 * prefetch of one, redelivering forever and blocking every later delivery behind it.
 */
public final class OrderStatusProjection {

    private OrderStatusProjection() {
    }

    public static Projection<OrderStatusView, OrderEvent, String> orderStatusProjection() {
        return Projection.<OrderStatusView, OrderEvent, String>builder(null)
                .id(OrderEvent::orderId)
                .on(OrderPlaced.class, (state, metadata, event) -> new OrderStatusView(
                        event.orderId(), event.product(), state == null ? "PLACED" : state.status(),
                        streamId(metadata), streamVersion(metadata), metadata.getPosition()))
                .on(OrderShipped.class, (state, metadata, event) -> state == null
                        ? new OrderStatusView(event.orderId(), null, "SHIPPED",
                                streamId(metadata), streamVersion(metadata), metadata.getPosition())
                        : ("PLACED".equals(state.status()) ? state.withStatus("SHIPPED") : state))
                .build();
    }

    // metadata.getStreamId()/getStreamVersion() throw when the stream extension is absent, which ADR 133 decision 7
    // says a publish(E) event, one arriving with no stream identity at all, legitimately does. Read nullable here
    // instead of unconditionally, so an event shaped that way folds a view with no stream identity recorded rather
    // than failing the save, which would redeliver the same event forever and, at the bridge's default prefetch of
    // one, block every later delivery behind it, the exact failure mode this class's own javadoc argues against.
    private static @Nullable String streamId(EventMetadata metadata) {
        return metadata.get(OccurrentCloudEventExtension.STREAM_ID);
    }

    // 0L stands in for "no stream version", the same way streamId() answers null. OrderStatusView.streamVersion is
    // a primitive long, not a boxed Long, so there is no other way to represent absence in that field.
    private static long streamVersion(EventMetadata metadata) {
        Object streamVersion = metadata.get(OccurrentCloudEventExtension.STREAM_VERSION);
        return streamVersion == null ? 0L : metadata.getStreamVersion();
    }

    /**
     * The materialized read model, holding an order, its current status, and the
     * {@link org.occurrent.cloudevents.EventMetadata} {@code OrderPlaced} arrived with, so a test can assert the
     * round trip directly off the view instead of reaching into the bridge. {@code OrderShipped} never overwrites
     * it once {@code OrderPlaced} has folded, {@code withStatus} only changes {@code status}, so this is
     * {@code OrderPlaced}'s metadata even when {@code OrderShipped} folds after it. The one exception is a view an
     * out-of-order {@code OrderShipped} created with nothing placed yet. Until {@code OrderPlaced} arrives and
     * fills {@code product} in, the metadata here is {@code OrderShipped}'s own.
     */
    public record OrderStatusView(String orderId, @Nullable String product, String status,
                                   @Nullable String streamId, long streamVersion, @Nullable Long position) {
        OrderStatusView withStatus(String newStatus) {
            return new OrderStatusView(orderId, product, newStatus, streamId, streamVersion, position);
        }
    }
}
