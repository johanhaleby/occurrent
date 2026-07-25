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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.OrderStatusView;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.orderStatusProjection;

/**
 * Feeds the projection DSL with <strong>domain events</strong> directly, with no CloudEvent conversion. This is how you
 * run a projection when your broker listener already deserializes domain events (its own message converter), so the live
 * path never round-trips through {@code toCloudEvent}/{@code toDomainEvent}.
 * <p>
 * {@code Projections.domainEventFeed(...)} returns the sink the listener calls. Call {@code update(event)} when the
 * message carries only the event, or {@code update(metadata, event)} when it also carries the stream id, version or
 * position, which is what a projection keyed on metadata needs. This is the live-tail form; for a new or rebuilt
 * projection that also needs catch-up, use {@code CatchupProjectionFeed} instead.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFedOrderStatusProjectionTest {

    @Test
    void a_listener_feeds_the_projection_dsl_with_domain_events_directly() {
        ConcurrentHashMap<String, OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);

        Projection<OrderStatusView, OrderEvent, String> projection = orderStatusProjection();

        // The sink the broker listener calls with each already-decoded domain event.
        MaterializedView<OrderEvent> feed = Projections.domainEventFeed(projection, repository);

        for (OrderEvent event : List.of(new OrderPlaced("order-1", "The Pragmatic Programmer"), new OrderShipped("order-1"))) {
            feed.update(event); // folded straight into the read model, no CloudEvent involved
        }

        assertThat(store.get("order-1")).isEqualTo(new OrderStatusView("order-1", "The Pragmatic Programmer", "SHIPPED"));
    }
}
