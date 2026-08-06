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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.OrderStatusView;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.orderStatusProjection;

@DisplayNameGeneration(ReplaceUnderscores.class)
class OrderStatusProjectionTest {

    private InMemorySubscriptionModel subscriptionModel;
    private ApplicationService<OrderEvent> applicationService;
    private CloudEventConverter<OrderEvent> converter;

    @BeforeEach
    void setup() {
        subscriptionModel = new InMemorySubscriptionModel();
        InMemoryEventStore eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:projection-dsl"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(OrderEvent.class))
                .idMapper(event -> UUID.randomUUID().toString())
                .build();
        applicationService = new GenericApplicationService<>(eventStore, converter);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void stream_projection_subscribes_and_materializes_the_order_status() {
        // The read model store: any key/value store works; here a plain map.
        ConcurrentHashMap<String, OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);

        Projection<OrderStatusView, OrderEvent, String> projection = orderStatusProjection();

        // One call creates the subscription (its filter derived from the handler event types) and materializes the view.
        ProjectionRunner.stream(subscriptionModel, converter).project("order-status", projection, repository);

        applicationService.execute("order-1", events -> List.of(new OrderPlaced("order-1", "The Pragmatic Programmer")));
        applicationService.execute("order-1", events -> List.of(new OrderShipped("order-1")));

        assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue();
        assertThat(store.get("order-1")).isEqualTo(new OrderStatusView("order-1", "The Pragmatic Programmer", "SHIPPED"));
    }
}
