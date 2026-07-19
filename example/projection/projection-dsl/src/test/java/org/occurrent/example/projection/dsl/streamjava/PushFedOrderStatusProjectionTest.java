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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.OrderStatusView;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.example.projection.dsl.streamjava.OrderStatusProjection.orderStatusProjection;

/**
 * Feeds the projection DSL from an external push source instead of a MongoDB change stream. In production this is how
 * you run the DSL when the app already forwards events to a broker (RabbitMQ, Kafka, ...) and consumes them with a
 * listener, rather than reading change streams.
 * <p>
 * The {@link PushSubscriptionModel} is the seam: the projection registers on it through the same
 * {@link ProjectionRunner} used for change-stream subscriptions, and the listener hands each received event to
 * {@link PushSubscriptionModel#accept(CloudEvent)}. Here a plain in-process queue stands in for the broker so the
 * example stays Docker-free; a real listener would deserialize the CloudEvents JSON off the broker and reconstruct the
 * {@link CloudEvent} before calling {@code accept}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PushFedOrderStatusProjectionTest {

    @Test
    void a_listener_feeds_the_projection_dsl_through_the_push_subscription_model() {
        CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:projection-dsl"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(OrderEvent.class))
                .idMapper(event -> UUID.randomUUID().toString())
                .build();

        // The read model store: any key/value store works; here a plain map.
        ConcurrentHashMap<String, OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);

        // The push model stands in for the change stream. The projection registers on it through the usual runner,
        // so nothing about the DSL changes; only the event source does.
        PushSubscriptionModel pushModel = new PushSubscriptionModel();
        Projection<OrderStatusView, OrderEvent, String> projection = orderStatusProjection();
        ProjectionRunner.agnostic(pushModel, converter).project("order-status", projection, repository);

        // The application's broker listener: on each message it reconstructs the CloudEvent and hands it to the model.
        // Feeding the model IS the higher-order function that drives the projection.
        Consumer<CloudEvent> brokerListener = pushModel::accept;

        // A message arrives for each forwarded event, in order, and the projection reacts synchronously on this thread.
        for (OrderEvent event : List.of(new OrderPlaced("order-1", "The Pragmatic Programmer"), new OrderShipped("order-1"))) {
            brokerListener.accept(converter.toCloudEvent(event));
        }

        assertThat(store.get("order-1")).isEqualTo(new OrderStatusView("order-1", "The Pragmatic Programmer", "SHIPPED"));
    }
}
