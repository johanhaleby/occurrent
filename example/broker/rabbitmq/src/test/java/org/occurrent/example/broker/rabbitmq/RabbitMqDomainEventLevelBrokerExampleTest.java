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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.springboot.blocking.OccurrentBlockingAnnotationConfiguration;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The domain-level half of the broker example. {@code CloudEventForwarder} publishes to RabbitMQ,
 * {@code RabbitMqDomainEventBridge} bridges the queue into a {@code DomainEventFeed}, and a
 * {@code @Projection(source = PUSH)} keeps a read model up to date directly from domain events, never decoding a
 * {@code CloudEvent} on the fold's own side.
 * <p>
 * Proves the two things ADR 133 changed here specifically. The domain bridge consumes nothing while a catch-up
 * replay is running, so an event forwarded before the projection is registered waits on the queue itself rather
 * than in an in-memory buffer, and once the replay hands over, the same event arrives a second time over the
 * broker and is folded exactly once. And {@link EventMetadata} (the stream id, the stream version, the global
 * position) survives the round trip through RabbitMQ's message headers for an event that reaches the feed only
 * through the broker, never through a catch-up replay reading the store directly.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RabbitMqDomainEventLevelBrokerExampleTest extends AbstractBrokerExampleTest {

    @Test
    void an_order_forwarded_before_the_projection_registers_overlaps_the_queue_and_a_later_order_proves_the_metadata_round_trip() throws Exception {
        CloudEventConverter<OrderEvent> converter = newConverter();
        CloudEventTypeMapper<OrderEvent> typeMapper = newTypeMapper();
        RabbitMqTopicExchangeDestinationResolver resolver = newResolver(typeMapper);
        MongoEventStore eventStore = newEventStore();

        CheckpointStorage domainCatchupMarker = new NativeMongoCheckpointStorage(mongoClient.getDatabase(databaseName), "domain-catchup-checkpoints");
        DomainEventFeed<OrderEvent> feed = new DomainEventFeed<>(eventStore, converter, OrderEvent::eventId, domainCatchupMarker);

        try (RabbitMqDomainEventBridge<OrderEvent> bridge = RabbitMqDomainEventBridge.builder(rabbitConnection, feed, queue)
                .resolver(resolver)
                .pollInterval(Duration.ofMillis(50))
                .build();
             RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(rabbitConnection, resolver).build()) {

            NativeMongoSubscriptionModel forwarderSubscriptionModel = new NativeMongoSubscriptionModel(
                    mongoClient.getDatabase(databaseName), EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING, Executors.newVirtualThreadPerTaskExecutor());
            CheckpointStorage forwarderCheckpoints = new NativeMongoCheckpointStorage(mongoClient.getDatabase(databaseName), "forwarder-checkpoints");
            DurableSubscriptionModel forwarderSubscription = new DurableSubscriptionModel(forwarderSubscriptionModel, forwarderCheckpoints);
            CloudEventForwarder forwarder = new CloudEventForwarder(forwarderSubscription, sink);
            forwarder.forward("forward-orders-domain");

            // Forwarded before anything is registered on the feed. RabbitMqDomainEventBridge.reconcileConsumption
            // gates on feed.isReadyForLiveDelivery(), which is false until a catch-up reaches live, so these two
            // messages queue up on the broker rather than being pulled off and buffered in memory.
            String orderBeforeCatchup = "order-" + UUID.randomUUID();
            eventStore.write(orderBeforeCatchup, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderBeforeCatchup, "Widget")));
            eventStore.write(orderBeforeCatchup, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderBeforeCatchup)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(adminChannel.queueDeclarePassive(queue).getMessageCount()).isEqualTo(2));

            Map<String, OrderStatusProjection.OrderStatusView> store = new ConcurrentHashMap<>();
            ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);

            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
            context.register(PropertiesConfig.class, OccurrentBlockingAnnotationConfiguration.class);
            // The registrar looks this bean up by type for every push projection it wires, a domain-fed one
            // included, so it has to be present even though the fold itself never touches a CloudEvent.
            context.registerBean("cloudEventConverter", CloudEventConverter.class, () -> converter);
            context.registerBean("domainEventFeed", DomainEventFeed.class, () -> feed);
            context.registerBean("viewStateRepository", ViewStateRepository.class, () -> repository);
            context.registerBean("orderProjectionHolder", OrderProjectionHolder.class, OrderProjectionHolder::new);
            try {
                context.refresh();

                // The default startup mode replays synchronously, so by the time refresh() returns the catch-up
                // has already folded both events straight from the store, before the bridge ever touched them.
                assertThat(store.get(orderBeforeCatchup)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED");

                // The bridge's coarse poll now notices the feed is ready for live delivery and starts consuming
                // the backlog it left untouched. Both messages arrive a second time, are recognised as already
                // delivered and are not folded again (the view above is unaffected), but are still acknowledged.
                // Closing the bridge first requeues anything still outstanding, so a nonzero count here would mean
                // a redelivered message was never actually acknowledged, not that the check ran too early.
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(adminChannel.queueDeclarePassive(queue).getMessageCount()).isZero());

                // A second order, published only after the feed is already live. It never touches the catch-up
                // replay, so its EventMetadata can only have reached the projection through the broker.
                String orderAfterCatchup = "order-" + UUID.randomUUID();
                CloudEvent placed = converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderAfterCatchup, "Gadget"));
                eventStore.write(orderAfterCatchup, placed);
                eventStore.write(orderAfterCatchup, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderAfterCatchup)));

                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(store.get(orderAfterCatchup)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED"));

                CloudEvent persistedPlaced = eventStore.read(orderAfterCatchup).events().findFirst().orElseThrow();
                EventMetadata expectedMetadata = EventMetadata.from(persistedPlaced);
                OrderStatusProjection.OrderStatusView view = store.get(orderAfterCatchup);
                assertThat(view.streamId()).isEqualTo(expectedMetadata.getStreamId());
                assertThat(view.streamVersion()).isEqualTo(expectedMetadata.getStreamVersion());
                assertThat(view.position()).isEqualTo(expectedMetadata.getPosition());
            } finally {
                context.close();
            }
            forwarderSubscription.shutdown();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PropertiesConfig {
    }

    static class OrderProjectionHolder {
        @Projection(id = "order-status-domain", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<OrderStatusProjection.OrderStatusView, OrderEvent, String> projection() {
            return OrderStatusProjection.orderStatusProjection();
        }
    }
}
