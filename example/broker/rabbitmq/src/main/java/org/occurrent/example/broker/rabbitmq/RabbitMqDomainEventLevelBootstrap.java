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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * A runnable, hand-wired bootstrap for the domain-level half of this broker example. It builds an event store, a
 * {@code CloudEventForwarder} publishing to RabbitMQ, a {@code RabbitMqDomainEventBridge} bridging the queue back
 * into a {@code DomainEventFeed}, and an {@link OrderStatusProjection} fed directly from domain events, entirely by
 * constructing each piece itself. There is no Spring Boot auto-configuration and no {@code application.yaml}
 * RabbitMQ configuration here. That arrives with the broker Spring Boot starter in
 * <a href="https://github.com/johanhaleby/occurrent/issues/846">#846</a>, and this class wires the same interfaces
 * that starter will eventually configure.
 * <p>
 * {@link #main(String[])} needs a real MongoDB single-node replica set and a real RabbitMQ, not Testcontainers.
 * Start both with:
 * <pre>{@code
 * docker run --rm -d --name occurrent-broker-example-mongo -p 27017:27017 mongo:8.0 --replSet rs0
 * docker exec occurrent-broker-example-mongo mongosh --eval "rs.initiate()"
 * docker run --rm -d --name occurrent-broker-example-rabbitmq -p 5672:5672 rabbitmq:4.1
 * }</pre>
 * give the Mongo container a couple of seconds to accept connections before the {@code rs.initiate()} call, then
 * run this class. It places and ships one order, waits for the read model to reach {@code SHIPPED}, and logs the
 * result, so the console shows the event travel the whole loop, from the store through the forwarder and the
 * broker to the bridge and the projection.
 *
 * @see RabbitMqCloudEventLevelBootstrap the CloudEvent-level half of the same example
 */
public final class RabbitMqDomainEventLevelBootstrap implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDomainEventLevelBootstrap.class);

    private static final String DATABASE_NAME = "occurrent_broker_example";
    private static final String EVENTS_COLLECTION = "events";
    private static final String EXCHANGE = "broker-example";
    private static final String QUEUE = "broker-example-domain";
    private static final String SUBSCRIPTION_ID = "broker-example-domain-projection";

    private final MongoEventStore eventStore;
    private final CloudEventConverter<OrderEvent> converter;
    private final DurableSubscriptionModel forwarderSubscription;
    private final RabbitMqCloudEventSink sink;
    private final RabbitMqDomainEventBridge<OrderEvent> bridge;
    private final Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews;

    private RabbitMqDomainEventLevelBootstrap(MongoEventStore eventStore, CloudEventConverter<OrderEvent> converter,
                                               DurableSubscriptionModel forwarderSubscription, RabbitMqCloudEventSink sink,
                                               RabbitMqDomainEventBridge<OrderEvent> bridge, Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews) {
        this.eventStore = eventStore;
        this.converter = converter;
        this.forwarderSubscription = forwarderSubscription;
        this.sink = sink;
        this.bridge = bridge;
        this.orderStatusViews = orderStatusViews;
    }

    /**
     * Wires the whole loop against an already-open {@link MongoClient} and {@link Connection}, so {@link #main}
     * and a test can share the exact same production wiring against different infrastructure.
     */
    public static RabbitMqDomainEventLevelBootstrap start(MongoClient mongoClient, Connection rabbitConnection) {
        MongoEventStore eventStore = new MongoEventStore(mongoClient, DATABASE_NAME, EVENTS_COLLECTION, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));

        CloudEventTypeMapper<OrderEvent> typeMapper = ReflectionCloudEventTypeMapper.simple(OrderEvent.class);
        CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:broker"))
                .typeMapper(typeMapper)
                .idMapper(OrderEvent::eventId)
                .build();
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(EXCHANGE, typeMapper);

        // Neither the sink nor the bridge declares the exchange itself, only queues and bindings against one that
        // already exists, so the application declares it, on its own short-lived channel.
        declareExchange(rabbitConnection);

        NativeMongoSubscriptionModel forwarderSubscriptionModel = new NativeMongoSubscriptionModel(
                mongoClient.getDatabase(DATABASE_NAME), EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING, Executors.newVirtualThreadPerTaskExecutor());
        CheckpointStorage forwarderCheckpoints = new NativeMongoCheckpointStorage(mongoClient.getDatabase(DATABASE_NAME), "forwarder-checkpoints");
        DurableSubscriptionModel forwarderSubscription = new DurableSubscriptionModel(forwarderSubscriptionModel, forwarderCheckpoints);
        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(rabbitConnection, resolver).build();
        CloudEventForwarder forwarder = new CloudEventForwarder(forwarderSubscription, sink);
        forwarder.forward(SUBSCRIPTION_ID + "-forwarder");

        CheckpointStorage catchupMarker = new NativeMongoCheckpointStorage(mongoClient.getDatabase(DATABASE_NAME), "domain-catchup-checkpoints");
        DomainEventFeed<OrderEvent> feed = new DomainEventFeed<>(eventStore, converter, OrderEvent::eventId, catchupMarker);

        Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(orderStatusViews::get, orderStatusViews::put);
        feed.register(SUBSCRIPTION_ID, OrderStatusProjection.orderStatusProjection(), repository);
        feed.catchUp(SUBSCRIPTION_ID);

        RabbitMqDomainEventBridge<OrderEvent> bridge = RabbitMqDomainEventBridge.builder(rabbitConnection, feed, QUEUE)
                .resolver(resolver)
                .build();

        return new RabbitMqDomainEventLevelBootstrap(eventStore, converter, forwarderSubscription, sink, bridge, orderStatusViews);
    }

    /**
     * The read model this bootstrap keeps up to date, live as events travel the whole loop.
     */
    public Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews() {
        return orderStatusViews;
    }

    /**
     * Writes an {@code OrderPlaced} and an {@code OrderShipped} for a fresh order id, then blocks until the read
     * model reports it {@code SHIPPED}, up to {@code timeout}.
     */
    public OrderStatusProjection.OrderStatusView placeAndShipOneOrder(Duration timeout) throws InterruptedException {
        String orderId = "order-" + UUID.randomUUID();
        eventStore.write(orderId, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget")));
        eventStore.write(orderId, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderId)));

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        OrderStatusProjection.OrderStatusView view;
        while (!"SHIPPED".equals((view = orderStatusViews.get(orderId)) == null ? null : view.status())) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Order " + orderId + " did not reach SHIPPED within " + timeout);
            }
            Thread.sleep(100);
        }
        return view;
    }

    private static void declareExchange(Connection rabbitConnection) {
        try {
            Channel channel = rabbitConnection.createChannel();
            try {
                channel.exchangeDeclare(EXCHANGE, "topic", true);
            } finally {
                channel.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to declare exchange \"" + EXCHANGE + "\"", e);
        }
    }

    @Override
    public void close() {
        try {
            bridge.close();
            forwarderSubscription.shutdown();
            sink.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close " + RabbitMqDomainEventLevelBootstrap.class.getSimpleName(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        String mongoUri = System.getProperty("mongoUri", "mongodb://localhost:27017/?replicaSet=rs0");
        String rabbitUri = System.getProperty("rabbitUri", "amqp://localhost:5672");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitUri);
        Connection rabbitConnection = connectionFactory.newConnection();
        try (RabbitMqDomainEventLevelBootstrap app = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(30));
            log.info("Order placed, forwarded to RabbitMQ, bridged back, and projected: {}", view);
        } finally {
            rabbitConnection.close();
            mongoClient.close();
        }
    }
}
