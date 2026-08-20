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
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
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
 * A runnable, hand-wired bootstrap for the CloudEvent-level half of this broker example. It builds an event store,
 * a {@code CloudEventForwarder} publishing to RabbitMQ, a {@code RabbitMqCloudEventBridge} bridging the queue back
 * into a {@code PushSubscriptionModel}, and an {@link OrderStatusProjection} fed live through
 * {@link ProjectionRunner}, entirely by constructing each piece itself. There is no Spring Boot
 * auto-configuration and no {@code application.yaml} RabbitMQ configuration here. That arrives with the broker
 * Spring Boot starter in <a href="https://github.com/johanhaleby/occurrent/issues/846">#846</a>, and this class
 * wires the same interfaces that starter will eventually configure.
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
 * <p>
 * {@code rs.initiate()} with no member list advertises the container's own hostname as the replica set member
 * address, not {@code localhost}, and a driver doing ordinary replica set discovery switches to that unreachable
 * hostname the moment it reads the config back. The default {@code mongoUri} therefore carries
 * {@code directConnection=true}, which keeps the driver on the one address it was given instead of switching, the
 * same fix a single-node dev replica set needs everywhere this pattern shows up. Verified end to end against the
 * commands above from a clean container, with and without that parameter. The driver fails with an
 * {@code UnknownHostException} on the container's own hostname without it, and this class runs the whole loop to
 * {@code SHIPPED} with it. Override {@code mongoUri} only with another URI that also either sets
 * {@code directConnection=true} or advertises a host-reachable member address.
 * <p>
 * The default {@code rabbitUri} carries no credentials, so the client falls back to RabbitMQ's default
 * {@code guest} account. Verified on macOS with Colima, where the docker command above accepts that account over the
 * published port with no extra step, which is what {@link #main(String[])} was actually run against. RabbitMQ
 * restricts {@code guest} to connections it can recognise as loopback, and on native Linux Docker a host
 * connection through a published port can arrive at the container from the Docker bridge address rather than one
 * RabbitMQ recognises as loopback, which fails the login with {@code ACCESS_REFUSED}. This was not verified on
 * that platform. If it bites, start the container with {@code -e RABBITMQ_DEFAULT_USER=broker-example -e
 * RABBITMQ_DEFAULT_PASS=broker-example} instead, a named user is never subject to the loopback restriction, and
 * carry the same credentials in {@code rabbitUri}, for example
 * {@code amqp://broker-example:broker-example@localhost:5672}.
 * <p>
 * The catch-up completion marker is in-memory, paired with the in-memory read model rather than with the durable
 * event store. Losing both together on a real restart is honest. A fresh run replays the store from the start and
 * rebuilds the read model to match. Pairing a durable marker with an in-memory read model instead would have the
 * marker survive a restart the read model does not, so a later catch-up skips a replay the read model never
 * actually received, and every order projected before the restart stays permanently missing. A production
 * deployment persists both the marker and the read model, in stores with the same lifetime, never just one of
 * the two.
 *
 * @see RabbitMqDomainEventLevelBootstrap the domain-level half of the same example
 */
public final class RabbitMqCloudEventLevelBootstrap implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqCloudEventLevelBootstrap.class);

    private static final String DATABASE_NAME = "occurrent_broker_example";
    private static final String EVENTS_COLLECTION = "events";
    private static final String EXCHANGE = "broker-example";
    private static final String QUEUE = "broker-example-cloudevent";
    private static final String SUBSCRIPTION_ID = "broker-example-cloudevent-projection";

    private final MongoEventStore eventStore;
    private final CloudEventConverter<OrderEvent> converter;
    private final DurableSubscriptionModel forwarderSubscription;
    private final RabbitMqCloudEventSink sink;
    private final PushSubscriptionModel pushModel;
    private final CatchupThenPushSubscriptionModel catchupThenPush;
    private final RabbitMqCloudEventBridge bridge;
    private final Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews;

    private RabbitMqCloudEventLevelBootstrap(MongoEventStore eventStore, CloudEventConverter<OrderEvent> converter,
                                              DurableSubscriptionModel forwarderSubscription, RabbitMqCloudEventSink sink,
                                              PushSubscriptionModel pushModel, CatchupThenPushSubscriptionModel catchupThenPush,
                                              RabbitMqCloudEventBridge bridge, Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews) {
        this.eventStore = eventStore;
        this.converter = converter;
        this.forwarderSubscription = forwarderSubscription;
        this.sink = sink;
        this.pushModel = pushModel;
        this.catchupThenPush = catchupThenPush;
        this.bridge = bridge;
        this.orderStatusViews = orderStatusViews;
    }

    /**
     * Wires the whole loop against an already-open {@link MongoClient} and {@link Connection}, so {@link #main}
     * and a test can share the exact same production wiring against different infrastructure.
     */
    public static RabbitMqCloudEventLevelBootstrap start(MongoClient mongoClient, Connection rabbitConnection) {
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

        // From here on, forwarder.forward(...) and the bridge builder each start a background subscription the
        // instant they succeed, before this method has anywhere to return a handle to. A later step throwing
        // (the bridge, the projection's catch-up) would otherwise leak whatever already started. The catch below
        // closes exactly that, in reverse order, before the failure reaches the caller.
        DurableSubscriptionModel forwarderSubscription = null;
        RabbitMqCloudEventSink sink = null;
        CatchupThenPushSubscriptionModel catchupThenPush = null;
        RabbitMqCloudEventBridge bridge = null;
        try {
            NativeMongoSubscriptionModel forwarderSubscriptionModel = new NativeMongoSubscriptionModel(
                    mongoClient.getDatabase(DATABASE_NAME), EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING, Executors.newVirtualThreadPerTaskExecutor());
            CheckpointStorage forwarderCheckpoints = new NativeMongoCheckpointStorage(mongoClient.getDatabase(DATABASE_NAME), "forwarder-checkpoints");
            forwarderSubscription = new DurableSubscriptionModel(forwarderSubscriptionModel, forwarderCheckpoints);
            sink = RabbitMqCloudEventSink.builder(rabbitConnection, resolver).build();
            CloudEventForwarder forwarder = new CloudEventForwarder(forwarderSubscription, sink);
            forwarder.forward(SUBSCRIPTION_ID + "-forwarder");

            RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
            PushSubscriptionModel pushModel = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
            // In-memory, paired with the in-memory orderStatusViews below, not the durable event store. Both are
            // lost together on a real restart, so a fresh run genuinely replays rather than a durable marker
            // skipping a replay the read model never actually got. See the class javadoc.
            CheckpointStorage catchupMarker = new InMemoryCheckpointStorage();
            catchupThenPush = new CatchupThenPushSubscriptionModel(eventStore, pushModel, catchupMarker);

            bridge = RabbitMqCloudEventBridge.builder(rabbitConnection, pushModel, outcomeChannel, QUEUE)
                    .resolver(resolver)
                    .build();

            Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews = new ConcurrentHashMap<>();
            ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(orderStatusViews::get, orderStatusViews::put);
            ProjectionRunner.stream(catchupThenPush, converter).project(SUBSCRIPTION_ID, OrderStatusProjection.orderStatusProjection(), repository);

            return new RabbitMqCloudEventLevelBootstrap(eventStore, converter, forwarderSubscription, sink, pushModel, catchupThenPush, bridge, orderStatusViews);
        } catch (RuntimeException e) {
            closeQuietly(bridge, e);
            closeQuietly(catchupThenPush, e);
            closeQuietly(forwarderSubscription, e);
            closeQuietly(sink, e);
            throw e;
        }
    }

    // Closes whatever of start()'s partially-built pieces is non-null, folding a failure closing it into the
    // original exception instead of replacing it, since the original is the one the caller asked about.
    private static void closeQuietly(@Nullable AutoCloseable closeable, RuntimeException original) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }

    private static void closeQuietly(@Nullable SubscriptionModel subscription, RuntimeException original) {
        if (subscription == null) {
            return;
        }
        try {
            subscription.shutdown();
        } catch (RuntimeException closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }

    /**
     * The read model this bootstrap keeps up to date, live as events travel the whole loop.
     */
    public Map<String, OrderStatusProjection.OrderStatusView> orderStatusViews() {
        return orderStatusViews;
    }

    /** Package-private, for a test to confirm {@link #close()} actually shuts this down rather than leaking it. */
    PushSubscriptionModel pushModel() {
        return pushModel;
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

    /**
     * Closes the bridge first, so nothing is consuming, then the catch-up-then-live model {@code catchupThenPush}
     * wraps (which shuts {@link #pushModel} down too, cascading), then the forwarder and the sink. A
     * {@link RabbitMqCloudEventBridge} deliberately never shuts its {@link PushSubscriptionModel} down itself,
     * since a bridge only ever holds the model, it does not own its lifecycle, so shutting it down here is this
     * bootstrap's own job to do, not something closing the bridge already covers.
     * <p>
     * Each of the four is closed in its own try, so one throwing does not skip the rest. The first failure is
     * rethrown with every later one attached as a suppressed exception, which is what an {@link AutoCloseable}
     * implementation owes its caller, every resource actually gets a close attempt regardless of what an earlier
     * one did.
     */
    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            bridge.close();
        } catch (Exception e) {
            failure = collectFailure(failure, e);
        }
        try {
            catchupThenPush.shutdown();
        } catch (RuntimeException e) {
            failure = collectFailure(failure, e);
        }
        try {
            forwarderSubscription.shutdown();
        } catch (RuntimeException e) {
            failure = collectFailure(failure, e);
        }
        try {
            sink.close();
        } catch (Exception e) {
            failure = collectFailure(failure, e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    // Wraps a checked close() failure as unchecked if it is not already, folds a second-or-later failure into the
    // first as a suppressed exception instead of discarding it, and returns whichever exception the caller should
    // eventually throw.
    private static RuntimeException collectFailure(@Nullable RuntimeException first, Exception e) {
        RuntimeException wrapped = e instanceof RuntimeException re
                ? re
                : new RuntimeException("Failed to close " + RabbitMqCloudEventLevelBootstrap.class.getSimpleName(), e);
        if (first == null) {
            return wrapped;
        }
        first.addSuppressed(wrapped);
        return first;
    }

    public static void main(String[] args) throws Exception {
        String mongoUri = System.getProperty("mongoUri", "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true");
        String rabbitUri = System.getProperty("rabbitUri", "amqp://localhost:5672");

        // All three resources in one try-with-resources, not a hand-rolled finally, so the language's own
        // suppression rules apply. Closing in reverse declaration order means app closes before either client
        // still underneath it does, and a failure closing a client becomes a suppressed exception on whatever the
        // try body itself threw instead of replacing it. A hand-rolled finally that itself throws has no such
        // rule, it silently discards the body's own exception.
        try (MongoClient mongoClient = MongoClients.create(mongoUri);
             Connection rabbitConnection = newRabbitConnection(rabbitUri);
             RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(30));
            log.info("Order placed, forwarded to RabbitMQ, bridged back, and projected: {}", view);
        }
    }

    private static Connection newRabbitConnection(String rabbitUri) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitUri);
        return connectionFactory.newConnection();
    }
}
