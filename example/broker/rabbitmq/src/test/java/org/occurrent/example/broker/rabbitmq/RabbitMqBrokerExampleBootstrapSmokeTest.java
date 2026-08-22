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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Runs both {@link RabbitMqCloudEventLevelBootstrap} and {@link RabbitMqDomainEventLevelBootstrap} exactly as
 * {@code main(String[])} would, against this module's real Testcontainers MongoDB and RabbitMQ rather than the
 * localhost ones an operator supplies, since it exists to catch a broken {@code start(...)} wiring in CI, not to
 * verify infrastructure. Both bootstraps use fixed database, queue and checkpoint names by design, so an operator
 * gets stable names to point tooling at, rather than the scratch names {@link AbstractBrokerExampleTest} generates
 * per method to keep the other tests isolated. A local rerun against reused containers therefore leaves both
 * bootstraps' state behind for the next run, which is harmless here since every order id is still fresh, and does
 * not happen in CI, where each run gets fresh containers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RabbitMqBrokerExampleBootstrapSmokeTest extends AbstractBrokerExampleTest {

    @Test
    void the_cloud_event_level_bootstrap_starts_and_completes_one_order() throws Exception {
        try (RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(20));
            assertThat(view.status()).isEqualTo("SHIPPED");
        }
    }

    @Test
    void the_domain_event_level_bootstrap_starts_and_completes_one_order() throws Exception {
        try (RabbitMqDomainEventLevelBootstrap app = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView view = app.placeAndShipOneOrder(Duration.ofSeconds(20));
            assertThat(view.status()).isEqualTo("SHIPPED");
        }
    }

    /**
     * Proves the in-memory catch-up marker and the in-memory read model actually stay paired. A stray durable
     * marker beside an in-memory read model would have this order caught up once, by the first {@code start(...)},
     * then silently skipped by the second, since the marker would already claim the replay done. Both
     * {@code start(...)} calls share this test's real Mongo, so the order is genuinely still there to replay.
     */
    @Test
    void a_second_cloud_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped() throws Exception {
        OrderStatusProjection.OrderStatusView placedByFirstBoot;
        try (RabbitMqCloudEventLevelBootstrap firstBoot = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            placedByFirstBoot = firstBoot.placeAndShipOneOrder(Duration.ofSeconds(20));
        }

        try (RabbitMqCloudEventLevelBootstrap secondBoot = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView replayed = secondBoot.orderStatusViews().get(placedByFirstBoot.orderId());
            assertThat(replayed).isNotNull();
            assertThat(replayed.status()).isEqualTo("SHIPPED");
        }
    }

    /**
     * The domain-level twin of {@link #a_second_cloud_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped()}.
     */
    @Test
    void a_second_domain_event_level_bootstrap_replays_an_order_the_first_one_placed_and_shipped() throws Exception {
        OrderStatusProjection.OrderStatusView placedByFirstBoot;
        try (RabbitMqDomainEventLevelBootstrap firstBoot = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            placedByFirstBoot = firstBoot.placeAndShipOneOrder(Duration.ofSeconds(20));
        }

        try (RabbitMqDomainEventLevelBootstrap secondBoot = RabbitMqDomainEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            OrderStatusProjection.OrderStatusView replayed = secondBoot.orderStatusViews().get(placedByFirstBoot.orderId());
            assertThat(replayed).isNotNull();
            assertThat(replayed.status()).isEqualTo("SHIPPED");
        }
    }

    /**
     * Proves that a failure partway through {@code start(...)} does not leak the forwarder subscription that
     * already started before it. The sink built in the same window is not asserted here, neither bootstrap exposes
     * an accessor for it, and {@code RabbitMqCloudEventSink} itself has no closed-state to probe from outside. A
     * leaked sink would show up as an open channel on the connection, not as anything this probe can observe.
     * Failing the bridge's own channel open,
     * the second {@code openChannel()} call on the connection and every one after it (the first is the sink's),
     * forces exactly that partial-construction failure without touching the bridge's own topology. Every call
     * after the first has to fail, not just the second, since {@code RabbitMqCloudEventBridge.Builder#build()}
     * retries a failed channel open by default (#867), so a proxy that only failed the second call would let a
     * later retry attempt succeed instead of ever reaching the failure this test exists to force. If the forwarder
     * subscription leaked, it would still be watching the store and would forward a fresh order to the exchange,
     * so a queue bound directly to it, independent of the bridge that never got to declare its own, proves it did
     * not.
     */
    @Test
    void a_failure_partway_through_starting_the_cloud_event_level_bootstrap_closes_what_already_started() throws Exception {
        Connection failingOnSecondChannel = failOnSecondOpenChannel(rabbitConnection);

        assertThatThrownBy(() -> RabbitMqCloudEventLevelBootstrap.start(mongoClient, failingOnSecondChannel))
                .isInstanceOf(RabbitMqBridgeException.class);

        assertNothingArrivesFromALeakedForwarder();
    }

    /**
     * The domain-level twin of {@link #a_failure_partway_through_starting_the_cloud_event_level_bootstrap_closes_what_already_started()}.
     */
    @Test
    void a_failure_partway_through_starting_the_domain_event_level_bootstrap_closes_what_already_started() throws Exception {
        Connection failingOnSecondChannel = failOnSecondOpenChannel(rabbitConnection);

        assertThatThrownBy(() -> RabbitMqDomainEventLevelBootstrap.start(mongoClient, failingOnSecondChannel))
                .isInstanceOf(RabbitMqBridgeException.class);

        assertNothingArrivesFromALeakedForwarder();
    }

    /** A {@link Connection} that delegates everything to {@code real}, except its second no-argument
     * {@code openChannel()} call and every one after it, which it fails instead, simulating the bridge's own
     * channel open failing while leaving the sink's earlier one, and everything built from it, already running.
     * Fails permanently rather than once, so the bridge's own default retry (#867) still exhausts into the same
     * failure this test forces, instead of succeeding on a later attempt. */
    private static Connection failOnSecondOpenChannel(Connection real) {
        AtomicInteger openChannelCalls = new AtomicInteger();
        InvocationHandler handler = (proxy, method, args) -> {
            if ("openChannel".equals(method.getName()) && method.getParameterCount() == 0
                    && openChannelCalls.incrementAndGet() >= 2) {
                return Optional.empty();
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, handler);
    }

    /**
     * Binds a fresh queue directly to the bootstraps' own {@link RabbitMqCloudEventLevelBootstrap#EXCHANGE}
     * (both bootstraps share the identical exchange, database and collection names by design), writes a fresh
     * order straight into {@link RabbitMqCloudEventLevelBootstrap#DATABASE_NAME}, the same database a leaked
     * forwarder subscription would still be watching, and asserts nothing arrives. A live forwarder would publish
     * it within milliseconds of the write, well inside the wait below. Referencing the bootstraps' own constants
     * rather than repeating the literals here means a renamed exchange or database cannot leave this probe
     * silently watching the wrong one while a real leak goes undetected.
     * {@link #the_leak_probe_itself_sees_a_forwarder_that_is_actually_still_running()} is this method's own
     * positive control, proving the probe mechanism catches a forwarder that genuinely is running before trusting
     * it to prove the negative everywhere else in this class.
     */
    private void assertNothingArrivesFromALeakedForwarder() throws Exception {
        String probeQueue = "leak-probe-" + UUID.randomUUID();
        adminChannel.queueDeclare(probeQueue, false, false, true, null);
        adminChannel.queueBind(probeQueue, RabbitMqCloudEventLevelBootstrap.EXCHANGE, "#");
        try {
            CloudEventConverter<OrderEvent> converter = newConverter();
            MongoEventStore eventStore = new MongoEventStore(mongoClient, RabbitMqCloudEventLevelBootstrap.DATABASE_NAME,
                    RabbitMqCloudEventLevelBootstrap.EVENTS_COLLECTION, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
            String orderId = "order-" + UUID.randomUUID();
            eventStore.write(orderId, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget")));

            Thread.sleep(2000);
            assertThat(adminChannel.queueDeclarePassive(probeQueue).getMessageCount()).isZero();
        } finally {
            adminChannel.queueDelete(probeQueue);
        }
    }

    /**
     * The positive control {@link #assertNothingArrivesFromALeakedForwarder()}'s own javadoc promises: the exact
     * same probe mechanism, run while a bootstrap's forwarder genuinely is still running rather than after a
     * simulated partial-construction failure, must see the order arrive. Without this, a probe that silently never
     * saw anything, a wrong exchange name after a rename, for example, would make every "did not leak" assertion
     * above pass vacuously.
     */
    @Test
    void the_leak_probe_itself_sees_a_forwarder_that_is_actually_still_running() throws Exception {
        try (RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            String probeQueue = "leak-probe-control-" + UUID.randomUUID();
            adminChannel.queueDeclare(probeQueue, false, false, true, null);
            adminChannel.queueBind(probeQueue, RabbitMqCloudEventLevelBootstrap.EXCHANGE, "#");
            try {
                CloudEventConverter<OrderEvent> converter = newConverter();
                MongoEventStore eventStore = new MongoEventStore(mongoClient, RabbitMqCloudEventLevelBootstrap.DATABASE_NAME,
                        RabbitMqCloudEventLevelBootstrap.EVENTS_COLLECTION, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
                String orderId = "order-" + UUID.randomUUID();
                eventStore.write(orderId, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget")));

                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(adminChannel.queueDeclarePassive(probeQueue).getMessageCount()).isEqualTo(1));
            } finally {
                adminChannel.queueDelete(probeQueue);
            }
        }
    }

    /**
     * {@code close()} must shut the push model down itself. {@link RabbitMqCloudEventBridge} only ever holds the
     * model it was given, it never shuts it down, on the ADR 133 grounds that a bridge does not own the model's
     * lifecycle, so a bootstrap that only closed the bridge would leak the model on every successful run, not just
     * a partial-construction failure. {@code hasSubscriptions()} is the direct, documented effect of
     * {@code RegisteringSubscribable.shutdown()}, which clears every registration, so it is {@code false} only if
     * the model was actually shut down rather than merely abandoned mid-registration.
     */
    @Test
    void closing_the_cloud_event_level_bootstrap_shuts_the_push_model_down_too() throws Exception {
        PushSubscriptionModel pushModel;
        try (RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, rabbitConnection)) {
            app.placeAndShipOneOrder(Duration.ofSeconds(20));
            pushModel = app.pushModel();
            assertThat(pushModel.hasSubscriptions()).isTrue();
        }
        assertThat(pushModel.hasSubscriptions()).isFalse();
    }

    /**
     * Proves {@code close()} still shuts the catch-up model and the forwarder down when the bridge's own channel
     * fails to close, rather than the bridge's failure aborting the rest. The channel's {@code close()} is made to
     * throw a plain {@link RuntimeException}, not one of the checked or {@link
     * com.rabbitmq.client.ShutdownSignalException} failures {@link RabbitMqCloudEventBridge#close()} already treats
     * as best effort, so the failure actually reaches the bootstrap uncaught instead of being swallowed before it
     * gets there. {@code hasSubscriptions()} false afterward proves the catch-up model still ran, and nothing
     * arriving from the probe proves the forwarder subscription still ran too, both despite the bridge going first
     * and throwing.
     */
    @Test
    void a_failure_closing_the_cloud_event_level_bootstraps_bridge_channel_still_closes_the_rest() throws Exception {
        Connection failingChannelClose = failCloseOnSecondOpenChannel(rabbitConnection);
        RabbitMqCloudEventLevelBootstrap app = RabbitMqCloudEventLevelBootstrap.start(mongoClient, failingChannelClose);
        app.placeAndShipOneOrder(Duration.ofSeconds(20));
        PushSubscriptionModel pushModel = app.pushModel();

        assertThatThrownBy(app::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated failure closing the bridge's channel");

        assertThat(pushModel.hasSubscriptions()).isFalse();
        assertNothingArrivesFromALeakedForwarder();
    }

    /**
     * The domain-level twin of {@link #a_failure_closing_the_cloud_event_level_bootstraps_bridge_channel_still_closes_the_rest()}.
     * The domain-level bootstrap has no separate push model to probe, so only the forwarder-leak probe applies here.
     */
    @Test
    void a_failure_closing_the_domain_event_level_bootstraps_bridge_channel_still_closes_the_rest() throws Exception {
        Connection failingChannelClose = failCloseOnSecondOpenChannel(rabbitConnection);
        RabbitMqDomainEventLevelBootstrap app = RabbitMqDomainEventLevelBootstrap.start(mongoClient, failingChannelClose);
        app.placeAndShipOneOrder(Duration.ofSeconds(20));

        assertThatThrownBy(app::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated failure closing the bridge's channel");

        assertNothingArrivesFromALeakedForwarder();
    }

    /**
     * Like {@link #failOnSecondOpenChannel(Connection)}, but the bridge's own channel opens normally, the same real
     * channel {@code start(...)} would have gotten, wrapped only to fail its own no-argument {@code close()} call.
     */
    private static Connection failCloseOnSecondOpenChannel(Connection real) {
        AtomicInteger openChannelCalls = new AtomicInteger();
        InvocationHandler handler = (proxy, method, args) -> {
            Object result;
            try {
                result = method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if ("openChannel".equals(method.getName()) && method.getParameterCount() == 0
                    && openChannelCalls.incrementAndGet() == 2
                    && result instanceof Optional<?> maybeChannel && maybeChannel.isPresent()) {
                return Optional.of(failingCloseChannel((Channel) maybeChannel.get()));
            }
            return result;
        };
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, handler);
    }

    /** Delegates everything to {@code real} except the no-argument {@code close()}, which throws instead. */
    private static Channel failingCloseChannel(Channel real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                throw new RuntimeException("Simulated failure closing the bridge's channel");
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, handler);
    }
}
