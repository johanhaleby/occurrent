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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A Copilot review finding: {@code BlockingHandover.PreDispatchRefusalException} escaping {@code handleDelivery} is
 * not proof that this bridge's own model has a permanently failed catch-up. A handler this bridge's own model
 * genuinely dispatched to can call an unrelated {@link CatchupThenPushSubscriptionModel} whose own catch-up has
 * already failed, and that exception type escapes the handler unwrapped too, indistinguishable by type alone from
 * this model's own refusal. Before the fix this proves, the bridge stopped itself permanently for a healthy model,
 * over an ordinary handler failure that happened to touch a broken model elsewhere.
 * <p>
 * Builds two independent models. {@code otherWrapper}'s catch-up is forced to fail permanently, the same way
 * {@code RabbitMqCloudEventBridgeCatchUpFailureParkTest} does. The bridge under test is built on {@code liveFeed}, a
 * plain, healthy {@link PushSubscriptionModel} with no catch-up wrapper of its own, whose one handler calls
 * {@code otherWrapper}'s own live feed for {@code id-1} only, and lets the resulting {@code PreDispatchRefusalException}
 * propagate unhandled, exactly the shape a handler that fans out to a second projection or saga would take.
 * {@link DeliveryFailurePolicy#PARK} is configured so that failure resolves {@code id-1} on its first attempt
 * rather than redelivering it forever, since the nested call fails identically on every retry and this test cares
 * about {@code id-2}, not about {@code id-1}'s own resolution. Publishes {@code id-1}, confirms it reached the
 * handler and was parked rather than the bridge stopping itself, then publishes {@code id-2}, an ordinary event
 * with no nested call, and asserts it still reaches the handler, since a permanently and incorrectly stopped bridge
 * would never have gotten to it.
 */
class RabbitMqCloudEventBridgeNestedRefusalTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void a_nested_handovers_permanent_refusal_escaping_a_handler_does_not_stop_this_bridges_own_healthy_model() throws Exception {
        // otherWrapper: its own catch-up fold throws, so its inner live feed's acceptRedeliverable(...) throws
        // PreDispatchRefusalException on every later call, unrelated to the bridge under test here.
        PushSubscriptionModel otherLiveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), new RoutingOutcomeChannel());
        InMemoryEventStore otherStore = new InMemoryEventStore();
        otherStore.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel otherWrapper = new CatchupThenPushSubscriptionModel(otherStore, otherLiveFeed, null);
        SubscriptionHandle otherSubscription = otherWrapper.subscribe("other", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated permanent catch-up failure for the unrelated model");
        });
        assertThatThrownBy(() -> otherSubscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("otherWrapper's own catch-up must have actually failed and propagated")
                .hasMessageContaining("simulated permanent catch-up failure");

        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        String parkingQueue = "parking-queue-" + UUID.randomUUID();
        String parkingExchange = "parking-exchange-" + UUID.randomUUID();
        adminChannel.exchangeDeclare(parkingExchange, "topic", false, true, null);
        adminChannel.queueDeclare(parkingQueue, false, false, false, null);
        adminChannel.queueBind(parkingQueue, parkingExchange, "#");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        liveFeed.subscribe("proj", ce -> {
            handled.add(ce.getId());
            if (ce.getId().equals("id-1")) {
                // The nested, unrelated refusal, escaping this handler unwrapped, the same as a handler that fans
                // an event out to a second projection or saga would let it.
                otherLiveFeed.acceptRedeliverable(cloudEvent("id-1-fanout", OrderPlaced.class.getName()));
            }
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(parkingExchange, "parked"))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).contains("id-1"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(queueMessageCount(parkingQueue))
                            .as("id-1's nested, unrelated refusal is an ordinary handler failure here, parked under "
                                    + "PARK rather than left for a permanent stop that never fires")
                            .isEqualTo(1));

            publish(OrderPlaced.class.getName(), "id-2");

            // A bridge incorrectly, permanently stopped by the nested refusal above would never reach id-2 at all.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).contains("id-2"));
        }
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .withExtension("streamid", "s1")
                .build();
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, routingKey);
        return queue;
    }

    private void publish(String type, String id) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(type)
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, type, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    private static final class OrderPlaced {
    }
}
