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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link RabbitMqCloudEventBridge.Builder#readinessSource(java.util.function.Predicate)} is the gate that closes
 * the catch-up acknowledgement hole, proven here against a real broker rather than against
 * {@code CatchupThenPushSubscriptionModel} itself, which {@code CatchupThenPushSubscriptionModelTest} already
 * covers for {@code isReadyForLiveDelivery(String)} in isolation. A {@code readinessSource} answering {@code false}
 * must keep this bridge from ever pulling a matching message off the queue at all, not merely from acknowledging
 * one it already pulled, since a message that reaches the handler here would already have been reported
 * {@code DELIVERED} and acknowledged before this test could observe anything wrong.
 */
class RabbitMqCloudEventBridgeReadinessTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void a_readiness_source_answering_false_keeps_a_matching_message_on_the_queue_and_never_invokes_the_handler() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .readinessSource(subscriptionId -> false)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            // Long enough for several poll intervals to pass, so this is a genuine "never consumed" assertion, not
            // a race against the bridge's own poll cadence.
            Thread.sleep(500);

            assertThat(handled).isEmpty();
            assertThat(queueMessageCount(queue)).isEqualTo(1);
        }
    }

    @Test
    void flipping_readiness_to_true_drains_and_acknowledges_a_message_that_arrived_while_not_ready() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        AtomicBoolean ready = new AtomicBoolean(false);
        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .readinessSource(subscriptionId -> ready.get())
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            Thread.sleep(200);
            assertThat(handled).isEmpty();

            ready.set(true);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
    }

    @Test
    void no_readiness_source_configured_consumes_immediately_the_same_as_before_this_capability_existed() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
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

    private void assertAcknowledged(String queue) throws Exception {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private static final class OrderPlaced {
    }
}
