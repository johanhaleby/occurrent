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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The RabbitMQ half of {@code KafkaCloudEventBridgePauseDuringDeliveryTest}: {@code RoutingOutcome.NOT_DELIVERABLE}
 * covers both "the filter threw" and "the model is stopped / the subscription is paused / nothing registered"
 * alike, and only the first is a genuine failure. Because this bridge's coarse poll only cancels its consumer up
 * to one {@code pollInterval} later, a {@code pauseSubscription(id)} called from inside a message handler while
 * more messages are already queued behind it (prefetchCount == 1, so they arrive one at a time) can still hand the
 * bridge a lifecycle {@code NOT_DELIVERABLE} for a message already in flight. Before the fix this proves, every
 * such message got parked and acknowledged under {@link DeliveryFailurePolicy#PARK}, even though nothing about it
 * was broken, only paced.
 * <p>
 * Publishes four messages, pauses the subscription from the first message's own handler while a long
 * {@code pollInterval} is in effect, and asserts none of the remaining three ever reach the parking queue, staying
 * on the source queue instead.
 */
class RabbitMqCloudEventBridgePauseDuringDeliveryTest extends RabbitMqTestSupport {

    @Test
    void pausing_a_subscription_from_inside_a_handler_does_not_park_and_acknowledge_messages_already_queued_behind_it() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        String parkingQueue = "parking-queue-" + UUID.randomUUID();
        String parkingExchange = "parking-exchange-" + UUID.randomUUID();
        adminChannel.exchangeDeclare(parkingExchange, "topic", false, true, null);
        adminChannel.queueDeclare(parkingQueue, false, false, false, null);
        adminChannel.queueBind(parkingQueue, parkingExchange, "#");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", ce -> {
            handled.add(ce.getId());
            if (ce.getId().equals("id-1")) {
                // Paused from inside the very handler that is processing the first message. The bridge's coarse
                // poll (pollInterval below) will not notice this for a long time, so the remaining, already-queued
                // messages are what this test is about.
                model.pauseSubscription("sub");
            }
        });

        // Long enough that the coarse poll cannot possibly cancel the consumer, or release anything, during this
        // test's own window, so any parking or acknowledging of messages 2-4 is exclusively the paused-delivery
        // path, never the poll noticing the pause and cancelling consumption.
        Duration pollInterval = Duration.ofSeconds(30);

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(pollInterval)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(parkingExchange, "parked"))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            publish(OrderPlaced.class.getName(), "id-2");
            publish(OrderPlaced.class.getName(), "id-3");
            publish(OrderPlaced.class.getName(), "id-4");

            // Long enough for the bridge to have fetched and attempted every message prefetchCount == 1 would
            // allow it to, well before the 30-second pollInterval could ever intervene.
            Thread.sleep(2000);

            assertThat(handled).as("the first message is delivered before the pause takes effect").contains("id-1");
            assertThat(queueMessageCount(parkingQueue))
                    .as("a message paced behind a mid-delivery pause must never be parked, since nothing about it is broken")
                    .isZero();
            assertThat(queueMessageCount(queue))
                    .as("every message behind the pause must stay on the source queue rather than being silently "
                            + "acknowledged into the parking queue")
                    .isEqualTo(3);
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
