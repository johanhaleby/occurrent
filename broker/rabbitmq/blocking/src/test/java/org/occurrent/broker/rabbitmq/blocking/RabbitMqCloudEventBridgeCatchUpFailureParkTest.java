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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A permanently failed catch-up must stop {@link RabbitMqCloudEventBridge} rather than route every later message
 * through {@link DeliveryFailurePolicy}. Before the fix this proves, a later message under
 * {@link DeliveryFailurePolicy#PARK} was republished to the parking destination and the original acknowledged,
 * silently discarding messages arriving after the failure instead of leaving them on the source queue.
 * <p>
 * Forces a catch-up failure (a fold that throws for the one historical event), confirms the failure propagated,
 * then publishes two live messages and asserts they stay on the source queue, negatively acknowledged, with the
 * parking queue empty.
 */
class RabbitMqCloudEventBridgeCatchUpFailureParkTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void messages_published_after_a_permanent_catch_up_failure_are_not_parked_and_acked_under_PARK() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        String parkingQueue = "parking-queue-" + UUID.randomUUID();
        String parkingExchange = "parking-exchange-" + UUID.randomUUID();
        adminChannel.exchangeDeclare(parkingExchange, "topic", false, true, null);
        adminChannel.queueDeclare(parkingQueue, false, false, false, null);
        adminChannel.queueBind(parkingQueue, parkingExchange, "#");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        // The replay fold throws for the one historical event, which BlockingHandover.catchUp(..) records as a
        // permanent catch-up failure (catchUpFailure), then rethrows.
        SubscriptionHandle subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated catch-up fold failure for " + ce.getId());
        });

        // Confirms the failure actually happened and propagated, rather than assuming it from timing.
        assertThatThrownBy(() -> subscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the catch-up replay must have failed and propagated the failure")
                .hasMessageContaining("simulated catch-up fold failure");
        assertThat(model.isReadyForLiveDelivery("proj")).as("a failed catch-up is never ready for live delivery").isFalse();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(parkingExchange, "parked"))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            publish(OrderPlaced.class.getName(), "id-2");

            // Long enough for several poll intervals and several refuse-and-redeliver round trips, so this is a
            // genuine steady-state assertion, not a race against the bridge's own pace.
            Thread.sleep(1000);

            assertThat(queueMessageCount(parkingQueue))
                    .as("a message that arrived after a permanently failed catch-up must never be parked")
                    .isZero();
            assertThat(queueMessageCount(queue))
                    .as("both messages must stay on the source queue, unacknowledged, rather than being silently dropped")
                    .isEqualTo(2);
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
