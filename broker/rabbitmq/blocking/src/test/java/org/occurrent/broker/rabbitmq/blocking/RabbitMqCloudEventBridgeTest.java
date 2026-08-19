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
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RabbitMqCloudEventBridgeTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void acks_on_delivered_and_the_message_does_not_stay_on_the_queue() throws Exception {
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
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
    }

    @Test
    void a_filtered_event_is_acknowledged_without_invoking_the_handler() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        // Registered for a different type, so the one event published below is FILTERED, not DELIVERED.
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type(SomethingElse.class.getName())), cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
            assertThat(handled).isEmpty();
        }
    }

    @Test
    void the_bridge_does_not_consume_before_a_subscription_is_registered_then_starts_once_one_is() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            // No subscription registered yet: give the coarse poll several chances to (wrongly) consume, then
            // confirm the message is still sitting on the queue, untouched.
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
    }

    /**
     * The redelivery-no-double-process proof #415 asks for. The handler applies its effect (recording the event id
     * in an idempotent set) and only then throws on its first attempt, simulating a failure that happens after the
     * effect but before the bridge could acknowledge it. The bridge nacks with requeue, RabbitMQ redelivers the same
     * message, and the second attempt's idempotent {@code add} to the same set is a no-op, proving the fold applied
     * the event exactly once despite being handed it twice.
     */
    @Test
    void a_handler_that_fails_once_is_redelivered_and_an_idempotent_fold_applies_the_event_exactly_once() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        AtomicInteger attempts = new AtomicInteger();
        Set<String> appliedIds = ConcurrentHashMap.newKeySet();
        model.subscribe("sub", cloudEvent -> {
            int attempt = attempts.incrementAndGet();
            appliedIds.add(cloudEvent.getId());
            if (attempt == 1) {
                throw new RuntimeException("simulated failure after the effect was already applied");
            }
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
            assertThat(appliedIds).containsExactly("id-1");
        }
    }

    @Test
    void parks_a_delivery_that_keeps_failing_and_acknowledges_the_original_once_the_park_is_confirmed() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        RabbitMqDestination parkingDestination = RabbitMqDestination.of(exchange, "parked");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
            throw new RuntimeException("always fails");
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
        }
    }

    @Test
    void declares_the_queue_and_binds_it_from_the_resolvers_catchAllDestination_when_declareTopology_is_true() throws Exception {
        String queue = "bridge-declared-" + UUID.randomUUID();
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .resolver(resolver)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
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

    private static final class SomethingElse {
    }
}
