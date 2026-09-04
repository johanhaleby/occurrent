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
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
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
        }
        assertAcknowledged(queue);
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

            // FILTERED never reaches a handler, so there is no positive signal to await directly. The ready count
            // dropping to zero reliably means the broker handed the delivery to the bridge's consumer, whether or
            // not it goes on to acknowledge it, so this only waits for the delivery to have happened. The actual
            // acknowledgement proof is assertAcknowledged(...) below, checked only once the bridge has closed.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
            assertThat(handled).isEmpty();
        }
        assertAcknowledged(queue);
    }

    /**
     * The silent-loss case ADR 133's amendment on {@code RabbitMqCloudEventMapper} exists to prevent. A
     * {@code streamversion} extension used to come back off the wire as the String {@code "3"}, so
     * {@code Filter.streamVersion(eq(3))}, whose operand is a {@code Long}, never matched it. The bridge then
     * acknowledged the FILTERED delivery, discarding an event the filter should have accepted.
     */
    @Test
    void a_numeric_extension_survives_the_broker_round_trip_so_a_numeric_filter_still_matches_it() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.streamVersion(Condition.eq(3L))), cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(OrderPlaced.class.getName())
                    .withExtension("streamid", "stream-1")
                    .withExtension("streamversion", 3L)
                    .build();
            BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
            adminChannel.basicPublish(exchange, OrderPlaced.class.getName(), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
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
            // confirm the message is still sitting on the queue, untouched. Sound to check while the bridge is
            // still open, unlike an acknowledgement claim. Nothing has been delivered to any consumer yet, so the
            // ready count is not the ambiguous one an outstanding-but-unacked delivery would produce.
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
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
        }
        assertAcknowledged(queue);
        assertThat(appliedIds).containsExactly("id-1");
    }

    /**
     * {@code PushSubscriptionModel.routeReportingMatch} propagates an {@link AssertionError} from a filter, and a
     * handler can throw one too, so the bridge has to redeliver on it exactly as it does on a {@link RuntimeException},
     * rather than leaving it uncaught and stalling the consumer at prefetch one.
     */
    @Test
    void a_handler_that_throws_an_assertionError_is_redelivered_rather_than_stalling_the_consumer() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        AtomicInteger attempts = new AtomicInteger();
        model.subscribe("sub", cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("simulated assertion failure");
            }
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
        }
        assertAcknowledged(queue);
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

            // The parking queue has no consumer attached, so its own ready count is not the ambiguous one a
            // delivery outstanding on the bridge's consumer would produce; sound to check while the bridge is open.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
        }
        assertAcknowledged(queue);
    }

    /**
     * A message with no {@code cloudEvents_} headers at all cannot become a {@link CloudEvent}, since
     * {@link CloudEventBuilder#build()} requires an id, a source and a type. {@link DeliveryFailurePolicy#PARK}
     * still applies to it rather than always redelivering. The bridge parks the delivery's own raw properties and
     * body unchanged, the same way it parks a decodable message too, since parking never depends on a CloudEvent
     * existing to rebuild one from.
     */
    @Test
    void an_undecodable_message_is_parked_with_its_raw_bytes_and_the_original_is_acknowledged_once_confirmed() throws Exception {
        String queue = declareAndBindQueue("malformed");
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        RabbitMqDestination parkingDestination = RabbitMqDestination.of(exchange, "parked");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            BasicProperties malformedProperties = new BasicProperties.Builder().contentType("text/plain").build();
            byte[] malformedBody = "not a cloud event".getBytes(StandardCharsets.UTF_8);
            adminChannel.basicPublish(exchange, "malformed", malformedProperties, malformedBody);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo(malformedBody);
            assertThat(parked.getProps().getContentType()).isEqualTo("text/plain");
        }
        assertAcknowledged(queue);
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

    /**
     * The plain {@link Filter} overload of {@code bindingFilter}, which wraps into an
     * {@link org.occurrent.subscription.AgnosticSubscriptionFilter} and delegates. The queue binds only the routing
     * key that filter narrows to, so an event of another type published first never reaches the handler.
     */
    @Test
    void a_plain_filter_bindingFilter_binds_only_the_routing_keys_that_filter_narrows_to() throws Exception {
        String queue = "bridge-plain-filter-" + UUID.randomUUID();
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .resolver(resolver)
                .bindingFilter(Filter.type(OrderPlaced.class.getName()))
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(SomethingElse.class.getName(), "id-something-else");
            publish(OrderPlaced.class.getName(), "id-placed");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(handled).extracting(CloudEvent::getId).containsExactly("id-placed"));
        }
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        // Not auto-delete. The default no-arg queueDeclare() is exclusive and auto-delete, so the queue itself
        // would vanish the instant the bridge's consumer disconnects, which is exactly what assertAcknowledged(..)
        // needs to survive to tell an acknowledged message apart from one merely delivered and never acknowledged.
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

    /**
     * Asserts {@code queue} is empty, meaningfully only once the bridge that consumed it has already been closed
     * (a try-with-resources block exiting, typically). RabbitMQ's ready count excludes a delivery still outstanding
     * on a consumer, so checking it while the bridge is still open cannot tell an acknowledged message apart from
     * one merely delivered and never acknowledged, both read zero the instant the bridge receives it. Closing the
     * bridge first requeues whatever it never acknowledged, so a message still gone afterwards is proof of an
     * actual acknowledgement rather than an artifact of the consumer still holding it.
     */
    private void assertAcknowledged(String queue) throws Exception {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private static final class OrderPlaced {
    }

    private static final class SomethingElse {
    }
}
