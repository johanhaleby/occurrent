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

package org.occurrent.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The Kafka half of {@code RabbitMqCloudEventBridgeNestedRefusalTest}: a Copilot review finding.
 * {@code BlockingHandover.PreDispatchRefusalException} escaping {@code handleRecord} is not proof that this
 * bridge's own model has a permanently failed catch-up. A handler this bridge's own model genuinely dispatched to
 * can call an unrelated {@link CatchupThenPushSubscriptionModel} whose own catch-up has already failed, and that
 * exception type escapes the handler unwrapped too, indistinguishable by type alone from this model's own refusal.
 * Before the fix this proves, the bridge stopped its poll loop permanently for a healthy model, over an ordinary
 * handler failure that happened to touch a broken model elsewhere.
 * <p>
 * Builds two independent models. {@code otherWrapper}'s catch-up is forced to fail permanently, the same way
 * {@code KafkaCloudEventBridgeCatchUpFailureParkTest} does. The bridge under test is built on {@code liveFeed}, a
 * plain, healthy {@link PushSubscriptionModel} with no catch-up wrapper of its own, whose handler calls
 * {@code otherWrapper}'s own live feed for {@code id-1} only, and lets the resulting {@code PreDispatchRefusalException}
 * propagate unhandled. {@link DeliveryFailurePolicy#PARK} is configured so that failure resolves {@code id-1} on
 * its first attempt rather than seeking back to it forever. Publishes {@code id-1}, confirms it reached the
 * handler and was parked rather than the bridge stopping itself, then publishes {@code id-2}, an ordinary record
 * with no nested call, and asserts it still reaches the handler.
 */
class KafkaCloudEventBridgeNestedRefusalTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void a_nested_handovers_permanent_refusal_escaping_a_handler_does_not_stop_this_bridges_own_healthy_model() throws Exception {
        // otherWrapper: its own catch-up fold throws, so its inner live feed's acceptRedeliverable(...) throws
        // PreDispatchRefusalException on every later call, unrelated to the bridge under test here.
        PushSubscriptionModel otherLiveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), new RoutingOutcomeChannel());
        InMemoryEventStore otherStore = new InMemoryEventStore();
        otherStore.write("s1", List.of(orderPlacedWithId("historical")));
        CatchupThenPushSubscriptionModel otherWrapper = new CatchupThenPushSubscriptionModel(otherStore, otherLiveFeed, null);
        SubscriptionHandle otherSubscription = otherWrapper.subscribe("other", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated permanent catch-up failure for the unrelated model");
        });
        assertThatThrownBy(() -> otherSubscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("otherWrapper's own catch-up must have actually failed and propagated")
                .hasMessageContaining("simulated permanent catch-up failure");

        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = "parking-topic-" + UUID.randomUUID();
        createNamedTopic(parkingTopic, 1);

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        liveFeed.subscribe("proj", ce -> {
            handled.add(ce.getId());
            if (ce.getId().equals("id-1")) {
                // The nested, unrelated refusal, escaping this handler unwrapped, the same as a handler that fans
                // an event out to a second projection or saga would let it.
                otherLiveFeed.acceptRedeliverable(orderPlacedWithId("id-1-fanout"));
            }
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), liveFeed, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.of(parkingTopic))
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).contains("id-1"));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(recordCount(parkingTopic))
                            .as("id-1's nested, unrelated refusal is an ordinary handler failure here, parked under "
                                    + "PARK rather than left for a permanent stop that never fires")
                            .isEqualTo(1));

            publishCloudEvent(topic, "stream-1", orderPlaced("id-2"));

            // A bridge incorrectly, permanently stopped by the nested refusal above would never reach id-2 at all.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled).contains("id-2"));
        } finally {
            deleteTopic(parkingTopic);
        }
    }

    private long recordCount(String recordTopic) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + recordTopic,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(recordTopic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(3));
            return records.count();
        }
    }

    private static CloudEvent orderPlacedWithId(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("com.acme.OrderPlaced")
                .withExtension("streamid", "s1")
                .build();
    }

    private Map<String, Object> consumerConfig(String groupId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static CloudEvent orderPlaced(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("com.acme.OrderPlaced")
                .withExtension("streamid", "stream-1")
                .build();
    }
}
