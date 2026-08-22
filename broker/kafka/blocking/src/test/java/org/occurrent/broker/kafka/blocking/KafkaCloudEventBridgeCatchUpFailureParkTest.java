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
import org.apache.kafka.common.TopicPartition;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka half of {@code RabbitMqCloudEventBridgeCatchUpFailureParkTest}: a permanently failed catch-up must
 * stop {@link KafkaCloudEventBridge} rather than route every later record through
 * {@link DeliveryFailurePolicy}. Before the fix this proves, a later record under
 * {@link DeliveryFailurePolicy#PARK} was republished to the parking topic and its offset staged for commit,
 * exactly as a genuinely delivered record would be, silently discarding records arriving after the failure
 * instead of leaving their offset uncommitted.
 * <p>
 * Forces a catch-up failure (a fold that throws for the one historical event), confirms the failure propagated,
 * then publishes two live records and asserts no offset is ever committed for them and the parking topic stays
 * empty.
 */
class KafkaCloudEventBridgeCatchUpFailureParkTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void records_published_after_a_permanent_catch_up_failure_are_not_parked_or_committed_under_PARK() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = "parking-topic-" + UUID.randomUUID();
        createNamedTopic(parkingTopic, 1);

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(orderPlacedWithId("historical")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        // The replay fold throws for the one historical event, which BlockingHandover.catchUp(..) records as a
        // permanent catch-up failure (catchUpFailure), then rethrows.
        SubscriptionHandle subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated catch-up fold failure for " + ce.getId());
        });

        assertThatThrownBy(() -> subscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the catch-up replay must have failed and propagated the failure")
                .hasMessageContaining("simulated catch-up fold failure");
        assertThat(model.isReadyForLiveDelivery("proj")).as("a failed catch-up is never ready for live delivery").isFalse();

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), liveFeed, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.of(parkingTopic))
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
            publishCloudEvent(topic, "stream-1", orderPlaced("id-2"));

            // Long enough for several poll cycles and several seek-back-and-retry round trips, so this is a
            // genuine steady-state assertion, not a race against the bridge's own pace.
            Thread.sleep(1000);

            assertThat(recordCount(parkingTopic))
                    .as("a record that arrived after a permanently failed catch-up must never be parked")
                    .isZero();
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0)))
                    .as("no offset must ever be committed for a record refused by a permanently failed catch-up")
                    .isNull();
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
