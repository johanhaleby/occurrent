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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Kafka half of {@code RabbitMqCloudEventBridgePauseDuringDeliveryTest}: {@code RoutingOutcome.NOT_DELIVERABLE}
 * covers both "the filter threw" and "the model is stopped / the subscription is paused / nothing registered"
 * alike, and only the first is a genuine failure. This bridge's coarse gate, {@code shouldConsume()}, is read once
 * before {@code poll()} and never rechecked inside {@code processBatch(...)}, so a {@code pauseSubscription(id)}
 * called from inside a record's own handler can still hand the bridge a lifecycle {@code NOT_DELIVERABLE} for a
 * later record in the very same poll batch. Before the fix this proves, every such record got parked and its
 * offset committed under {@link DeliveryFailurePolicy#PARK}, even though nothing about it was broken, only paced.
 * <p>
 * Publishes five records to a single-partition topic before the bridge ever starts, so a single {@code poll()} is
 * very likely to return all five in one batch, pauses the subscription from the first record's own handler, and
 * asserts no offset is committed past the first record and the parking topic stays empty.
 */
class KafkaCloudEventBridgePauseDuringDeliveryTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void pausing_a_subscription_from_inside_a_handler_does_not_park_or_commit_records_later_in_the_same_batch() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = "parking-topic-" + UUID.randomUUID();
        createNamedTopic(parkingTopic, 1);

        // Published before the bridge ever starts, so they are all already on the topic for the very first poll()
        // to return in one batch.
        publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-2"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-3"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-4"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-5"));

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", ce -> {
            handled.add(ce.getId());
            if (ce.getId().equals("id-1")) {
                // Paused from inside the very handler processing the first record in the batch. processBatch(..)
                // never rechecks shouldConsume() per record, so the remaining records in this same poll are what
                // this test is about.
                model.pauseSubscription("sub");
            }
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.of(parkingTopic))
                .build()) {
            // Long enough for the bridge's first poll to fetch and process the whole batch, well before a second
            // poll (pollTimeout above is 5 seconds) could ever start a fresh one.
            Thread.sleep(2000);

            assertThat(handled).as("the first record is delivered before the pause takes effect").contains("id-1");
            assertThat(recordCount(parkingTopic))
                    .as("a record paced behind a mid-delivery pause must never be parked, since nothing about it is broken")
                    .isZero();
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0)))
                    .as("no offset must ever be committed past the record that was actually delivered before the pause")
                    .isIn((Object) null, 1L);
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
