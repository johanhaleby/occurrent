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

package org.occurrent.broker.kafka.blocking.domain;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.KafkaTestSupport;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class KafkaDomainEventBridgeTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void commits_on_delivered_and_the_committed_offset_advances() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
        // No catch-up in this test (nothing stored, nothing to replay): go straight live so a live event is folded
        // immediately rather than buffered awaiting a replay handover that never happens.
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * The domain bridge's own copy of {@code KafkaCloudEventBridgeTest}'s numeric-extension round-trip test. Both
     * bridges share {@code KafkaCloudEventMapper}, so the fix covers both, but this exact class of bug, a rebuilt
     * CloudEvent losing the type a live filter needs to match it, gets its own direct proof rather than relying on
     * the CloudEvent bridge's coverage alone.
     */
    @Test
    void a_numeric_extension_survives_the_broker_round_trip_so_a_stream_version_filter_still_matches_it() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()).and(Filter.streamVersion(Condition.eq(3L))));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withDataContentType("text/plain")
                    .withData("order-1".getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-1")
                    .withExtension("streamversion", 3L)
                    .build();
            publishCloudEvent(topic, "stream-1", cloudEvent);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
        }
    }

    @Test
    void the_bridge_does_not_consume_before_a_projection_is_registered_then_starts_once_one_is() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            Thread.sleep(POLL_TIMEOUT.toMillis() * 10);
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
            assertThat(handled).isEmpty();

            feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
    }

    /**
     * The silent-loss case ADR 133 exists to prevent. A projection registered but not yet caught up or gone live
     * only buffers in memory, and a {@code DomainEventFeed} bound for {@code goLive(..)} never replays, so a
     * committed record in that window would be lost for good on a crash. Proof this cannot happen: the offset
     * stays uncommitted until the application actually calls {@code goLive(..)}, at which point the record is
     * delivered and only then committed.
     */
    @Test
    void a_message_delivered_before_the_feed_has_gone_live_is_not_committed_and_is_delivered_once_it_does() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        // Registered, but goLive(..) is deliberately not called yet: hasProjection() is already true, so without the
        // isReadyForLiveDelivery() gate the bridge would start consuming and commit a merely buffered event.
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            Thread.sleep(POLL_TIMEOUT.toMillis() * 10);
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
            assertThat(handled).isEmpty();

            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    @Test
    void redelivers_on_a_projection_failure_and_the_retry_succeeds() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("fails once");
            }
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    @Test
    void a_projection_that_throws_an_assertionError_is_redelivered_rather_than_stalling_the_consumer() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("simulated assertion failure");
            }
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
        }
    }

    @Test
    void parks_a_delivery_that_keeps_failing_and_commits_the_original_only_once_the_park_is_confirmed() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = createTopic(1);
        KafkaDestination parkingDestination = KafkaDestination.of(parkingTopic);

        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            throw new RuntimeException("always fails");
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            publish("stream-1", "id-1", "order-1");

            ConsumerRecord<String, byte[]> parked = consumeOneRecord(parkingTopic);
            assertThat(headerValue(parked, "ce_id")).isEqualTo("id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    @Test
    void an_undecodable_message_is_parked_with_its_raw_bytes_and_the_original_commits_once_confirmed() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = createTopic(1);
        KafkaDestination parkingDestination = KafkaDestination.of(parkingTopic);

        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", event -> {
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            byte[] malformedBody = "not a cloud event".getBytes(StandardCharsets.UTF_8);
            publishRaw(topic, "stream-1", malformedBody);

            ConsumerRecord<String, byte[]> parked = consumeOneRecord(parkingTopic);
            assertThat(parked.value()).isEqualTo(malformedBody);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * The permanent stop {@code UnreadableLiveFilterException} requires: a data payload filter with no
     * {@code DataFieldReader} is refused live, the bridge stops itself rather than redelivering into the same
     * permanent failure, and the triggering record's offset is never committed. Proven here by the committed
     * offset staying at its pre-failure value (never advancing to or past the triggering record) even after the
     * bridge has visibly stopped, and by the consumer group having no members left, immediately, rather than
     * waiting out {@code max.poll.interval.ms}'s eviction window.
     */
    @Test
    void an_unreadable_live_filter_stops_the_bridge_and_leaves_the_group_immediately_without_committing_the_triggering_record() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        // A data-field filter with the default (refusing) DataFieldReader: refused live on the first acceptCloudEvent.
        feed.register("proj", handled::add, Filter.data("amount", Condition.eq(42)));
        feed.goLive("proj");

        KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build();
        try {
            publish("stream-1", "id-1", "order-1");

            // The bridge stops itself and leaves the group; wait for that to have visibly happened (no more
            // members), well under max.poll.interval.ms's default five-minute eviction window.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(consumerGroupMemberCount(groupId)).isZero());
            assertThat(handled).isEmpty();
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
        } finally {
            bridge.close();
        }
    }

    private Map<String, Object> consumerConfig(String groupId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private void publish(String key, String id, String orderId) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(TestOrderPlaced.class.getName())
                .withDataContentType("text/plain")
                .withData(orderId.getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .build();
        publishCloudEvent(topic, key, cloudEvent);
    }

    private record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {

        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withDataContentType("text/plain")
                    .withData(domainEvent.orderId().getBytes(StandardCharsets.UTF_8))
                    .build();
        }

        @Override
        public TestOrderPlaced toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new TestOrderPlaced(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
            return TestOrderPlaced.class.getName();
        }
    }
}
