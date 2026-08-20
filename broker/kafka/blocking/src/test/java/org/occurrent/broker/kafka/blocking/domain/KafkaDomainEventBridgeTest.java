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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
     * committed record in that window would be lost for good on a crash. Proof this cannot happen. The offset
     * stays uncommitted until the application actually calls {@code goLive(..)}, at which point the record is
     * delivered and only then committed.
     */
    @Test
    void a_message_delivered_before_the_feed_has_gone_live_is_not_committed_and_is_delivered_once_it_does() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        // Registered, but goLive(..) is deliberately not called yet. hasProjection() is already true, so without the
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

    /**
     * Proves ADR 133's rule directly on this bridge's own copy of the seek-and-break partition loop: "after a seek
     * the bridge stops processing that partition's remaining polled records." Three events land on the topic's one
     * partition, in order, before the bridge ever starts consuming, so all three are available in the very first
     * {@code poll()} call. The middle one fails on every attempt. If the loop kept walking the batch past it, the
     * third would eventually be handled and its offset committed. Instead it stays untouched for as long as the
     * middle one keeps failing. The CloudEvent-level bridge has its own version of this proof, but this bridge
     * carries its own separate copy of the same loop, so a regression here would not fail that suite.
     */
    @Test
    void after_a_seek_the_bridge_stops_processing_that_partitions_remaining_polled_records_for_this_poll() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        publish("stream-1", "id-1", "order-1");
        publish("stream-1", "id-2", "order-2");
        publish("stream-1", "id-3", "order-3");

        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger order2Attempts = new AtomicInteger();
        MaterializedView<TestOrderPlaced> view = event -> {
            if (event.orderId().equals("order-2")) {
                order2Attempts.incrementAndGet();
                throw new RuntimeException("order-2 always fails");
            }
            handled.add(event.orderId());
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            // order-2 is redelivered at least three times, proving the loop keeps retrying it rather than moving on.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(order2Attempts.get()).isGreaterThanOrEqualTo(3));

            assertThat(handled).containsExactly("order-1");
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L);
        }
    }

    /**
     * Proves the ADR's other half of the same rule on this bridge's own copy of the loop: "other partitions in the
     * same poll are unaffected, since their offsets are independent." Partition 0 gets a permanently-failing event,
     * partition 1 gets one that always succeeds. The second partition's offset still commits despite the first's
     * failure in the same poll batch.
     */
    @Test
    void a_failure_on_one_partition_does_not_block_committing_another_partitions_progress_in_the_same_poll() throws Exception {
        String twoPartitionTopic = createTopic(2);
        publishCloudEvent(twoPartitionTopic, 0, "stream-1", testOrderPlaced("id-fails", "order-fails"));
        publishCloudEvent(twoPartitionTopic, 1, "stream-1", testOrderPlaced("id-succeeds", "order-succeeds"));

        String groupId = "group-" + UUID.randomUUID();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger orderFailsAttempts = new AtomicInteger();
        MaterializedView<TestOrderPlaced> view = event -> {
            if (event.orderId().equals("order-fails")) {
                orderFailsAttempts.incrementAndGet();
                throw new RuntimeException("always fails");
            }
            handled.add(event.orderId());
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(twoPartitionTopic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly("order-succeeds"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(twoPartitionTopic, 1))).isEqualTo(1L));
            // Proves partition 0 was genuinely reached and kept failing, not merely never touched. The null commit
            // below is only meaningful once order-fails is known to have actually been retried more than once.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(orderFailsAttempts.get()).isGreaterThanOrEqualTo(2));
            assertThat(committedOffset(groupId, new TopicPartition(twoPartitionTopic, 0))).isNull();
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
     * Proves the {@code Consumer} thread-ownership property directly, this bridge's own copy of it:
     * {@link KafkaDomainEventBridge#close()} never touches the {@code Consumer} itself, only the loop thread's own
     * {@code finally} block does, reached once its current work actually finishes. A projection that blocks well
     * past {@link KafkaDomainEventBridge.Builder#closeTimeout(Duration)} proves both halves: {@code close()}
     * returns promptly, at the join timeout, rather than waiting for it, and the consumer group has not departed
     * yet at that point, since nothing has closed its {@code Consumer}. Only once the projection is released does
     * the loop thread finish its own iteration and reach its {@code finally}, and only then does the group show a
     * clean departure.
     */
    @Test
    void close_returns_at_the_join_timeout_and_the_consumer_only_closes_once_the_loop_thread_finishes() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        MaterializedView<TestOrderPlaced> view = event -> {
            handlerEntered.countDown();
            try {
                assertThat(releaseHandler.await(5, TimeUnit.SECONDS)).as("test released the handler in time").isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .closeTimeout(Duration.ofMillis(200))
                .build();
        publish("stream-1", "id-1", "order-1");
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).as("handler entered").isTrue();

        long closeStartedAtNanos = System.nanoTime();
        bridge.close();
        Duration closeElapsed = Duration.ofNanos(System.nanoTime() - closeStartedAtNanos);
        assertThat(closeElapsed).isLessThan(Duration.ofSeconds(3));

        assertThat(consumerGroupMemberCount(groupId)).isEqualTo(1);

        releaseHandler.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(consumerGroupMemberCount(groupId)).isZero());
    }

    /**
     * What the permanent stop for {@code UnreadableLiveFilterException} guarantees. A data payload filter with no
     * {@code DataFieldReader} is refused live, the bridge stops itself rather than redelivering into the same
     * permanent failure, and the triggering record's offset is never committed. Proven here by the committed
     * offset staying at its pre-failure value (never advancing to or past the triggering record) even after the
     * bridge has visibly stopped, and by the consumer group's member count making the round trip from zero to one
     * (proving the bridge actually joined) and back to zero once the triggering record arrives (proving a real
     * departure, not an initial state that was already zero before the bridge ever joined at all), well inside
     * {@code max.poll.interval.ms}'s eviction window rather than waiting it out.
     */
    @Test
    void an_unreadable_live_filter_stops_the_bridge_and_leaves_the_group_immediately_without_committing_the_triggering_record() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        // A data-field filter with the default (refusing) DataFieldReader. Refused live on the first acceptCloudEvent.
        feed.register("proj", handled::add, Filter.data("amount", Condition.eq(42)));
        feed.goLive("proj");

        KafkaDomainEventBridge<TestOrderPlaced> bridge = KafkaDomainEventBridge.builder(consumerConfig(groupId), feed)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build();
        try {
            // The feed is already live, so the bridge joins the group immediately on its own, before any record is
            // published. Awaiting that join first is what makes the later zero-member check a proof of departure
            // rather than a state that could have been true all along because the bridge never joined at all.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(consumerGroupMemberCount(groupId)).isEqualTo(1));

            publish("stream-1", "id-1", "order-1");

            // The bridge stops itself and leaves the group. Wait for that transition back to zero, well under
            // max.poll.interval.ms's default five-minute eviction window.
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
        publishCloudEvent(topic, key, testOrderPlaced(id, orderId));
    }

    private static CloudEvent testOrderPlaced(String id, String orderId) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(TestOrderPlaced.class.getName())
                .withDataContentType("text/plain")
                .withData(orderId.getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .build();
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
