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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.condition.Condition;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class KafkaCloudEventBridgeTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void commits_on_delivered_and_the_committed_offset_advances() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    @Test
    void a_filtered_event_commits_too_without_invoking_the_handler() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type("com.acme.SomethingElse")), cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
            assertThat(handled).isEmpty();
        }
    }

    /**
     * The silent-loss case {@link KafkaCloudEventMapper}'s two verified corrections exist to prevent. Without the
     * {@code Long} restoration, {@code Filter.streamVersion(eq(3))} would compare a restored {@code String} "3"
     * against the {@code Long} operand and never match, and the bridge would commit a FILTERED delivery that
     * should have been DELIVERED.
     */
    @Test
    void a_numeric_extension_survives_the_broker_round_trip_so_a_numeric_filter_still_matches_it() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.streamVersion(Condition.eq(3L))), cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType("com.acme.OrderPlaced")
                    .withExtension("streamid", "stream-1")
                    .withExtension("streamversion", 3L)
                    .build();
            publishCloudEvent(topic, "stream-1", cloudEvent);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
    }

    @Test
    void the_bridge_does_not_consume_before_a_subscription_is_registered_then_starts_once_one_is() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            // No subscription registered yet. Give the coarse poll several chances to (wrongly) consume, then
            // confirm nothing committed and nothing was handled.
            Thread.sleep(POLL_TIMEOUT.toMillis() * 10);
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
            assertThat(handled).isEmpty();

            model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * The redelivery-no-double-process proof. The handler applies its effect (recording the event id in an
     * idempotent set) and only then throws on its first attempt, simulating a failure that happens after the
     * effect but before the bridge could commit it. The bridge seeks back, the next poll redelivers the same
     * record, and the second attempt's idempotent {@code add} to the same set is a no-op, proving the handler
     * applied the event exactly once despite being handed it twice.
     */
    @Test
    void a_handler_that_fails_once_is_redelivered_and_an_idempotent_fold_applies_the_event_exactly_once() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
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

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
        assertThat(appliedIds).containsExactly("id-1");
    }

    @Test
    void a_handler_that_throws_an_assertionError_is_redelivered_rather_than_stalling_the_consumer() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        AtomicInteger attempts = new AtomicInteger();
        model.subscribe("sub", cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("simulated assertion failure");
            }
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
        }
    }

    /**
     * Proves ADR 133's rule directly, "after a seek the bridge stops processing that partition's remaining polled
     * records." Three events land on the topic's one partition, in order, before the bridge ever starts consuming,
     * so all three are available in the bridge's very first {@code poll()} call. The middle one fails on every
     * attempt. If the bridge kept walking the batch past it, the third event would eventually be handled and its
     * offset committed. Instead it stays untouched for as long as the middle one keeps failing.
     */
    @Test
    void after_a_seek_the_bridge_stops_processing_that_partitions_remaining_polled_records_for_this_poll() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-2"));
        publishCloudEvent(topic, "stream-1", orderPlaced("id-3"));

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger id2Attempts = new AtomicInteger();
        model.subscribe("sub", cloudEvent -> {
            if (cloudEvent.getId().equals("id-2")) {
                id2Attempts.incrementAndGet();
                throw new RuntimeException("id-2 always fails");
            }
            handled.add(cloudEvent.getId());
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            // id-2 is redelivered at least three times, proving the loop keeps retrying it rather than moving on.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(id2Attempts.get()).isGreaterThanOrEqualTo(3));

            assertThat(handled).containsExactly("id-1");
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L);
        }
    }

    /**
     * Proves the ADR's other half of the same rule, "other partitions in the same poll are unaffected, since their
     * offsets are independent." Partition 0 gets a permanently-failing event, partition 1 gets one that always
     * succeeds. The second partition's offset still commits despite the first's failure in the same poll batch.
     */
    @Test
    void a_failure_on_one_partition_does_not_block_committing_another_partitions_progress_in_the_same_poll() throws Exception {
        String twoPartitionTopic = createTopic(2);
        publishCloudEvent(twoPartitionTopic, 0, null, orderPlaced("id-fails"));
        publishCloudEvent(twoPartitionTopic, 1, null, orderPlaced("id-succeeds"));

        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger idFailsAttempts = new AtomicInteger();
        model.subscribe("sub", cloudEvent -> {
            if (cloudEvent.getId().equals("id-fails")) {
                idFailsAttempts.incrementAndGet();
                throw new RuntimeException("always fails");
            }
            handled.add(cloudEvent.getId());
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(twoPartitionTopic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly("id-succeeds"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(twoPartitionTopic, 1))).isEqualTo(1L));
            // Proves partition 0 was genuinely reached and kept failing, not merely never touched. The null commit
            // below is only meaningful once id-fails is known to have actually been retried more than once.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(idFailsAttempts.get()).isGreaterThanOrEqualTo(2));
            assertThat(committedOffset(groupId, new TopicPartition(twoPartitionTopic, 0))).isNull();
        }
    }

    @Test
    void parks_a_delivery_that_keeps_failing_and_commits_the_original_only_once_the_park_is_confirmed() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = createTopic(1);
        KafkaDestination parkingDestination = KafkaDestination.of(parkingTopic);

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
            throw new RuntimeException("always fails");
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

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

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            byte[] malformedBody = "not a cloud event".getBytes();
            publishRaw(topic, "stream-1", malformedBody);

            ConsumerRecord<String, byte[]> parked = consumeOneRecord(parkingTopic);
            assertThat(parked.value()).isEqualTo(malformedBody);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * Proves the {@code Consumer} thread-ownership property directly. {@link KafkaCloudEventBridge#close()} never
     * touches the {@code Consumer} itself, only the loop thread's own {@code finally} block does, reached once its
     * current work actually finishes. A handler that blocks well past {@link KafkaCloudEventBridge.Builder#closeTimeout(Duration)}
     * proves both halves. {@code close()} returns promptly, at the join timeout, rather than waiting for the
     * handler, and the consumer group has not departed yet at that point, since nothing has closed its
     * {@code Consumer}. Only once the handler is released does the loop thread finish its own iteration and reach
     * its {@code finally}, and only then does the group show a clean departure.
     */
    @Test
    void close_returns_at_the_join_timeout_and_the_consumer_only_closes_once_the_loop_thread_finishes() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        model.subscribe("sub", cloudEvent -> {
            handlerEntered.countDown();
            try {
                assertThat(releaseHandler.await(5, TimeUnit.SECONDS)).as("test released the handler in time").isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .closeTimeout(Duration.ofMillis(200))
                .build();
        publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).as("handler entered").isTrue();

        long closeStartedAtNanos = System.nanoTime();
        bridge.close();
        Duration closeElapsed = Duration.ofNanos(System.nanoTime() - closeStartedAtNanos);
        // Well under the handler's own wait, proving close() did not block on it.
        assertThat(closeElapsed).isLessThan(Duration.ofSeconds(3));

        // The loop thread is still inside the handler, so nothing has closed its Consumer yet, this caller thread
        // included. The group is still exactly as joined as it was before close() was ever called.
        assertThat(consumerGroupMemberCount(groupId)).isEqualTo(1);

        releaseHandler.countDown();

        // Only once the handler returns does the loop thread finish this iteration, notice running is now false,
        // and reach its own finally, closing the Consumer and departing the group cleanly.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(consumerGroupMemberCount(groupId)).isZero());
    }

    /**
     * {@link EventA} is the top-level fixture {@code KafkaTopicPerTypeDestinationResolverTest} already declares in
     * this package, reused here rather than redeclared, since a nested class's qualified name would carry a
     * {@code $} that {@link KafkaTopicPerTypeDestinationResolver} refuses as an illegal topic name.
     */
    @Test
    void resolver_and_bindingFilter_narrows_the_subscribed_topics_to_destinationsFor() throws Exception {
        String eventATopic = EventA.class.getName();
        createNamedTopic(eventATopic, 1);
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver("", ReflectionCloudEventTypeMapper.qualified());

        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .resolver(resolver)
                .bindingFilter(StreamSubscriptionFilter.filter(Filter.type(eventATopic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(eventATopic, "stream-1", CloudEventBuilder.v1()
                    .withId("id-1").withSource(URI.create("urn:test")).withType(eventATopic)
                    .withExtension("streamid", "stream-1").build());

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        } finally {
            deleteTopic(eventATopic);
        }
    }

    @Test
    void resolver_alone_falls_back_to_catchAllDestination_subscribing_by_literal_topic_for_the_shared_topic_resolver() throws Exception {
        KafkaSharedTopicDestinationResolver resolver = new KafkaSharedTopicDestinationResolver(topic);
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .resolver(resolver)
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
    }

    @Test
    void resolver_alone_falls_back_to_catchAllDestination_subscribing_by_pattern_for_the_per_type_resolver() throws Exception {
        String prefix = "pattern-" + UUID.randomUUID() + "-";
        String topicA = prefix + "TypeA";
        String topicB = prefix + "TypeB";
        createNamedTopic(topicA, 1);
        createNamedTopic(topicB, 1);
        // catchAllDestination() never consults the type mapper, so any valid one works here. qualified() needs no
        // reference class the way simple(...) would.
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(prefix, ReflectionCloudEventTypeMapper.qualified());

        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .resolver(resolver)
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topicA, "stream-1", CloudEventBuilder.v1()
                    .withId("id-a").withSource(URI.create("urn:test")).withType("TypeA")
                    .withExtension("streamid", "stream-1").build());
            publishCloudEvent(topicB, "stream-1", CloudEventBuilder.v1()
                    .withId("id-b").withSource(URI.create("urn:test")).withType("TypeB")
                    .withExtension("streamid", "stream-1").build());

            // One bridge, subscribed by pattern with no explicit topic list, consumes both topics.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
        } finally {
            deleteTopic(topicA);
            deleteTopic(topicB);
        }
    }

    @Test
    void a_bindings_set_mixing_literal_and_pattern_typed_destinations_is_refused_at_build() {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        assertThatThrownBy(() -> KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic), KafkaDestination.ofPattern("prefix-.*")))
                .pollTimeout(POLL_TIMEOUT)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mix pattern-typed and literal");
    }

    /**
     * Proves this bridge survives a commit failure Kafka itself never marks retriable, rather than crashing or
     * silently losing the record. max.poll.interval.ms is set low and the handler blocks past it on id-1's first
     * delivery, so this Consumer is evicted from its own group by the time the handler returns and commitSync is
     * attempted, throwing CommitFailedException. The rejoin that follows recovers this bridge on its own, Kafka's
     * own committed-offset lookup on rejoin is what guarantees id-1 gets refetched here, not the rewind
     * {@code processBatch(...)} applies on an escaped commit failure. An eviction always regains a
     * fully lost assignment by re-reading the last committed offset, which the rewind is not needed to reproduce.
     * The rewind exists for the narrower case a full eviction cannot exercise. A commit that fails while this
     * Consumer's own group membership and assignment are never disturbed at all, which is not reproducible against
     * a real broker without either mocking or fault injection this module does not otherwise use, so this proof
     * is deliberately the closest one reachable without either, real resilience against a genuine commit failure,
     * not a mutation-discriminating proof of the rewind line itself.
     */
    @Test
    void a_commit_that_fails_permanently_does_not_crash_the_bridge_and_id_1_is_still_delivered_once_it_recovers() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "3000");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        AtomicInteger id1Attempts = new AtomicInteger();
        model.subscribe("sub", cloudEvent -> {
            if (cloudEvent.getId().equals("id-1") && id1Attempts.incrementAndGet() == 1) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(id1Attempts.get()).isGreaterThanOrEqualTo(2));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
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
