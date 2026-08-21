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
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code PushSubscriptionModel.acceptRedeliverable(CloudEvent)} reporting {@code RoutingOutcome.DEFERRED} is what
 * closes the catch-up acknowledgement hole on its own, with no {@code readinessSource} configured at all.
 * {@link KafkaCloudEventBridge.Builder#readinessSource(java.util.function.Predicate)} is a pacing hint on top of
 * that, proven here against a real broker rather than against {@code CatchupThenPushSubscriptionModel} itself,
 * which {@code CatchupThenPushSubscriptionModelTest} already covers for {@code isReadyForLiveDelivery(String)} in
 * isolation. A {@code readinessSource} answering {@code false} keeps this bridge from fetching a matching record
 * at all, cutting down on how often the refuse-and-redeliver round trip below happens, but is never what makes
 * that round trip safe. The crash-shaped case (records queue up while not ready, and a fresh bridge on the same
 * {@code group.id} still sees them once ready) proves the offset itself was never advanced during the not-ready
 * window, not merely that the in-process handler was quiet, and this bridge's per-partition throttle already
 * keeps a repeatedly {@code DEFERRED} record from driving it into a tight loop, with or without a
 * {@code readinessSource} configured.
 */
class KafkaCloudEventBridgeReadinessTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void a_readiness_source_answering_false_keeps_a_record_uncommitted_and_never_invokes_the_handler() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .readinessSource(subscriptionId -> false)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            // Long enough for several poll cycles, so this is a genuine "never fetched" assertion, not a race
            // against the bridge's own poll cadence.
            Thread.sleep(500);

            assertThat(handled).isEmpty();
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
        }
    }

    @Test
    void flipping_readiness_to_true_commits_a_record_that_arrived_while_not_ready() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        AtomicBoolean ready = new AtomicBoolean(false);
        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .readinessSource(subscriptionId -> ready.get())
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
            Thread.sleep(300);
            assertThat(handled).isEmpty();

            ready.set(true);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * The crash-shaped case. A record that arrives while not ready is never committed, so a bridge that crashes
     * (closed here, never having gone ready) and restarts on the same {@code group.id} still sees it, from the
     * broker's own backlog, exactly as at-least-once redelivery already guarantees for any other uncommitted record.
     * Nothing about the not-ready window is special-cased away by a partial commit.
     */
    @Test
    void a_record_that_never_became_ready_before_the_bridge_closed_is_redelivered_to_a_fresh_bridge_on_the_same_group() throws Exception {
        String groupId = "group-" + UUID.randomUUID();

        RoutingOutcomeChannel outcomeChannel1 = new RoutingOutcomeChannel();
        PushSubscriptionModel model1 = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel1);
        List<CloudEvent> handled1 = new CopyOnWriteArrayList<>();
        model1.subscribe("sub", cloudEvent -> handled1.add(cloudEvent));

        try (KafkaCloudEventBridge bridge1 = KafkaCloudEventBridge.builder(consumerConfig(groupId), model1, outcomeChannel1)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .readinessSource(subscriptionId -> false)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));
            Thread.sleep(500);
            assertThat(handled1).isEmpty();
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();
        }
        // bridge1 closed above, simulating a crash before readiness ever flipped true. Nothing was ever committed.

        RoutingOutcomeChannel outcomeChannel2 = new RoutingOutcomeChannel();
        PushSubscriptionModel model2 = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel2);
        List<CloudEvent> handled2 = new CopyOnWriteArrayList<>();
        model2.subscribe("sub", cloudEvent -> handled2.add(cloudEvent));

        try (KafkaCloudEventBridge bridge2 = KafkaCloudEventBridge.builder(consumerConfig(groupId), model2, outcomeChannel2)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled2).hasSize(1));
            assertThat(handled2.get(0).getId()).isEqualTo("id-1");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    @Test
    void no_readiness_source_configured_consumes_immediately_the_same_as_before_this_capability_existed() throws Exception {
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

    /**
     * The dec-0009 falsifying race, closed by construction this time rather than by narrowing a timing window: a
     * {@link CatchupThenPushSubscriptionModel} still replaying refuses a live delivery outright
     * ({@code RoutingOutcome.DEFERRED}) instead of buffering it and reporting {@code DELIVERED} ahead of the fold.
     * No {@code readinessSource} is configured here at all, proving {@code DEFERRED} alone, not the pacing hint, is
     * what keeps this bridge correct.
     */
    @Test
    void a_record_that_arrives_during_a_catch_up_replay_is_never_committed_and_is_delivered_once_live_with_no_readiness_source_configured() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(orderPlacedWithId("historical")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).as("the replay reached its one historical event").isTrue();

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), liveFeed, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            // Long enough for many seek-back-and-retry round trips against DEFERRED, so this is a genuine
            // "never folded" assertion, not a race against the bridge's own retry pace.
            Thread.sleep(500);

            assertThat(folded).as("refused, never staged for commit, while the replay is still blocked").doesNotContain("id-1");
            assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isNull();

            releaseReplay.countDown();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(folded).contains("id-1"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(committedOffset(groupId, new TopicPartition(topic, 0))).isEqualTo(1L));
        }
    }

    /**
     * {@code RoutingOutcome.DEFERRED} bypasses {@link DeliveryFailurePolicy} entirely, including {@code PARK}: a
     * record that only needs the replay to catch up must never be republished to a parking destination, since
     * nothing about it is broken. Reuses {@code processBatch(..)}'s existing seek-back and pacing, so this also
     * proves no new machinery was needed to redeliver a {@code DEFERRED} record.
     */
    @Test
    void park_configured_never_parks_a_deferred_record() throws Exception {
        String groupId = "group-" + UUID.randomUUID();
        String parkingTopic = createTopic(1);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(orderPlacedWithId("historical")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), liveFeed, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.of(parkingTopic))
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            Thread.sleep(500);

            assertThat(recordCount(parkingTopic)).as("DEFERRED bypasses PARK entirely").isZero();
        } finally {
            releaseReplay.countDown();
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

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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
