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
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link KafkaCloudEventBridge.Builder#readinessSource(java.util.function.Predicate)} is the gate that closes the
 * catch-up acknowledgement hole, proven here against a real broker rather than against
 * {@code CatchupThenPushSubscriptionModel} itself, which {@code CatchupThenPushSubscriptionModelTest} already
 * covers for {@code isReadyForLiveDelivery(String)} in isolation. A {@code readinessSource} answering {@code false}
 * must keep this bridge from ever fetching a matching record at all, not merely from committing one it already
 * fetched, since a record that reaches the handler here would already have been staged for commit on
 * {@code RoutingOutcome.DELIVERED} before this test could observe anything wrong. The crash-shaped case (records
 * queue up while not ready, and a fresh bridge on the same {@code group.id} still sees them once ready) proves the
 * offset itself was never advanced during the not-ready window, not merely that the in-process handler was quiet.
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
