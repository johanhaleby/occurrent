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
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The {@link DeliveryFailurePolicy#REDELIVER} twin of {@code KafkaCloudEventBridgeNestedRefusalTest}, which only
 * exercises {@code PARK}: CLAIM 5 of PR #895's adversarial verification asks for both policies. The routing
 * decision under test, whether a {@code PreDispatchRefusalException} is this bridge's own model refusing
 * permanently versus an ordinary handler failure that happened to touch a different, broken model, is read from
 * {@code outcomeChannel.takeLastOutcome()} before {@code failureAction} is ever consulted, so it does not branch on
 * policy at all; this proves the same fix holds under the other one too.
 * <p>
 * Under {@code REDELIVER}, id-1's nested, unrelated refusal fails identically on every attempt (the same one
 * historical event keeps failing {@code otherWrapper}'s already-dead catch-up), so this bridge seeks back to it and
 * retries forever, rather than ever resolving it the way {@code PARK} does on the first attempt. What distinguishes
 * the fix from the bug this bridge stopping itself permanently, silently, the first time id-1's nested refusal
 * escapes is that the handler for id-1 keeps being invoked, over and over, proving the poll loop is still alive and
 * still consuming rather than dead after the very first delivery.
 */
class KafkaCloudEventBridgeNestedRefusalRedeliverTest extends KafkaTestSupport {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @Test
    void a_nested_handovers_permanent_refusal_under_redeliver_does_not_stop_this_bridges_own_healthy_model() throws Exception {
        PushSubscriptionModel otherLiveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), new RoutingOutcomeChannel());
        InMemoryEventStore otherStore = new InMemoryEventStore();
        otherStore.write("s1", List.of(orderPlacedWithId("historical")));
        CatchupThenPushSubscriptionModel otherWrapper = new CatchupThenPushSubscriptionModel(otherStore, otherLiveFeed, null);
        Subscription otherSubscription = otherWrapper.subscribe("other", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated permanent catch-up failure for the unrelated model");
        });
        assertThatThrownBy(() -> otherSubscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("otherWrapper's own catch-up must have actually failed and propagated")
                .hasMessageContaining("simulated permanent catch-up failure");

        String groupId = "group-" + UUID.randomUUID();

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> handled = new CopyOnWriteArrayList<>();
        liveFeed.subscribe("proj", ce -> {
            handled.add(ce.getId());
            if (ce.getId().equals("id-1")) {
                // The nested, unrelated refusal, escaping this handler unwrapped, on every single attempt.
                otherLiveFeed.acceptRedeliverable(orderPlacedWithId("id-1-fanout"));
            }
        });

        try (KafkaCloudEventBridge bridge = KafkaCloudEventBridge.builder(consumerConfig(groupId), liveFeed, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of(topic)))
                .pollTimeout(POLL_TIMEOUT)
                .onDeliveryFailure(DeliveryFailurePolicy.REDELIVER)
                .build()) {
            publishCloudEvent(topic, "stream-1", orderPlaced("id-1"));

            // A bridge incorrectly, permanently stopped by the nested refusal would handle id-1 exactly once, then
            // never again. The fix keeps retrying it, since REDELIVER seeks back and paces rather than resolving.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(handled.stream().filter("id-1"::equals).count())
                            .as("id-1's nested, unrelated refusal is an ordinary REDELIVER failure here: the poll "
                                    + "loop must still be alive, retrying it, not dead after the very first attempt")
                            .isGreaterThanOrEqualTo(3));
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

    private java.util.Map<String, Object> consumerConfig(String groupId) {
        return java.util.Map.of(
                org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId,
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
