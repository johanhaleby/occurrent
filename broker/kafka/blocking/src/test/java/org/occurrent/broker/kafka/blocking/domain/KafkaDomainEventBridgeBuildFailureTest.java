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
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The domain bridge's twin of {@code KafkaCloudEventBridgeBuildFailureTest}. Every refusal happens before a
 * {@code Consumer} is ever constructed, so none of these need a real or mocked broker. See that class's own
 * javadoc for why an empty {@code bootstrap.servers} proves the refusal ran first.
 */
class KafkaDomainEventBridgeBuildFailureTest {

    @Test
    void onDeliveryFailure_PARK_without_a_parkingDestination_is_refused_before_any_consumer_is_opened() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(validConsumerConfig(), feed)
                .bindings(Set.of(KafkaDestination.of("topic")))
                .onDeliveryFailure(DeliveryFailurePolicy.PARK);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parkingDestination");
    }

    @Test
    void onDeliveryFailure_PARK_with_a_pattern_typed_parkingDestination_is_refused() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(validConsumerConfig(), feed)
                .bindings(Set.of(KafkaDestination.of("topic")))
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.ofPattern("prefix-.*"));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pattern-typed");
    }

    @Test
    void no_resolver_and_no_explicit_bindings_is_refused() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(validConsumerConfig(), feed);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("resolver");
    }

    @Test
    void consumerConfig_missing_group_id_is_refused_rather_than_failing_invisibly_later() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(consumerConfig, feed)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Test
    void consumerConfig_with_enable_auto_commit_absent_is_refused() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(consumerConfig, feed)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    @Test
    void consumerConfig_with_enable_auto_commit_set_to_true_is_refused() {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaDomainEventBridge.Builder<TestOrderPlaced> builder = KafkaDomainEventBridge.builder(consumerConfig, feed)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    private static Map<String, Object> validConsumerConfig() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    private record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {

        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId("id")
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
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
