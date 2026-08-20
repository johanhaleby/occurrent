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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every refusal {@link KafkaCloudEventBridge.Builder#build()} makes happens before a {@code Consumer} (or, under
 * {@link DeliveryFailurePolicy#PARK}, a parking {@code Producer}) is ever constructed, so none of these tests need
 * a real broker or a mocked one. {@code bootstrap.servers} is deliberately left empty in every case, which
 * {@code KafkaConsumer}'s own constructor would refuse with a {@code ConfigException} if it were ever reached, so
 * an {@link IllegalStateException} rather than that proves the refusal ran first.
 */
class KafkaCloudEventBridgeBuildFailureTest {

    @Test
    void onDeliveryFailure_PARK_without_a_parkingDestination_is_refused_before_any_consumer_is_opened() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(validConsumerConfig(), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of("topic")))
                .onDeliveryFailure(DeliveryFailurePolicy.PARK);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parkingDestination");
    }

    @Test
    void onDeliveryFailure_PARK_with_a_pattern_typed_parkingDestination_is_refused() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(validConsumerConfig(), model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of("topic")))
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(KafkaDestination.ofPattern("prefix-.*"));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pattern-typed");
    }

    @Test
    void no_resolver_and_no_explicit_bindings_is_refused() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(validConsumerConfig(), model, outcomeChannel);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("resolver");
    }

    @Test
    void consumerConfig_missing_group_id_is_refused_rather_than_failing_invisibly_later() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * Kafka treats a blank {@code group.id} the same as an absent one, throwing {@code InvalidGroupIdException}
     * only later, at the first commit or group operation, not at {@code KafkaConsumer} construction. Refused here
     * for the same reason a missing {@code group.id} is, rather than left to build a bridge whose only poll loop
     * then fails repeatedly instead of ever consuming.
     */
    @Test
    void consumerConfig_with_a_blank_group_id_is_refused_rather_than_failing_invisibly_later() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "   ",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Test
    void consumerConfig_with_enable_auto_commit_absent_is_refused() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
                .bindings(Set.of(KafkaDestination.of("topic")));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    @Test
    void consumerConfig_with_enable_auto_commit_set_to_true_is_refused() {
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
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
}
