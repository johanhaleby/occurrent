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

package org.occurrent.springboot.broker.kafka.blocking;

import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventBridge;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.broker.kafka.blocking.RoutingOutcomeChannel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Proves the wiring rules from the plan gate without a real broker. That covers what activates the whole
 * configuration, {@code bootstrap-servers} recognized in both its comma-separated and its indexed YAML list
 * binding forms included, what stays absent until its own prerequisite is met, and that {@code @Fallback} defers
 * to an application's own bean. Building a sink or a bridge never needs a reachable broker, since the Kafka client
 * waits until it actually sends or polls before it opens a connection. Every {@code bootstrap-servers} value here
 * is a placeholder address never actually dialed. That behavior is proved for real against a Testcontainers broker
 * in {@link KafkaBrokerAutoConfigurationIntegrationTest}.
 */
class OccurrentKafkaAutoConfigurationWiringTest {

    private static final String FAKE_BOOTSTRAP_SERVERS = "localhost:19092";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentKafkaAutoConfiguration.class))
            .withUserConfiguration(EnabledConfiguration.class);

    @Test
    void nothing_is_registered_without_bootstrap_servers() {
        contextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(KafkaCloudEventSink.class);
            assertThat(context).doesNotHaveBean(KafkaCloudEventBridgeFactory.class);
            assertThat(context).doesNotHaveBean(org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver.class);
        });
    }

    @Test
    void activates_with_the_comma_separated_scalar_binding_form() {
        contextRunner.withPropertyValues("occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS)
                .run(context -> assertThat(context).hasSingleBean(KafkaCloudEventBridgeFactory.class));
    }

    @Test
    void activates_with_the_indexed_yaml_list_binding_form() {
        contextRunner.withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers[0]=host1:9092",
                        "occurrent.broker.kafka.bootstrap-servers[1]=host2:9092")
                .run(context -> assertThat(context).hasSingleBean(KafkaCloudEventBridgeFactory.class));
    }

    /**
     * {@code bootstrap-servers[0]=} with nothing after the {@code =} binds to a list holding one blank string,
     * nonempty but configuring no actual server, the same reading the scalar form {@code bootstrap-servers=}
     * already rejects. {@link KafkaBootstrapServersConfiguredCondition} must reject this indexed form the same
     * way, rather than activating the starter on a blank entry that only fails once Kafka's own client uses it.
     */
    @Test
    void does_not_activate_for_an_indexed_form_with_only_a_blank_element() {
        contextRunner.withPropertyValues("occurrent.broker.kafka.bootstrap-servers[0]=")
                .run(context -> assertThat(context).doesNotHaveBean(KafkaCloudEventBridgeFactory.class));
    }

    @Test
    void resolver_bean_activates_once_the_topic_property_is_present() {
        contextRunner.withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS,
                        "occurrent.broker.kafka.topic=orders")
                .run(context -> {
                    assertThat(context).hasSingleBean(org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver.class);
                    assertThat(context.getBean(DestinationResolver.class)).isInstanceOf(org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver.class);
                });
    }

    /**
     * A plain {@code @ConditionalOnProperty} treats the literal value {@code false} as absent, even though
     * {@code false} is a legal Kafka topic name. {@link KafkaTopicConfiguredCondition} activates on any nonblank
     * value instead.
     */
    @Test
    void resolver_bean_activates_for_a_topic_literally_named_false() {
        contextRunner.withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS,
                        "occurrent.broker.kafka.topic=false")
                .run(context -> assertThat(context).hasSingleBean(org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver.class));
    }

    /**
     * A plain {@code @ConditionalOnProperty} treats an empty value as present, which would build a resolver for a
     * blank topic name here, one that later fails when a bridge tries to subscribe to it.
     * {@link KafkaTopicConfiguredCondition} requires a nonblank value.
     */
    @Test
    void resolver_bean_does_not_activate_for_a_blank_topic() {
        contextRunner.withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS,
                        "occurrent.broker.kafka.topic=")
                .run(context -> assertThat(context).doesNotHaveBean(org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver.class));
    }

    @Test
    void a_user_supplied_cloud_event_sink_takes_precedence_over_the_fallback() {
        CloudEventSink userSink = mock(CloudEventSink.class);
        contextRunner
                .withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS,
                        "occurrent.broker.kafka.topic=orders")
                .withBean(CloudEventSink.class, () -> userSink)
                .run(context -> {
                    // Both bean definitions genuinely exist, the user's and the @Fallback one, so hasSingleBean(...)
                    // would fail here on bean count alone. getBeanNamesForType(...) lists both without
                    // instantiating either, unlike assertThat(context).hasBean(name), which resolves the bean by
                    // name and would force the @Lazy fallback to build a producer. @Fallback only changes which one
                    // getBean(Class) resolves to, the same distinction
                    // OccurrentMongoAutoConfigurationStarterValidationTest draws for its own CloudEventConverter
                    // fallback.
                    assertThat(context.getBeanNamesForType(CloudEventSink.class))
                            .containsExactlyInAnyOrder("cloudEventSink", "occurrentKafkaCloudEventSink");
                    assertThat(context.getBean(CloudEventSink.class)).isSameAs(userSink);
                });
    }

    @Test
    void bridge_without_a_resolver_or_explicit_bindings_refuses_at_build() {
        contextRunner.withPropertyValues("occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS)
                .run(context -> {
                    KafkaCloudEventBridgeFactory factory = context.getBean(KafkaCloudEventBridgeFactory.class);
                    KafkaCloudEventBridge.Builder builder = factory.forGroup("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());
                    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("resolver");
                });
    }

    @Test
    void bridge_configured_to_park_without_a_parking_destination_refuses_at_build() {
        contextRunner.withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + FAKE_BOOTSTRAP_SERVERS,
                        "occurrent.broker.kafka.topic=orders",
                        "occurrent.broker.kafka.bridge.on-delivery-failure=PARK")
                .run(context -> {
                    KafkaCloudEventBridgeFactory factory = context.getBean(KafkaCloudEventBridgeFactory.class);
                    KafkaCloudEventBridge.Builder builder = factory.forGroup("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());
                    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("parkingDestination");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentKafkaBroker
    static class EnabledConfiguration {
    }
}
