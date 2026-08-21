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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventBridge;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.broker.kafka.blocking.RoutingOutcomeChannel;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka twin of {@code CatchupThenPushReadinessAmbiguousIdTest} on the RabbitMQ starter: two
 * {@code CatchupThenPushSubscriptionModel} beans subscribed under the same id ("orders", ADR 102 permits exactly
 * this), the first with a permanently failed catch-up, the second healthy. {@link CatchupThenPushReadiness#isReady}
 * picking a wrapper by id alone across the whole context would answer for the healthy bridge with the failing
 * wrapper's own permanent {@code false}, starving it forever even though its own model has nothing wrong with it.
 * {@link DefaultKafkaCloudEventBridgeFactory#forGroup} correlates by identity instead
 * ({@link CatchupThenPushReadiness#memoized(org.springframework.context.ApplicationContext, PushSubscriptionModel)}),
 * so the healthy model's own bridge, built with no manual {@code readinessSource(...)} call, must still consume.
 */
@Testcontainers
class CatchupThenPushReadinessAmbiguousIdTest {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:" + kafkaVersion()).withReuse(true);

    @Test
    void a_healthy_models_bridge_still_consumes_even_though_a_different_models_wrapper_sharing_its_subscription_id_has_permanently_failed() throws Exception {
        // The failing model: its catch-up fold throws, permanently failing its handover, so
        // isReadyForLiveDelivery("orders") answers false forever from this point on.
        PushSubscriptionModel failingLiveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), new RoutingOutcomeChannel());
        InMemoryEventStore failingStore = new InMemoryEventStore();
        failingStore.write("s1", List.of(CloudEventBuilder.v1()
                .withId("historical")
                .withSource(URI.create("urn:occurrent:test"))
                .withType(TestEvent.class.getName())
                .build()));
        CatchupThenPushSubscriptionModel failingWrapper = new CatchupThenPushSubscriptionModel(failingStore, failingLiveFeed, null);
        Subscription failingSubscription = failingWrapper.subscribe("orders", null, StartAt.subscriptionModelDefault(), ce -> {
            throw new RuntimeException("simulated permanent catch-up failure for the OTHER model sharing this id");
        });
        assertThatThrownBy(() -> failingSubscription.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the failing model's catch-up must have actually failed and propagated")
                .hasMessageContaining("simulated permanent catch-up failure");
        assertThat(failingWrapper.isReadyForLiveDelivery("orders")).isFalse();

        // The healthy model: nothing to replay, reaches live immediately. Its own outcomeChannel is kept, since
        // the bridge built further down must share the exact same one this live feed was constructed with.
        BlockingQueue<CloudEvent> healthyReceived = new LinkedBlockingQueue<>();
        RoutingOutcomeChannel healthyOutcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel healthyLiveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), healthyOutcomeChannel);
        CatchupThenPushSubscriptionModel healthyWrapper = new CatchupThenPushSubscriptionModel(new InMemoryEventStore(), healthyLiveFeed, null);
        healthyWrapper.subscribe("orders", null, StartAt.subscriptionModelDefault(), healthyReceived::add)
                .waitUntilStarted(Duration.ofSeconds(5));
        assertThat(healthyWrapper.isReadyForLiveDelivery("orders")).isTrue();

        String topic = "test-topic-" + UUID.randomUUID();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentKafkaAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + kafkaContainer.getBootstrapServers(),
                        "occurrent.broker.kafka.topic=" + topic,
                        "occurrent.broker.kafka.bridge.poll-timeout=100ms",
                        "occurrent.broker.kafka.consumer.additional-properties[auto.offset.reset]=earliest"
                )
                .run(context -> {
                    GenericApplicationContext springContext = (GenericApplicationContext) context.getSourceApplicationContext();
                    // Registered in this order deliberately: the failing wrapper first, so
                    // ApplicationContext.getBeansOfType(...) is very likely to hand it back before the healthy one,
                    // exactly the ambiguity this test describes. Bean names are irrelevant, CatchupThenPushReadiness
                    // looks these up by type for its id-based fallback, not by the
                    // "catchupThenPushSubscriptionModel-<id>" convention, and not through the identity registry
                    // either, since neither wrapper was published through the framework registrar.
                    springContext.getBeanFactory().registerSingleton("failingWrapper", failingWrapper);
                    springContext.getBeanFactory().registerSingleton("healthyWrapper", healthyWrapper);

                    // The root cause, checked directly: the ambiguous id-only lookup answers false for "orders" as
                    // long as the failing wrapper is found first, regardless of which model is actually asking.
                    assertThat(CatchupThenPushReadiness.isReady(springContext, "orders"))
                            .as("the id-only lookup across the whole context picks up the failing wrapper's answer")
                            .isFalse();

                    KafkaCloudEventSink sink = context.getBean(KafkaCloudEventSink.class);
                    KafkaCloudEventBridgeFactory bridgeFactory = context.getBean(KafkaCloudEventBridgeFactory.class);

                    // No manual readinessSource(...) call: the zero-config path, exactly what
                    // DefaultKafkaCloudEventBridgeFactory.forGroup pre-seeds for every bridge. Fed with the healthy
                    // model's own live feed, ADR 133 decision 1's "a bridge feeds the live model, never the
                    // wrapper" shape, the same as every other bridge test in this module.
                    KafkaCloudEventBridge healthyBridge = bridgeFactory.forGroup("test-group-" + UUID.randomUUID(), healthyLiveFeed, healthyOutcomeChannel).build();
                    try {
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType(TestEvent.class.getName())
                                .build();
                        sink.publish(event);

                        CloudEvent delivered = healthyReceived.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered)
                                .as("the healthy model's own bridge must still consume, even though an unrelated " +
                                        "model's wrapper sharing its subscription id has permanently failed")
                                .isNotNull();
                    } finally {
                        healthyBridge.close();
                        healthyLiveFeed.shutdown();
                    }
                });
    }

    private static String kafkaVersion() {
        String version = System.getProperty("test.kafka.version");
        return version == null || version.isBlank() ? "4.1.0" : version.trim();
    }

    /** Its fully qualified name is the event type published above. */
    private record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentKafkaBroker
    static class EnabledConfiguration {
    }
}
