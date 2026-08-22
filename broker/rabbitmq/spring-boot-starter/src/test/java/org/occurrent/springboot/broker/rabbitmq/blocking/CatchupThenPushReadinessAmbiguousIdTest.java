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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
 * {@link CatchupThenPushReadiness#isReady(org.springframework.context.ApplicationContext, String)} picks a wrapper
 * bean by subscription id alone, across every {@link CatchupThenPushSubscriptionModel} bean in the whole
 * application context, with no correlation to which bridge (and which underlying live feed) is actually asking.
 * ADR 102 allows two independent push models to subscribe under the same id, so that id-only lookup can let one
 * bridge's readiness answer come from a completely unrelated model's wrapper: if the first one found has a
 * permanently failed catch-up, its {@code isReadyForLiveDelivery(id)} answers {@code false} forever, and a
 * healthy sibling model's bridge built with the same lookup would inherit that answer and never consume, even
 * though its own model has nothing wrong with it.
 * {@link DefaultRabbitMqCloudEventBridgeFactory#forQueue} avoids this by asking
 * {@link CatchupThenPushReadiness#memoized(org.springframework.context.ApplicationContext, org.occurrent.subscription.push.blocking.PushSubscriptionModel)}
 * instead, which correlates by identity through the framework module's published wrapper registry.
 * <p>
 * Registers two {@code CatchupThenPushSubscriptionModel} beans that both subscribe as {@code "orders"}, the first
 * with a permanently failed catch-up, the second healthy, confirms the id-only lookup above is indeed ambiguous
 * for this setup, then builds the healthy model's bridge with no manual {@code readinessSource(...)} call (the
 * zero-config path) and asserts the healthy bridge still consumes.
 */
@Testcontainers
class CatchupThenPushReadinessAmbiguousIdTest {

    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion()).withReuse(true);

    private Connection connection;
    private String exchange;

    @BeforeEach
    void openConnectionAndScratchExchange() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connection = connectionFactory.newConnection();
        exchange = "test-exchange-" + UUID.randomUUID();
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDeclare(exchange, "topic", false, true, null);
        } finally {
            channel.close();
        }
    }

    @AfterEach
    void closeConnection() throws Exception {
        connection.close();
    }

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
        SubscriptionHandle failingSubscription = failingWrapper.subscribe("orders", null, StartAt.subscriptionModelDefault(), ce -> {
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

        String healthyQueue = "test-queue-healthy-" + UUID.randomUUID();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=" + exchange,
                        "occurrent.broker.rabbitmq.bridge.poll-interval=100ms"
                )
                .run(context -> {
                    GenericApplicationContext springContext = (GenericApplicationContext) context.getSourceApplicationContext();
                    // Registered in this order deliberately: the failing wrapper first, so
                    // ApplicationContext.getBeansOfType(...) is very likely to hand it back before the healthy one,
                    // exactly the ambiguity CLAIM 4 describes. Bean names are irrelevant, CatchupThenPushReadiness
                    // looks these up by type, not by the "catchupThenPushSubscriptionModel-<id>" convention.
                    springContext.getBeanFactory().registerSingleton("failingWrapper", failingWrapper);
                    springContext.getBeanFactory().registerSingleton("healthyWrapper", healthyWrapper);

                    // The root cause, checked directly: the shared, id-only lookup answers false for "orders" as
                    // long as the failing wrapper is found first, regardless of which model is actually asking.
                    assertThat(CatchupThenPushReadiness.isReady(springContext, "orders"))
                            .as("the id-only lookup across the whole context picks up the failing wrapper's answer")
                            .isFalse();

                    RabbitMqCloudEventSink sink = context.getBean(RabbitMqCloudEventSink.class);
                    RabbitMqCloudEventBridgeFactory bridgeFactory = context.getBean(RabbitMqCloudEventBridgeFactory.class);

                    // No manual readinessSource(...) call: the zero-config path, exactly what
                    // DefaultRabbitMqCloudEventBridgeFactory.forQueue pre-seeds for every bridge. Fed with the
                    // healthy model's own live feed, ADR 133 decision 1's "a bridge feeds the live model, never
                    // the wrapper" shape, the same as every other bridge test in this module.
                    RabbitMqCloudEventBridge healthyBridge = bridgeFactory.forQueue(healthyQueue, healthyLiveFeed, healthyOutcomeChannel).build();
                    try {
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType(TestEvent.class.getName())
                                .build();
                        sink.publish(event);

                        CloudEvent delivered = healthyReceived.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered)
                                .as("the healthy model's own bridge must still consume, even though an unrelated "
                                        + "model's wrapper sharing its subscription id has permanently failed")
                                .isNotNull();
                    } finally {
                        healthyBridge.close();
                        healthyLiveFeed.shutdown();
                    }
                });
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    /** Its fully qualified name is the event type published above. */
    private record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }
}
