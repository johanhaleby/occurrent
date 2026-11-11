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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqInboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqMessage;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqOutboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqUnroutableEventException;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the auto-configured sink and a factory-built source round-trip a real event through a real RabbitMQ,
 * the same loop {@code RabbitMqCloudEventLevelBrokerExampleTest} proves for the hand-wired bootstrap, driven here
 * by properties and the auto-configuration beans instead of manual builder calls.
 */
@Testcontainers
class RabbitMqBrokerAutoConfigurationIntegrationTest {

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
    void cloud_event_level_round_trip_through_the_auto_configured_sink_and_source() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                // No inferred destroy method: com.rabbitmq.client.Connection is itself Closeable, and this
                // connection is this test's own, torn down in closeConnection() below, not the context's to close
                // when the ApplicationContextRunner tears itself down at the end of run(...).
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=" + exchange,
                        "occurrent.broker.rabbitmq.source.poll-interval=100ms"
                )
                .run(context -> {
                    RabbitMqCloudEventSink sink = context.getBean(RabbitMqCloudEventSink.class);
                    RabbitMqCloudEventSourceFactory sourceFactory = context.getBean(RabbitMqCloudEventSourceFactory.class);

                    BlockingQueue<CloudEvent> received = new LinkedBlockingQueue<>();
                    PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
                    model.subscribe("test-subscription", received::add);

                    RabbitMqCloudEventSource source = sourceFactory.forQueue("test-queue-" + UUID.randomUUID(), model).build();
                    try {
                        // The type has to be a resolvable fully qualified class name, not an arbitrary string.
                        // ReflectionCloudEventTypeMapper.qualified() round-trips it through Class.forName(...) on
                        // every publish, and a name it cannot resolve throws immediately rather than delivering
                        // the event.
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType(TestEvent.class.getName())
                                .build();
                        sink.publish(event);

                        CloudEvent delivered = received.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered).isNotNull();
                        assertThat(delivered.getId()).isEqualTo(event.getId());
                        assertThat(delivered.getType()).isEqualTo(event.getType());
                    } finally {
                        source.close();
                        model.shutdown();
                    }
                });
    }

    /**
     * The sink keeps its unroutable policy private, so the property reaching it is only observable by what an
     * unroutable publish does. Nothing is bound to the exchange here, so every publish is returned by the broker.
     */
    @Test
    void the_auto_configured_sink_fails_an_unroutable_publish_by_default() {
        sinkContextRunner().run(context -> assertThatThrownBy(() -> context.getBean(RabbitMqCloudEventSink.class).publish(unroutableEvent()))
                .isInstanceOf(RabbitMqUnroutableEventException.class));
    }

    @Test
    void the_on_unroutable_property_makes_the_auto_configured_sink_log_and_continue_past_an_unroutable_publish() {
        sinkContextRunner()
                .withPropertyValues("occurrent.broker.rabbitmq.sink.on-unroutable=log-and-continue")
                .run(context -> assertThatCode(() -> context.getBean(RabbitMqCloudEventSink.class).publish(unroutableEvent())).doesNotThrowAnyException());
    }

    /**
     * The mapper is observable only through the message the sink publishes, so a plain {@code basicGet} reads what a
     * consumer that is not Occurrent would.
     */
    @Test
    void an_outbound_mapper_bean_is_used_by_the_auto_configured_sink() {
        sinkContextRunner()
                .withBean(RabbitMqOutboundMapper.class, () -> (cloudEvent, destination) -> new RabbitMqMessage(
                        new BasicProperties.Builder().messageId(cloudEvent.getId()).headers(Map.of("quantity", 3)).build(),
                        "{\"custom\":true}".getBytes(StandardCharsets.UTF_8)))
                .run(context -> {
                    String queue = declareQueueBoundTo(TestEvent.class.getName());

                    context.getBean(RabbitMqCloudEventSink.class).publish(unroutableEvent("id-1"));

                    try (Channel channel = connection.createChannel()) {
                        GetResponse response = channel.basicGet(queue, true);
                        assertThat(response).isNotNull();
                        assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"custom\":true}");
                        assertThat(response.getProps().getMessageId()).isEqualTo("id-1");
                        assertThat(response.getProps().getHeaders().get("quantity")).isEqualTo(3);
                        assertThat(response.getProps().getHeaders()).doesNotContainKey("cloudEvents_id");
                    }
                });
    }

    @Test
    void the_auto_configured_sink_publishes_the_cloudEvents_headers_when_there_is_no_outbound_mapper_bean() {
        sinkContextRunner().run(context -> {
            String queue = declareQueueBoundTo(TestEvent.class.getName());

            context.getBean(RabbitMqCloudEventSink.class).publish(unroutableEvent("id-1"));

            try (Channel channel = connection.createChannel()) {
                GetResponse response = channel.basicGet(queue, true);
                assertThat(response).isNotNull();
                assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
            }
        });
    }

    @Test
    void an_inbound_mapper_bean_is_used_by_a_source_the_factory_builds() {
        sinkContextRunner()
                .withBean(RabbitMqInboundMapper.class, () -> (properties, body) -> CloudEventBuilder.v1()
                        .withId(properties.getMessageId())
                        .withSource(URI.create("urn:plain-publisher"))
                        .withType(TestEvent.class.getName())
                        .withExtension("streamid", String.valueOf(properties.getHeaders().get("streamId")))
                        .withExtension("streamversion", ((Number) properties.getHeaders().get("streamVersion")).longValue())
                        .build())
                .run(context -> {
                    BlockingQueue<CloudEvent> received = new LinkedBlockingQueue<>();
                    PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
                    model.subscribe("test-subscription", received::add);

                    RabbitMqCloudEventSource source = context.getBean(RabbitMqCloudEventSourceFactory.class).forQueue("test-queue-" + UUID.randomUUID(), model).build();
                    try (Channel channel = connection.createChannel()) {
                        Map<String, Object> headers = new HashMap<>();
                        headers.put("streamId", "order-7");
                        headers.put("streamVersion", 3);
                        channel.basicPublish(exchange, "orders.placed", new BasicProperties.Builder().messageId("m-1").headers(headers).build(), new byte[0]);

                        CloudEvent delivered = received.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered).isNotNull();
                        assertThat(delivered.getExtension("streamid")).isEqualTo("order-7");
                        assertThat(delivered.getExtension("streamversion")).isEqualTo(3L);
                    } finally {
                        source.close();
                        model.shutdown();
                    }
                });
    }

    private String declareQueueBoundTo(String routingKey) throws Exception {
        try (Channel channel = connection.createChannel()) {
            String queue = channel.queueDeclare().getQueue();
            channel.queueBind(queue, exchange, routingKey);
            return queue;
        }
    }

    private ApplicationContextRunner sinkContextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=" + exchange);
    }

    private static CloudEvent unroutableEvent() {
        return unroutableEvent(UUID.randomUUID().toString());
    }

    private static CloudEvent unroutableEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(TestEvent.class.getName())
                .build();
    }

    /**
     * The starter wiring itself, not just {@code CatchupThenPushReadiness} in isolation: a source built through
     * {@link RabbitMqCloudEventSourceFactory#forQueue} with no manual {@code readinessSource(...)} call must still
     * defer to the exact {@link PushSubscriptionModel} it was built with, correlated by identity through the
     * {@code occurrentCatchupThenPushSubscriptionModelsByLiveFeed} bean a framework
     * {@code @Projection(source = PUSH)} or {@code @Saga(source = PUSH)} registration publishes. Registered here as
     * a plain map, standing in for that publisher, since this module has no compile-time dependency on the
     * framework autoconfigure module that does the real publishing.
     * <p>
     * Checked against the queue's own ready count, held at 1 the whole time the wrapper is parked, rather than
     * against whether the handler ever ran: {@code RoutingOutcome.DEFERRED} alone would already keep the handler
     * from running even with no {@code readinessSource} wired at all, refusing and redelivering every fetch, so
     * that alone cannot tell "never fetched because readinessSource gated it" apart from "fetched repeatedly and
     * refused every time". A ready count that never drops below 1 is what only the former produces.
     */
    @Test
    void the_factory_built_source_defers_to_a_catchup_then_push_wrapper_the_identity_registry_names_for_its_own_liveFeed() throws Exception {
        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        String queue = "test-queue-" + UUID.randomUUID();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=" + exchange,
                        "occurrent.broker.rabbitmq.source.poll-interval=100ms"
                )
                .run(context -> {
                    RabbitMqCloudEventSink sink = context.getBean(RabbitMqCloudEventSink.class);
                    RabbitMqCloudEventSourceFactory sourceFactory = context.getBean(RabbitMqCloudEventSourceFactory.class);

                    BlockingQueue<CloudEvent> received = new LinkedBlockingQueue<>();
                    PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing());
                    InMemoryEventStore store = new InMemoryEventStore();
                    store.write("s1", List.of(CloudEventBuilder.v1()
                            .withId("historical")
                            .withSource(URI.create("urn:occurrent:test"))
                            .withType(TestEvent.class.getName())
                            .build()));
                    CatchupThenPushSubscriptionModel wrapper = new CatchupThenPushSubscriptionModel(store, liveFeed, null);
                    ((org.springframework.context.support.GenericApplicationContext) context.getSourceApplicationContext())
                            .getBeanFactory().registerSingleton("occurrentCatchupThenPushSubscriptionModelsByLiveFeed",
                                    java.util.Map.of(liveFeed, wrapper));
                    wrapper.subscribe("test-subscription", null, StartAt.subscriptionModelDefault(), ce -> {
                        if (ce.getId().equals("historical")) {
                            replayEntered.countDown();
                            try {
                                assertThat(releaseReplay.await(5, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        } else {
                            received.add(ce);
                        }
                    });
                    assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

                    RabbitMqCloudEventSource source = sourceFactory.forQueue(queue, liveFeed).build();
                    try {
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType(TestEvent.class.getName())
                                .build();
                        sink.publish(event);

                        // Sampled repeatedly rather than once: a source that is fetching and refusing on every poll
                        // (readinessSource not actually gating it) still shows the queue empty at some individual
                        // instants too, since a fetched-then-nacked message spends part of each cycle "delivered,
                        // not yet ready again". Never dropping below 1 across many samples is what only a source
                        // that never fetched at all produces.
                        try (Channel adminChannel = connection.createChannel()) {
                            for (int i = 0; i < 20; i++) {
                                assertThat(adminChannel.queueDeclarePassive(queue).getMessageCount())
                                        .as("the auto-wired readinessSource must keep this source from fetching at all while parked")
                                        .isEqualTo(1);
                                Thread.sleep(20);
                            }
                        }

                        releaseReplay.countDown();

                        CloudEvent delivered = received.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered).isNotNull();
                        assertThat(delivered.getId()).isEqualTo(event.getId());
                    } finally {
                        source.close();
                        liveFeed.shutdown();
                    }
                });
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    /** Its fully qualified name is the event type published above, see the comment there. */
    private record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }
}
