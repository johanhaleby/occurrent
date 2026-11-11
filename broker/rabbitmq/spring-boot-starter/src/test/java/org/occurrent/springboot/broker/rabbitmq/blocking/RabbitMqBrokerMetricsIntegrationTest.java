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
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSinkListener;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSource;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSourceFactory;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.assertj.core.api.ThrowingConsumer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The Micrometer listeners are wired into the sink and the sources the starter builds, against a real broker, with
 * the {@link MeterRegistry} that Spring Boot's own metrics auto-configuration registers. This is the check that
 * {@code @ConditionalOnBean(MeterRegistry.class)} sees Boot's registry, which only a context activated with
 * {@code @EnableAutoConfiguration} next to {@code @EnableOccurrentRabbitMqBroker} can show.
 */
@Testcontainers
class RabbitMqBrokerMetricsIntegrationTest {

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
        try (Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(exchange, "topic", false, true, null);
        }
    }

    @AfterEach
    void closeConnection() throws Exception {
        connection.close();
    }

    @Test
    void a_publish_through_the_auto_configured_sink_is_counted_and_timed_in_the_registry_spring_boot_provides() throws Exception {
        declareQueueBoundTo(TestEvent.class.getName());

        contextRunner().run(context -> {
            context.getBean(RabbitMqCloudEventSink.class).publish(event("id-1"));

            MeterRegistry registry = context.getBean(MeterRegistry.class);
            assertThat(registry.get("occurrent.rabbitmq.sink.published").tag("exchange", exchange).counter().count()).isEqualTo(1);
            assertThat(registry.get("occurrent.rabbitmq.sink.publish").tag("exchange", exchange).timer().count()).isEqualTo(1);
            assertThat(registry.find("occurrent.rabbitmq.sink.unroutable").meter()).isNull();
            assertThat(registry.find("occurrent.rabbitmq.sink.failed").meter()).isNull();
        });
    }

    @Test
    void an_unroutable_publish_through_the_auto_configured_sink_is_counted_as_unroutable_and_not_as_failed() {
        contextRunner().withPropertyValues("occurrent.broker.rabbitmq.sink.on-unroutable=log-and-continue").run(context -> {
            context.getBean(RabbitMqCloudEventSink.class).publish(event("id-1"));

            MeterRegistry registry = context.getBean(MeterRegistry.class);
            assertThat(registry.get("occurrent.rabbitmq.sink.unroutable").tag("exchange", exchange).counter().count()).isEqualTo(1);
            assertThat(registry.find("occurrent.rabbitmq.sink.failed").meter()).isNull();
            assertThat(registry.find("occurrent.rabbitmq.sink.published").meter()).isNull();
        });
    }

    @Test
    void a_delivery_to_a_source_the_factory_builds_is_counted_for_its_queue_in_the_registry_spring_boot_provides() {
        contextRunner().run(context -> {
            String queue = "test-queue-" + UUID.randomUUID();
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
            model.subscribe("test-subscription", received::add);
            RabbitMqCloudEventSource source = context.getBean(RabbitMqCloudEventSourceFactory.class).forQueue(queue, model).build();
            try {
                context.getBean(RabbitMqCloudEventSink.class).publish(event("id-1"));

                MeterRegistry registry = context.getBean(MeterRegistry.class);
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(registry.get("occurrent.rabbitmq.source.delivered").tag("queue", queue).counter().count()).isEqualTo(1));
                assertThat(received).hasSize(1);
            } finally {
                source.close();
                model.shutdown();
            }
        });
    }

    @Test
    void a_delivery_to_a_domain_source_the_domain_factory_builds_is_counted_for_its_queue() {
        contextRunner().run(context -> {
            String queue = "test-domain-queue-" + UUID.randomUUID();
            List<TestDomainEvent> received = new CopyOnWriteArrayList<>();
            TestDomainEventConverter converter = new TestDomainEventConverter();
            DomainEventFeed<TestDomainEvent> feed =
                    new DomainEventFeed<>(new InMemoryEventStore(), converter, TestDomainEvent::id);
            feed.register("test-domain-projection", received::add, Filter.type(TestDomainEvent.class.getName()));
            feed.goLive("test-domain-projection");
            RabbitMqDomainEventSource<TestDomainEvent> source =
                    context.getBean(RabbitMqDomainEventSourceFactory.class).forQueue(queue, feed).build();
            try {
                CloudEvent event = converter.toCloudEvent(new TestDomainEvent("id-1"));
                context.getBean(RabbitMqCloudEventSink.class).publish(event);

                MeterRegistry registry = context.getBean(MeterRegistry.class);
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(registry.find("occurrent.rabbitmq.source.delivered").tag("queue", queue).counter())
                                .as("the delivered counter for the queue").isNotNull()
                                .satisfies(counter -> assertThat(counter.count()).isEqualTo(1)));
                assertThat(received).containsExactly(new TestDomainEvent("id-1"));
            } finally {
                source.close();
            }
        });
    }

    @Test
    void an_application_sink_listener_bean_receives_the_publish_and_the_micrometer_sink_counters_stay_unregistered() throws Exception {
        declareQueueBoundTo(TestEvent.class.getName());
        List<RabbitMqDestination> published = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        RabbitMqSinkListener applicationListener = new RabbitMqSinkListener() {
            @Override
            public void published(RabbitMqDestination destination, Duration publishTime) {
                published.add(destination);
                calls.incrementAndGet();
            }
        };

        contextRunner().withListener(applicationListener).run(context -> {
            context.getBean(RabbitMqCloudEventSink.class).publish(event("id-1"));

            assertThat(published).containsExactly(RabbitMqDestination.of(exchange, TestEvent.class.getName()));
            assertThat(context.getBean(MeterRegistry.class).find("occurrent.rabbitmq.sink.published").meter()).isNull();
        });
    }

    // Started through SpringApplication, as an application starts, so the deferred import selectors of
    // @EnableAutoConfiguration and @EnableOccurrentRabbitMqBroker are ordered as in production. No MeterRegistry is declared.
    private Application contextRunner() {
        return new Application();
    }

    private final class Application {
        private final List<String> properties = new java.util.ArrayList<>(List.of(
                "spring.main.banner-mode=off", "spring.jmx.enabled=false",
                "occurrent.broker.rabbitmq.exchange=" + exchange,
                "occurrent.broker.rabbitmq.source.poll-interval=100ms"));
        private RabbitMqSinkListener sinkListener;

        Application withPropertyValues(String property) {
            properties.add(property);
            return this;
        }

        Application withListener(RabbitMqSinkListener listener) {
            this.sinkListener = listener;
            return this;
        }

        void run(ThrowingConsumer<ConfigurableApplicationContext> assertions) {
            SpringApplicationBuilder builder = new SpringApplicationBuilder(BootApplication.class)
                    .web(WebApplicationType.NONE)
                    .properties(properties.toArray(String[]::new))
                    .initializers(ctx -> {
                        GenericApplicationContext context = (GenericApplicationContext) ctx;
                        // No inferred destroy method. This connection is this test's own, torn down in closeConnection()
                        context.registerBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""));
                        context.registerBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified);
                        if (sinkListener != null) {
                            context.registerBean(RabbitMqSinkListener.class, () -> sinkListener);
                        }
                    });
            try (ConfigurableApplicationContext context = builder.run()) {
                assertions.accept(context);
            }
        }
    }

    private void declareQueueBoundTo(String routingKey) throws Exception {
        try (Channel channel = connection.createChannel()) {
            String queue = channel.queueDeclare().getQueue();
            channel.queueBind(queue, exchange, routingKey);
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(TestEvent.class.getName())
                .build();
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    private record TestDomainEvent(String id) {
    }

    private static final class TestDomainEventConverter implements CloudEventConverter<TestDomainEvent> {
        @Override
        public CloudEvent toCloudEvent(TestDomainEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(domainEvent.id())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(getCloudEventType(TestDomainEvent.class))
                    .withData(domainEvent.id().getBytes(StandardCharsets.UTF_8))
                    .build();
        }

        @Override
        public TestDomainEvent toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = requireNonNull(cloudEvent.getData()).toBytes();
            return new TestDomainEvent(new String(data, StandardCharsets.UTF_8));
        }

        // The resolver round-trips the type through Class.forName, so the name has to be the class
        @Override
        public String getCloudEventType(Class<? extends TestDomainEvent> type) {
            return TestDomainEvent.class.getName();
        }
    }

    /** Its fully qualified name is the event type published above, see the comment there. */
    private record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentRabbitMqBroker
    static class BootApplication {
    }
}
