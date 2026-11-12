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

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSource;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.autoconfigure.registry.HealthContributorRegistryAutoConfiguration;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.health.registry.HealthContributorRegistry;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The {@code rabbitMqBroker} health contributor, without a broker. Sources are built over a mocked connection and
 * channel, whose {@code isOpen()} the test controls, since the contributor only asks them whether they work. A
 * sink nothing has asked for is never created by a health call, which is the point of the {@code @Lazy} sink.
 */
class OccurrentRabbitMqAutoConfigurationHealthTest {

    private static final String CONTRIBUTOR_BEAN = "rabbitMqBrokerHealthContributor";

    private final AtomicBoolean connectionOpen = new AtomicBoolean(true);
    private final AtomicReference<Channel> channel = new AtomicReference<>();

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
            .withUserConfiguration(EnabledConfiguration.class)
            .withBean(Connection.class, this::connection)
            .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
            .withPropertyValues("occurrent.broker.rabbitmq.exchange=orders");

    @Test
    void the_contributor_is_registered_as_rabbitMqBroker_and_lists_nothing_before_any_sink_or_source_exists() {
        withRegistry().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(HealthContributorRegistry.class).getContributor("rabbitMqBroker"))
                    .isSameAs(context.getBean("rabbitMqBrokerHealthContributor"));
            assertThat(contributor(context).stream()).isEmpty();
        });
    }

    @Test
    void an_application_health_indicator_bean_named_rabbitMqBroker_replaces_the_contributor() {
        HealthIndicator applicationIndicator = () -> Health.up().build();

        withRegistry().withBean("rabbitMqBroker", HealthIndicator.class, () -> applicationIndicator).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
            assertThat(context.getBean(HealthContributorRegistry.class).getContributor("rabbitMqBroker"))
                    .isSameAs(applicationIndicator);
        });
    }

    @Test
    void an_application_health_indicator_bean_named_rabbitMqBrokerHealthIndicator_replaces_the_contributor() {
        HealthIndicator applicationIndicator = () -> Health.up().build();

        withRegistry().withBean("rabbitMqBrokerHealthIndicator", HealthIndicator.class, () -> applicationIndicator)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
                    assertThat(context.getBean(HealthContributorRegistry.class).getContributor("rabbitMqBroker"))
                            .isSameAs(applicationIndicator);
                });
    }

    @Test
    void a_cloud_event_source_bean_is_listed_UP_and_DOWN_after_close() {
        contextRunner.withUserConfiguration(CloudEventSourceConfiguration.class).run(context -> {
            assertThat(indicator(context, "ordersSource").health().getStatus()).isEqualTo(Status.UP);

            context.getBean(RabbitMqCloudEventSource.class).close();

            Health health = indicator(context, "ordersSource").health();
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "CLOSED");
        });
    }

    @Test
    void a_domain_event_source_bean_is_listed_UP_and_DOWN_after_close() {
        contextRunner.withUserConfiguration(DomainEventSourceConfiguration.class).run(context -> {
            assertThat(indicator(context, "ordersDomainSource").health().getStatus()).isEqualTo(Status.UP);

            context.getBean(RabbitMqDomainEventSource.class).close();

            Health health = indicator(context, "ordersDomainSource").health();
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "CLOSED");
        });
    }

    @Test
    void a_source_that_stopped_for_good_is_DOWN_with_status_STOPPED_although_its_connection_is_open() {
        contextRunner.withUserConfiguration(CloudEventSourceConfiguration.class).run(context -> {
            RabbitMqCloudEventSource source = context.getBean(RabbitMqCloudEventSource.class);

            closeChannelForGoodAndPoll(source);

            Health health = indicator(context, "ordersSource").health();
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "STOPPED").containsEntry("connected", true);
        });
    }

    @Test
    void a_source_whose_connection_is_closed_is_DOWN_although_it_is_not_closed() {
        contextRunner.withUserConfiguration(CloudEventSourceConfiguration.class).run(context -> {
            connectionOpen.set(false);

            Health health = indicator(context, "ordersSource").health();
            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("status", "OPEN").containsEntry("connected", false);
        });
    }

    @Test
    void a_health_call_never_creates_the_lazy_sink_and_the_sink_is_listed_once_something_has_asked_for_it() {
        contextRunner.run(context -> {
            assertThat(contributor(context).stream()).isEmpty();
            assertThat(contributor(context).getContributor("occurrentRabbitMqCloudEventSink")).isNull();
            assertThat(context.getBeanFactory().containsSingleton("occurrentRabbitMqCloudEventSink")).as("the sink singleton exists after health calls").isFalse();

            RabbitMqCloudEventSink sink = context.getBean(RabbitMqCloudEventSink.class);

            assertThat(contributor(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("occurrentRabbitMqCloudEventSink");
            assertThat(indicator(context, "occurrentRabbitMqCloudEventSink").health().getStatus()).isEqualTo(Status.UP);

            sink.close();

            assertThat(indicator(context, "occurrentRabbitMqCloudEventSink").health().getStatus()).isEqualTo(Status.DOWN);
        });
    }

    @Test
    void the_contributor_is_absent_when_management_health_rabbitmqbroker_enabled_is_false() {
        contextRunner.withPropertyValues("management.health.rabbitmqbroker.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_contributor_is_absent_when_the_kebab_case_management_health_rabbit_mq_broker_enabled_is_false() {
        contextRunner.withPropertyValues("management.health.rabbit-mq-broker.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_contributor_is_absent_when_the_camel_case_management_health_rabbitMqBroker_enabled_is_false() {
        contextRunner.withPropertyValues("management.health.rabbitMqBroker.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_contributor_is_absent_when_the_environment_variable_MANAGEMENT_HEALTH_RABBITMQBROKER_ENABLED_is_false() {
        contextRunner.withInitializer(context -> TestPropertyValues.of("MANAGEMENT_HEALTH_RABBITMQBROKER_ENABLED=false")
                .applyTo(context.getEnvironment(), TestPropertyValues.Type.SYSTEM_ENVIRONMENT)).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_contributor_is_absent_when_management_health_defaults_enabled_is_false() {
        contextRunner.withPropertyValues("management.health.defaults.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_contributor_is_present_when_defaults_are_disabled_but_rabbitmqbroker_is_enabled() {
        contextRunner.withPropertyValues("management.health.defaults.enabled=false",
                "management.health.rabbitmqbroker.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean(CONTRIBUTOR_BEAN);
        });
    }

    @Test
    void the_context_starts_without_a_health_contributor_when_spring_boot_health_is_not_on_the_classpath() {
        contextRunner.withClassLoader(new FilteredClassLoader("org.springframework.boot.health")).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CONTRIBUTOR_BEAN);
            assertThat(context).hasSingleBean(RabbitMqCloudEventSink.class);
        });
    }

    // Boot's registry, which names each contributor and refuses two with one name
    private ApplicationContextRunner withRegistry() {
        return contextRunner.withConfiguration(AutoConfigurations.of(HealthContributorRegistryAutoConfiguration.class));
    }

    private static CompositeHealthContributor contributor(AssertableApplicationContext context) {
        return context.getBean(CONTRIBUTOR_BEAN, CompositeHealthContributor.class);
    }

    private static HealthIndicator indicator(AssertableApplicationContext context, String name) {
        HealthContributor contributor = contributor(context).getContributor(name);
        assertThat(contributor).as("a health indicator named " + name).isInstanceOf(HealthIndicator.class);
        return (HealthIndicator) contributor;
    }

    // The poll runs once a minute in these sources, so the test runs it itself
    private void closeChannelForGoodAndPoll(RabbitMqCloudEventSource source) throws Exception {
        ArgumentCaptor<ShutdownListener> listeners = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel.get(), atLeastOnce()).addShutdownListener(listeners.capture());
        listeners.getAllValues().forEach(listener ->
                listener.shutdownCompleted(new ShutdownSignalException(false, false, null, mock(Channel.class))));
        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }

    private Connection connection() {
        try {
            Connection connection = mock(Connection.class);
            Channel channel = mock(Channel.class);
            this.channel.set(channel);
            when(channel.getConnection()).thenReturn(connection);
            when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");
            when(connection.openChannel()).thenReturn(Optional.of(channel));
            when(connection.isOpen()).thenAnswer(invocation -> connectionOpen.get());
            return connection;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class CloudEventSourceConfiguration {
        @Bean
        RabbitMqCloudEventSource ordersSource(Connection connection) {
            PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
            model.subscribe("sub", cloudEvent -> { });
            return RabbitMqCloudEventSource.builder(connection, model, "orders-queue").declareTopology(false).pollInterval(Duration.ofMinutes(1)).build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DomainEventSourceConfiguration {
        @Bean
        @SuppressWarnings("unchecked")
        RabbitMqDomainEventSource<Object> ordersDomainSource(Connection connection) {
            DomainEventFeed<Object> feed = mock(DomainEventFeed.class);
            when(feed.hasProjection()).thenReturn(true);
            when(feed.isReadyForLiveDelivery()).thenReturn(true);
            return RabbitMqDomainEventSource.builder(connection, feed, "orders-queue").declareTopology(false).pollInterval(Duration.ofMinutes(1)).build();
        }
    }
}
