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

import com.rabbitmq.client.Connection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSinkListener;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceListener;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Which {@link RabbitMqSinkListener} and {@link RabbitMqSourceListener} beans the starter registers, without a
 * broker. {@code BootApplication} is started through {@code SpringApplication} and activated the way an application activates the starter, by
 * {@code @EnableAutoConfiguration} next to {@code @EnableOccurrentRabbitMqBroker}, so the {@link MeterRegistry} in
 * the context is the one Spring Boot's own metrics auto-configuration registers, not a bean this test declares.
 */
class OccurrentRabbitMqAutoConfigurationMetricsWiringTest {

    @Test
    void the_micrometer_listeners_are_registered_when_spring_boots_metrics_auto_configuration_provides_the_registry() {
        try (ConfigurableApplicationContext context = application(true).run()) {
            assertThat(context.getBeanFactory().containsBeanDefinition("simpleMeterRegistry")).as("the registry is Spring Boot's, not declared by this test").isTrue();
            assertThat(context.getBean(MeterRegistry.class)).isInstanceOf(SimpleMeterRegistry.class);
            assertThat(context.getBean(RabbitMqSinkListener.class)).isInstanceOf(MicrometerRabbitMqSinkListener.class);
            assertThat(context.getBean(RabbitMqSourceListener.class)).isInstanceOf(MicrometerRabbitMqSourceListener.class);
        }
    }

    @Test
    void no_listener_beans_are_registered_without_a_meter_registry() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(Connection.class, () -> mock(Connection.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(MeterRegistry.class);
                    assertThat(context).doesNotHaveBean(RabbitMqSinkListener.class);
                    assertThat(context).doesNotHaveBean(RabbitMqSourceListener.class);
                });
    }

    @Test
    void no_listener_beans_are_registered_without_a_connection_even_with_a_meter_registry() {
        try (ConfigurableApplicationContext context = application(false).run()) {
            assertThat(context.getBeansOfType(MeterRegistry.class)).hasSize(1);
            assertThat(context.getBeansOfType(RabbitMqSinkListener.class)).isEmpty();
            assertThat(context.getBeansOfType(RabbitMqSourceListener.class)).isEmpty();
        }
    }

    @Test
    void application_listener_beans_replace_the_micrometer_ones() {
        RabbitMqSinkListener sinkListener = new RabbitMqSinkListener() {
        };
        RabbitMqSourceListener sourceListener = new RabbitMqSourceListener() {
        };
        try (ConfigurableApplicationContext context = application(true).initializers(ctx -> {
            ((GenericApplicationContext) ctx).registerBean(RabbitMqSinkListener.class, () -> sinkListener);
            ((GenericApplicationContext) ctx).registerBean(RabbitMqSourceListener.class, () -> sourceListener);
        }).run()) {
            assertThat(context.getBeansOfType(RabbitMqSinkListener.class)).hasSize(1);
            assertThat(context.getBeansOfType(RabbitMqSourceListener.class)).hasSize(1);
            assertThat(context.getBean(RabbitMqSinkListener.class)).isSameAs(sinkListener);
            assertThat(context.getBean(RabbitMqSourceListener.class)).isSameAs(sourceListener);
        }
    }

    // Started the way an application starts, through SpringApplication, so the deferred import selectors of
    // @EnableAutoConfiguration and @EnableOccurrentRabbitMqBroker are ordered as they are in production. The test
    // declares no MeterRegistry.
    private static SpringApplicationBuilder application(boolean withConnection) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(BootApplication.class)
                .web(WebApplicationType.NONE)
                .properties("spring.main.banner-mode=off", "spring.jmx.enabled=false");
        if (withConnection) {
            builder.initializers(ctx -> ((GenericApplicationContext) ctx).registerBean(Connection.class, () -> mock(Connection.class)));
        }
        return builder;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentRabbitMqBroker
    static class BootApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }
}
