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

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceStatus;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * One health indicator per {@link RabbitMqCloudEventSink}, {@link RabbitMqCloudEventSource} and
 * {@link RabbitMqDomainEventSource} bean, named after the bean. See ADR 171.
 * <p>
 * The beans are looked up on every health call, and only those already created are listed, so a {@code @Lazy} sink
 * nothing has asked for yet is not created by a health check, and a source bean created after startup shows up on the
 * next call.
 */
final class RabbitMqBrokerHealthContributor implements CompositeHealthContributor {

    private final ConfigurableListableBeanFactory beanFactory;

    RabbitMqBrokerHealthContributor(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = requireNonNull(beanFactory, "beanFactory cannot be null");
    }

    @Override
    public @Nullable HealthContributor getContributor(String name) {
        return indicators().get(name);
    }

    @Override
    public Stream<Entry> stream() {
        return indicators().entrySet().stream().map(entry -> new Entry(entry.getKey(), entry.getValue()));
    }

    private Map<String, HealthIndicator> indicators() {
        Map<String, HealthIndicator> indicators = new LinkedHashMap<>();
        addCreated(indicators, RabbitMqCloudEventSink.class, sink -> () -> sinkHealth(sink));
        addCreated(indicators, RabbitMqCloudEventSource.class, source -> () -> sourceHealth(source.status(), source.isConnected()));
        addCreated(indicators, RabbitMqDomainEventSource.class, source -> () -> sourceHealth(source.status(), source.isConnected()));
        return indicators;
    }

    // No eager init and containsSingleton, so neither the type check nor the lookup creates a bean
    private <T> void addCreated(Map<String, HealthIndicator> indicators, Class<T> type, Function<T, HealthIndicator> indicator) {
        for (String name : beanFactory.getBeanNamesForType(type, false, false)) {
            if (beanFactory.containsSingleton(name)) {
                indicators.put(name, indicator.apply(beanFactory.getBean(name, type)));
            }
        }
    }

    private static Health sinkHealth(RabbitMqCloudEventSink sink) {
        boolean open = sink.isOpen();
        return (open ? Health.up() : Health.down()).withDetail("open", open).build();
    }

    private static Health sourceHealth(RabbitMqSourceStatus status, boolean connected) {
        boolean up = status == RabbitMqSourceStatus.OPEN && connected;
        return (up ? Health.up() : Health.down())
                .withDetail("status", status.name())
                .withDetail("connected", connected)
                .build();
    }
}
