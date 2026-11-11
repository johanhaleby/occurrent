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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSinkListener;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Counts what a {@code RabbitMqCloudEventSink} publishes, per exchange. The routing key is not a tag, since a
 * routing key derived from the event type has one value per type. See ADR 171.
 */
final class MicrometerRabbitMqSinkListener implements RabbitMqSinkListener {

    static final String PUBLISHED = "occurrent.rabbitmq.sink.published";
    static final String UNROUTABLE = "occurrent.rabbitmq.sink.unroutable";
    static final String FAILED = "occurrent.rabbitmq.sink.failed";
    static final String PUBLISH = "occurrent.rabbitmq.sink.publish";
    static final String EXCHANGE_TAG = "exchange";

    private final MeterRegistry registry;
    private final Map<String, Counter> published = new ConcurrentHashMap<>();
    private final Map<String, Counter> unroutable = new ConcurrentHashMap<>();
    private final Map<String, Counter> failed = new ConcurrentHashMap<>();
    private final Map<String, Timer> publishTime = new ConcurrentHashMap<>();

    MicrometerRabbitMqSinkListener(MeterRegistry registry) {
        this.registry = requireNonNull(registry, MeterRegistry.class.getSimpleName() + " cannot be null");
    }

    @Override
    public void published(RabbitMqDestination destination, Duration publishTime) {
        counter(published, PUBLISHED, "Publishes the broker confirmed and routed", destination).increment();
        this.publishTime.computeIfAbsent(destination.exchange(), exchange -> Timer.builder(PUBLISH)
                        .description("Time from the start of a publish attempt to the broker's confirm")
                        .tag(EXCHANGE_TAG, exchange)
                        .register(registry))
                .record(publishTime);
    }

    @Override
    public void unroutable(RabbitMqDestination destination) {
        counter(unroutable, UNROUTABLE, "Publishes the broker returned because no queue was bound for them", destination).increment();
    }

    @Override
    public void failed(RabbitMqDestination destination, Throwable failure) {
        counter(failed, FAILED, "Publish attempts that failed for a reason other than an unroutable message", destination).increment();
    }

    private Counter counter(Map<String, Counter> counters, String name, String description, RabbitMqDestination destination) {
        return counters.computeIfAbsent(destination.exchange(), exchange -> Counter.builder(name)
                .description(description)
                .tag(EXCHANGE_TAG, exchange)
                .register(registry));
    }
}
