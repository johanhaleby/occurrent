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
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Counts what happens to the deliveries of a {@code RabbitMqCloudEventSource} or a {@code RabbitMqDomainEventSource},
 * per queue. See ADR 171.
 */
final class MicrometerRabbitMqSourceListener implements RabbitMqSourceListener {

    static final String DELIVERED = "occurrent.rabbitmq.source.delivered";
    static final String FAILED = "occurrent.rabbitmq.source.failed";
    static final String PARKED = "occurrent.rabbitmq.source.parked";
    static final String REDELIVERED = "occurrent.rabbitmq.source.redelivered";
    static final String QUEUE_TAG = "queue";

    private final MeterRegistry registry;
    private final Map<String, Counter> delivered = new ConcurrentHashMap<>();
    private final Map<String, Counter> failed = new ConcurrentHashMap<>();
    private final Map<String, Counter> parked = new ConcurrentHashMap<>();
    private final Map<String, Counter> redelivered = new ConcurrentHashMap<>();

    MicrometerRabbitMqSourceListener(MeterRegistry registry) {
        this.registry = requireNonNull(registry, MeterRegistry.class.getSimpleName() + " cannot be null");
    }

    @Override
    public void delivered(String queue) {
        counter(delivered, DELIVERED, "Deliveries the source acknowledged after its subscriber took or filtered them", queue).increment();
    }

    @Override
    public void failed(String queue, @Nullable Throwable failure) {
        counter(failed, FAILED, "Deliveries that went to the delivery failure policy", queue).increment();
    }

    @Override
    public void parked(String queue) {
        counter(parked, PARKED, "Failed deliveries the source parked and acknowledged", queue).increment();
    }

    @Override
    public void redelivered(String queue) {
        counter(redelivered, REDELIVERED, "Failed deliveries the source handed back to the queue", queue).increment();
    }

    private Counter counter(Map<String, Counter> counters, String name, String description, String queue) {
        return counters.computeIfAbsent(queue, __ -> Counter.builder(name)
                .description(description)
                .tag(QUEUE_TAG, queue)
                .register(registry));
    }
}
