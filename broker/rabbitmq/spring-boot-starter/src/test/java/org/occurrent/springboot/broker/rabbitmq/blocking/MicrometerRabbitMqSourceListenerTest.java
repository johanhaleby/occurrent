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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The meters {@link MicrometerRabbitMqSourceListener} registers, named in ADR 171, against a {@link SimpleMeterRegistry}.
 */
class MicrometerRabbitMqSourceListenerTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerRabbitMqSourceListener listener = new MicrometerRabbitMqSourceListener(registry);

    @Test
    void delivered_counts_per_queue() {
        listener.delivered("orders");
        listener.delivered("orders");
        listener.delivered("invoices");

        assertThat(registry.get("occurrent.rabbitmq.source.delivered").tag("queue", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.get("occurrent.rabbitmq.source.delivered").tag("queue", "invoices").counter().count()).isEqualTo(1);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName()).containsOnly("occurrent.rabbitmq.source.delivered");
    }

    @Test
    void failed_counts_per_queue_with_and_without_a_failure() {
        listener.failed("orders", new IllegalStateException("boom"));
        listener.failed("orders", null);

        assertThat(registry.get("occurrent.rabbitmq.source.failed").tag("queue", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName()).containsOnly("occurrent.rabbitmq.source.failed");
    }

    @Test
    void parked_counts_per_queue() {
        listener.parked("orders");

        assertThat(registry.get("occurrent.rabbitmq.source.parked").tag("queue", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName()).containsOnly("occurrent.rabbitmq.source.parked");
    }

    @Test
    void redelivered_counts_per_queue() {
        listener.redelivered("orders");

        assertThat(registry.get("occurrent.rabbitmq.source.redelivered").tag("queue", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName()).containsOnly("occurrent.rabbitmq.source.redelivered");
    }

    @Test
    void queue_is_the_only_tag_on_every_meter() {
        listener.delivered("orders");
        listener.failed("orders", null);
        listener.parked("orders");
        listener.redelivered("orders");

        assertThat(registry.getMeters()).hasSize(4).allSatisfy(meter -> assertThat(meter.getId().getTags()).singleElement()
                .satisfies(tag -> {
                    assertThat(tag.getKey()).isEqualTo("queue");
                    assertThat(tag.getValue()).isEqualTo("orders");
                }));
    }
}
