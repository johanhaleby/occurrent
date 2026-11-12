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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The meters {@link MicrometerRabbitMqSinkListener} registers, named in ADR 171, against a {@link SimpleMeterRegistry}.
 */
class MicrometerRabbitMqSinkListenerTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerRabbitMqSinkListener listener = new MicrometerRabbitMqSinkListener(registry);

    @Test
    void published_counts_per_exchange_and_records_the_publish_time_in_a_timer_with_the_same_exchange_tag() {
        listener.published(RabbitMqDestination.of("orders", "order.placed"), Duration.ofMillis(150));
        listener.published(RabbitMqDestination.of("orders", "order.shipped"), Duration.ofMillis(50));
        listener.published(RabbitMqDestination.of("invoices", "invoice.sent"), Duration.ofMillis(10));

        assertThat(registry.get("occurrent.rabbitmq.sink.published").tag("exchange", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.get("occurrent.rabbitmq.sink.published").tag("exchange", "invoices").counter().count()).isEqualTo(1);
        assertThat(registry.get("occurrent.rabbitmq.sink.publish").tag("exchange", "orders").timer().count()).isEqualTo(2);
        assertThat(registry.get("occurrent.rabbitmq.sink.publish").tag("exchange", "orders").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(200);
        assertThat(registry.get("occurrent.rabbitmq.sink.publish").tag("exchange", "invoices").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10);
    }

    @Test
    void the_routing_key_is_not_a_tag_on_any_meter() {
        listener.published(RabbitMqDestination.of("orders", "order.placed"), Duration.ofMillis(1));
        listener.unroutable(RabbitMqDestination.of("orders", "order.placed"));
        listener.failed(RabbitMqDestination.of("orders", "order.placed"), new IllegalStateException());

        assertThat(registry.getMeters())
                .filteredOn(meter -> !meter.getId().getName().equals("occurrent.rabbitmq.sink.unresolved"))
                .hasSize(4).allSatisfy(meter -> assertThat(meter.getId().getTags()).singleElement()
                .satisfies(tag -> {
                    assertThat(tag.getKey()).isEqualTo("exchange");
                    assertThat(tag.getValue()).isEqualTo("orders");
                }));
    }

    @Test
    void unroutable_counts_per_exchange_and_touches_no_other_meter() {
        listener.unroutable(RabbitMqDestination.of("orders", "order.placed"));
        listener.unroutable(RabbitMqDestination.of("orders", "order.shipped"));

        assertThat(registry.get("occurrent.rabbitmq.sink.unroutable").tag("exchange", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName())
                .containsExactlyInAnyOrder("occurrent.rabbitmq.sink.unroutable", "occurrent.rabbitmq.sink.unresolved");
        assertThat(registry.get("occurrent.rabbitmq.sink.unresolved").counter().count()).isZero();
    }

    @Test
    void failed_counts_per_exchange_and_touches_no_other_meter() {
        listener.failed(RabbitMqDestination.of("orders", "order.placed"), new IllegalStateException("boom"));

        assertThat(registry.get("occurrent.rabbitmq.sink.failed").tag("exchange", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName())
                .containsExactlyInAnyOrder("occurrent.rabbitmq.sink.failed", "occurrent.rabbitmq.sink.unresolved");
        assertThat(registry.get("occurrent.rabbitmq.sink.unresolved").counter().count()).isZero();
    }

    @Test
    void unresolved_counts_in_a_meter_without_tags_and_touches_no_other_meter() {
        listener.unresolved(new IllegalStateException("the resolver returned null"));
        listener.unresolved(new IllegalArgumentException("no destination for this event"));

        Counter unresolved = registry.get("occurrent.rabbitmq.sink.unresolved").counter();
        assertThat(unresolved.count()).isEqualTo(2);
        assertThat(unresolved.getId().getTags()).isEmpty();
        assertThat(registry.getMeters()).extracting(meter -> meter.getId().getName())
                .containsExactly("occurrent.rabbitmq.sink.unresolved");
    }
}
