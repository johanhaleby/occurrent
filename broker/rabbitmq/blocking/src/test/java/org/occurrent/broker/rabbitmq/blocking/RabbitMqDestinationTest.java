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

package org.occurrent.broker.rabbitmq.blocking;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqDestinationTest {

    @Test
    void of_creates_a_destination_with_no_headers() {
        RabbitMqDestination destination = RabbitMqDestination.of("my-exchange", "my.routing.key");

        assertThat(destination.exchange()).isEqualTo("my-exchange");
        assertThat(destination.routingKey()).isEqualTo("my.routing.key");
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void withHeaders_returns_a_copy_with_the_new_headers_and_leaves_the_original_untouched() {
        RabbitMqDestination original = RabbitMqDestination.of("my-exchange", "my.routing.key");

        RabbitMqDestination withHeaders = original.withHeaders(Map.of("tenant", "acme"));

        assertThat(original.headers()).isEmpty();
        assertThat(withHeaders.headers()).containsExactly(Map.entry("tenant", "acme"));
        assertThat(withHeaders.exchange()).isEqualTo(original.exchange());
        assertThat(withHeaders.routingKey()).isEqualTo(original.routingKey());
    }

    @Test
    void headers_are_defensively_copied_so_mutating_the_caller_supplied_map_afterwards_has_no_effect() {
        Map<String, String> mutableHeaders = new HashMap<>();
        mutableHeaders.put("tenant", "acme");

        RabbitMqDestination destination = new RabbitMqDestination("my-exchange", "my.routing.key", mutableHeaders);
        mutableHeaders.put("tenant", "other");
        mutableHeaders.put("extra", "value");

        assertThat(destination.headers()).containsExactly(Map.entry("tenant", "acme"));
    }

    @Test
    void headers_returned_are_unmodifiable() {
        RabbitMqDestination destination = RabbitMqDestination.of("my-exchange", "my.routing.key").withHeaders(Map.of("tenant", "acme"));

        assertThatThrownBy(() -> destination.headers().put("more", "stuff")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void a_header_key_using_the_reserved_cloudEvents_prefix_is_refused_at_construction() {
        Map<String, String> headers = Map.of(RabbitMqCloudEventMapper.HEADER_PREFIX + "streamid", "should-not-be-allowed");

        assertThatThrownBy(() -> new RabbitMqDestination("my-exchange", "my.routing.key", headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RabbitMqCloudEventMapper.HEADER_PREFIX);
    }

    @Test
    void exchange_cannot_be_null() {
        assertThatThrownBy(() -> new RabbitMqDestination(null, "my.routing.key", Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void routingKey_cannot_be_null() {
        assertThatThrownBy(() -> new RabbitMqDestination("my-exchange", null, Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void headers_cannot_be_null() {
        assertThatThrownBy(() -> new RabbitMqDestination("my-exchange", "my.routing.key", null))
                .isInstanceOf(NullPointerException.class);
    }
}
