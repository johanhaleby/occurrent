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

package org.occurrent.broker.kafka.blocking;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaDestinationTest {

    @Test
    void of_with_a_key_creates_a_destination_with_no_headers() {
        KafkaDestination destination = KafkaDestination.of("my-topic", "stream-1");

        assertThat(destination.topic()).isEqualTo("my-topic");
        assertThat(destination.key()).isEqualTo("stream-1");
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void of_with_no_key_creates_a_destination_with_a_null_key_and_no_headers() {
        KafkaDestination destination = KafkaDestination.of("my-topic");

        assertThat(destination.topic()).isEqualTo("my-topic");
        assertThat(destination.key()).isNull();
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void withHeaders_returns_a_copy_with_the_new_headers_and_leaves_the_original_untouched() {
        KafkaDestination original = KafkaDestination.of("my-topic", "stream-1");

        KafkaDestination withHeaders = original.withHeaders(Map.of("tenant", "acme"));

        assertThat(original.headers()).isEmpty();
        assertThat(withHeaders.headers()).containsExactly(Map.entry("tenant", "acme"));
        assertThat(withHeaders.topic()).isEqualTo(original.topic());
        assertThat(withHeaders.key()).isEqualTo(original.key());
    }

    @Test
    void headers_are_defensively_copied_so_mutating_the_caller_supplied_map_afterwards_has_no_effect() {
        Map<String, String> mutableHeaders = new HashMap<>();
        mutableHeaders.put("tenant", "acme");

        KafkaDestination destination = new KafkaDestination("my-topic", "stream-1", mutableHeaders, false);
        mutableHeaders.put("tenant", "other");
        mutableHeaders.put("extra", "value");

        assertThat(destination.headers()).containsExactly(Map.entry("tenant", "acme"));
    }

    @Test
    void headers_returned_are_unmodifiable() {
        KafkaDestination destination = KafkaDestination.of("my-topic", "stream-1").withHeaders(Map.of("tenant", "acme"));

        assertThatThrownBy(() -> destination.headers().put("more", "stuff")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void a_header_key_using_the_reserved_ce_prefix_is_refused_at_construction() {
        Map<String, String> headers = Map.of(KafkaDestination.HEADER_PREFIX + "streamid", "should-not-be-allowed");

        assertThatThrownBy(() -> new KafkaDestination("my-topic", "stream-1", headers, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(KafkaDestination.HEADER_PREFIX);
    }

    @Test
    void a_header_key_equal_to_the_reserved_content_type_header_is_refused_at_construction() {
        Map<String, String> headers = Map.of(KafkaDestination.CONTENT_TYPE_HEADER, "text/plain");

        assertThatThrownBy(() -> new KafkaDestination("my-topic", "stream-1", headers, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(KafkaDestination.CONTENT_TYPE_HEADER);
    }

    @Test
    void topic_cannot_be_null() {
        assertThatThrownBy(() -> new KafkaDestination(null, "stream-1", Map.of(), false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void key_may_be_null() {
        KafkaDestination destination = new KafkaDestination("my-topic", null, Map.of(), false);

        assertThat(destination.key()).isNull();
    }

    @Test
    void headers_cannot_be_null() {
        assertThatThrownBy(() -> new KafkaDestination("my-topic", "stream-1", null, false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void of_defaults_topicIsPattern_to_false() {
        assertThat(KafkaDestination.of("my-topic").topicIsPattern()).isFalse();
        assertThat(KafkaDestination.of("my-topic", "stream-1").topicIsPattern()).isFalse();
    }

    @Test
    void withHeaders_carries_topicIsPattern_over_unchanged() {
        KafkaDestination pattern = KafkaDestination.ofPattern("prefix-.*");

        KafkaDestination withHeaders = pattern.withHeaders(Map.of());

        assertThat(withHeaders.topicIsPattern()).isTrue();
    }

    @Test
    void ofPattern_creates_a_pattern_typed_destination_with_no_key_and_no_headers() {
        KafkaDestination destination = KafkaDestination.ofPattern("prefix-.*");

        assertThat(destination.topic()).isEqualTo("prefix-.*");
        assertThat(destination.topicIsPattern()).isTrue();
        assertThat(destination.key()).isNull();
        assertThat(destination.headers()).isEmpty();
    }
}
