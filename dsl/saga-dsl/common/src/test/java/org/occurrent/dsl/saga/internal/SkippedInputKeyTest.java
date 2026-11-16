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

package org.occurrent.dsl.saga.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SkippedInputKey.CloudEventKey;
import org.occurrent.dsl.saga.internal.SkippedInputKey.StreamKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("The key a quarantined saga instance records a skipped input under")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SkippedInputKeyTest {

    @Test
    void reads_a_stream_key_back_as_its_stream_id_and_version() {
        assertThat(SkippedInputKey.parse("order-1@7")).isEqualTo(new StreamKey("order-1", 7));
    }

    @Test
    void reads_a_stream_key_whose_stream_id_contains_an_at_sign_by_its_last_one() {
        assertThat(SkippedInputKey.parse("user@example.com@12")).isEqualTo(new StreamKey("user@example.com", 12));
    }

    @Test
    void reads_a_cloud_event_key_back_as_the_id_and_source_it_was_built_from() {
        String key = SkippedInputKey.cloudEventKey("e1", "urn:orders");

        assertThat(SkippedInputKey.parse(key)).isEqualTo(new CloudEventKey("e1", "urn:orders"));
    }

    @Test
    void writes_a_cloud_event_key_as_the_documented_literal_and_reads_it_back_as_the_same_id_and_source() {
        String key = SkippedInputKey.cloudEventKey("e1", "urn:orders");

        assertAll(
                () -> assertThat(key).isEqualTo("cloudevent:2:e1:urn:orders@cloudevent"),
                () -> assertThat(SkippedInputKey.parse("cloudevent:2:e1:urn:orders@cloudevent")).isEqualTo(new CloudEventKey("e1", "urn:orders"))
        );
    }

    @Test
    void refuses_a_cloud_event_key_whose_length_is_near_the_largest_int_instead_of_overflowing_the_bounds_check() {
        assertThatThrownBy(() -> SkippedInputKey.parse("cloudevent:2147483647:e1:urn:orders@cloudevent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("neither a stream key");
    }

    @Test
    void reads_a_cloud_event_key_back_when_the_id_contains_colons_and_at_signs() {
        String id = "tenant:42:order@7:x";
        String key = SkippedInputKey.cloudEventKey(id, "urn:a:b@c");

        assertThat(SkippedInputKey.parse(key)).isEqualTo(new CloudEventKey(id, "urn:a:b@c"));
    }

    @Test
    void reads_a_cloud_event_key_back_when_the_id_is_empty() {
        assertThat(SkippedInputKey.parse(SkippedInputKey.cloudEventKey("", "urn:orders"))).isEqualTo(new CloudEventKey("", "urn:orders"));
    }

    @Test
    void reads_a_stream_id_that_starts_like_a_cloud_event_key_as_the_stream_key_it_is() {
        assertThat(SkippedInputKey.parse("cloudevent:x@3")).isEqualTo(new StreamKey("cloudevent:x", 3));
    }

    @Test
    void is_the_redelivery_key_for_an_event_with_a_stream_id_and_version() {
        EventMeta meta = new EventMeta("order-1", 7L, 30L, null, "e1", "urn:orders");

        assertThat(meta.skipKey()).isEqualTo("order-1@7");
    }

    @Test
    void is_built_from_the_cloud_event_id_and_source_for_an_event_without_a_stream_version() {
        EventMeta meta = new EventMeta(null, null, 30L, null, "e1", "urn:orders");

        assertThat(SkippedInputKey.parse(meta.skipKey())).isEqualTo(new CloudEventKey("e1", "urn:orders"));
    }

    @Test
    void is_absent_for_an_input_with_neither_a_stream_version_nor_an_id_and_source() {
        assertThat(EventMeta.NONE.skipKey()).isNull();
    }

    @Test
    void refuses_a_key_of_neither_kind_and_names_both_formats() {
        assertThatThrownBy(() -> SkippedInputKey.parse("order-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streamId@streamVersion")
                .hasMessageContaining("cloudevent:<length of id>:<id>:<source>@cloudevent");
    }
}
