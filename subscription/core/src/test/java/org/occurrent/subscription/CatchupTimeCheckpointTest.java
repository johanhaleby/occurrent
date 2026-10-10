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

package org.occurrent.subscription;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupTimeCheckpointTest {

    private static final String TIME = "2026-01-01T10:00:05.123Z";
    private static final String ORIGIN = "2026-01-01T10:00:00Z";

    // The form of a Mongo resume token checkpoint's string, which holds colons, commas and quotes
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("{\"resumeToken\": {\"_data\": \"8266F2\"}}");

    @Test
    void the_string_form_puts_the_time_first_and_the_live_start_last_and_verbatim() {
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN);

        assertThat(checkpoint.asString()).isEqualTo("2026-01-01T10:00:05.123Z;origin:2026-01-01T10:00:00Z;liveFrom:{\"resumeToken\": {\"_data\": \"8266F2\"}}");
        assertThat(checkpoint).hasToString(checkpoint.asString());
        assertThat(checkpoint.time()).isEqualTo(TIME);
        assertThat(checkpoint.replayOrigin()).isEqualTo(ORIGIN);
        assertThat(checkpoint.liveFrom()).isSameAs(LIVE_START);
    }

    @Test
    void parse_reads_the_string_form_back_as_it_comes_out_of_a_storage_that_keeps_strings() {
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN);

        CatchupTimeCheckpoint parsed = CatchupTimeCheckpoint.parse(new StringBasedCheckpoint(checkpoint.asString()));

        assertThat(parsed).isEqualTo(checkpoint);
        assertThat(parsed.time()).isEqualTo(TIME);
        assertThat(parsed.replayOrigin()).isEqualTo(ORIGIN);
        assertThat(parsed.liveFrom()).isInstanceOf(StringBasedCheckpoint.class);
        assertThat(parsed.liveFrom().asString()).isEqualTo(LIVE_START.asString());
        assertThat(parsed.asString()).isEqualTo(checkpoint.asString());
    }

    @Test
    void parse_returns_a_catch_up_time_checkpoint_as_it_is() {
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN);

        assertThat(CatchupTimeCheckpoint.parse(checkpoint)).isSameAs(checkpoint);
    }

    @Test
    void a_live_start_holding_the_markers_is_kept_verbatim() {
        Checkpoint liveStart = new StringBasedCheckpoint("token;origin:9;liveFrom:inner");

        CatchupTimeCheckpoint parsed = CatchupTimeCheckpoint.parse(new StringBasedCheckpoint(CatchupTimeCheckpoint.of(TIME, liveStart, ORIGIN).asString()));

        assertThat(parsed.time()).isEqualTo(TIME);
        assertThat(parsed.replayOrigin()).isEqualTo(ORIGIN);
        assertThat(parsed.liveFrom().asString()).isEqualTo("token;origin:9;liveFrom:inner");
    }

    @Test
    void both_forms_are_catch_up_time_checkpoints() {
        CatchupTimeCheckpoint checkpoint = CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN);

        assertThat(CatchupTimeCheckpoint.isCatchupTimeCheckpoint(checkpoint)).isTrue();
        assertThat(CatchupTimeCheckpoint.isCatchupTimeCheckpoint(new StringBasedCheckpoint(checkpoint.asString()))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2026-01-01T10:00:05.123Z",
            "{\"resumeToken\": {\"_data\": \"8266F2\"}}",
            "position:42",
            "position:42;origin:7;replayTo:40;liveFrom:token",
            "position:42;origin:7;liveFrom:token",
            "2026-01-01T10:00:05.123Z;origin:2026-01-01T10:00:00Z",
            "2026-01-01T10:00:05.123Z;origin:2026-01-01T10:00:00Z;liveFrom:",
            "2026-01-01T10:00:05.123Z;liveFrom:token",
            "2026-01-01T10:00:05.123Z;origin:;liveFrom:token",
            ";origin:2026-01-01T10:00:00Z;liveFrom:token",
            " ;origin:2026-01-01T10:00:00Z;liveFrom:token",
            "2026-01-01T10:00:05.123Z;origin: ;liveFrom:token",
            "2026-01-01T10:00:05.123Z;liveFrom:token;origin:2026-01-01T10:00:00Z",
            "2026-01-01T10:00:05.123Z;origin:2026-01-01T10:00:00Z;x;liveFrom:token",
            ""
    })
    void anything_that_is_not_the_string_form_is_not_a_catch_up_time_checkpoint(String value) {
        Checkpoint checkpoint = new StringBasedCheckpoint(value);

        assertThat(CatchupTimeCheckpoint.isCatchupTimeCheckpoint(checkpoint)).isFalse();
        assertThatThrownBy(() -> CatchupTimeCheckpoint.parse(checkpoint)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_global_checkpoint_is_not_a_catch_up_time_checkpoint_and_the_other_way_around() {
        GlobalCheckpoint position = GlobalCheckpoint.of(42, LIVE_START, 7, 40);
        CatchupTimeCheckpoint time = CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN);

        assertThat(CatchupTimeCheckpoint.isCatchupTimeCheckpoint(position)).isFalse();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(time)).isFalse();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(new StringBasedCheckpoint(time.asString()))).isFalse();
    }

    @Test
    void the_live_start_is_compared_by_its_string_form() {
        Checkpoint typed = new Checkpoint() {
            @Override
            public String asString() {
                return LIVE_START.asString();
            }
        };

        assertThat(CatchupTimeCheckpoint.of(TIME, typed, ORIGIN)).isEqualTo(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN)).hasSameHashCodeAs(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN));
        assertThat(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN)).isNotEqualTo(CatchupTimeCheckpoint.of("2026-01-01T10:00:06Z", LIVE_START, ORIGIN));
        assertThat(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN)).isNotEqualTo(CatchupTimeCheckpoint.of(TIME, LIVE_START, "2025-12-31T00:00:00Z"));
        assertThat(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN)).isNotEqualTo(CatchupTimeCheckpoint.of(TIME, new StringBasedCheckpoint("other"), ORIGIN));
        assertThat(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN)).isNotEqualTo(new StringBasedCheckpoint(CatchupTimeCheckpoint.of(TIME, LIVE_START, ORIGIN).asString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2026-01-01T10:00:05Z;x"})
    void a_time_must_be_non_blank_and_free_of_the_separator(String time) {
        assertThatThrownBy(() -> CatchupTimeCheckpoint.of(time, LIVE_START, ORIGIN)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("time");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2026-01-01T10:00:00Z;x"})
    void a_replay_origin_must_be_non_blank_and_free_of_the_separator(String replayOrigin) {
        assertThatThrownBy(() -> CatchupTimeCheckpoint.of(TIME, LIVE_START, replayOrigin)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("replayOrigin");
    }

    @Test
    void a_live_start_and_the_times_cannot_be_null() {
        assertThatThrownBy(() -> CatchupTimeCheckpoint.of(TIME, null, ORIGIN)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> CatchupTimeCheckpoint.of(null, LIVE_START, ORIGIN)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> CatchupTimeCheckpoint.of(TIME, LIVE_START, null)).isInstanceOf(NullPointerException.class);
    }
}
