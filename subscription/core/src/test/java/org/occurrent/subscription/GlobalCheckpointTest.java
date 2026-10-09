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
class GlobalCheckpointTest {

    // The form of a Mongo resume token checkpoint's string, which holds colons, commas and quotes
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("{\"resumeToken\": {\"_data\": \"8266F2\"}}");

    @Test
    void a_checkpoint_without_a_live_start_keeps_the_string_form_older_versions_store() {
        GlobalCheckpoint checkpoint = GlobalCheckpoint.of(42);

        assertThat(checkpoint.asString()).isEqualTo("position:42");
        assertThat(checkpoint.position()).isEqualTo(42);
        assertThat(checkpoint.liveFrom()).isEmpty();
        assertThat(checkpoint.replayOrigin()).isEmpty();
        assertThat(checkpoint.replayTo()).isEmpty();
    }

    @Test
    void a_checkpoint_with_a_live_start_puts_the_live_start_last_and_verbatim() {
        GlobalCheckpoint checkpoint = GlobalCheckpoint.of(42, LIVE_START, 7, 40);

        assertThat(checkpoint.asString()).isEqualTo("position:42;origin:7;replayTo:40;liveFrom:{\"resumeToken\": {\"_data\": \"8266F2\"}}");
        assertThat(checkpoint.liveFrom()).contains(LIVE_START);
        assertThat(checkpoint.replayOrigin()).hasValue(7);
        assertThat(checkpoint.replayTo()).hasValue(40);
    }

    @Test
    void parse_reads_back_both_string_forms_as_they_come_out_of_a_storage_that_keeps_strings() {
        GlobalCheckpoint withLiveStart = GlobalCheckpoint.of(42, LIVE_START, 7, 40);
        GlobalCheckpoint withoutLiveStart = GlobalCheckpoint.of(42);

        GlobalCheckpoint parsedWithLiveStart = GlobalCheckpoint.parse(new StringBasedCheckpoint(withLiveStart.asString()));
        GlobalCheckpoint parsedWithoutLiveStart = GlobalCheckpoint.parse(new StringBasedCheckpoint(withoutLiveStart.asString()));

        assertThat(parsedWithLiveStart).isEqualTo(withLiveStart);
        assertThat(parsedWithLiveStart.liveFrom().map(Checkpoint::asString)).contains(LIVE_START.asString());
        assertThat(parsedWithLiveStart.replayOrigin()).hasValue(7);
        assertThat(parsedWithLiveStart.replayTo()).hasValue(40);
        assertThat(parsedWithoutLiveStart).isEqualTo(withoutLiveStart);
        assertThat(parsedWithoutLiveStart.liveFrom()).isEmpty();
        assertThat(GlobalCheckpoint.positionOf(new StringBasedCheckpoint(withLiveStart.asString()))).isEqualTo(42);
    }

    @Test
    void a_live_start_stored_without_a_replay_end_is_read_back_as_a_plain_position() {
        GlobalCheckpoint parsed = GlobalCheckpoint.parse(new StringBasedCheckpoint("position:42;origin:7;liveFrom:" + LIVE_START.asString()));

        assertThat(parsed).isEqualTo(GlobalCheckpoint.of(42));
        assertThat(parsed.liveFrom()).isEmpty();
    }

    @Test
    void a_live_start_holding_the_replay_end_marker_is_kept_verbatim() {
        Checkpoint liveStart = new StringBasedCheckpoint("token;replayTo:9");

        GlobalCheckpoint parsed = GlobalCheckpoint.parse(new StringBasedCheckpoint(GlobalCheckpoint.of(42, liveStart, 7, 40).asString()));

        assertThat(parsed.replayTo()).hasValue(40);
        assertThat(parsed.liveFrom().map(Checkpoint::asString)).contains("token;replayTo:9");
    }

    @Test
    void parse_returns_a_global_checkpoint_as_it_is() {
        GlobalCheckpoint checkpoint = GlobalCheckpoint.of(42, LIVE_START, 7, 40);

        assertThat(GlobalCheckpoint.parse(checkpoint)).isSameAs(checkpoint);
    }

    @Test
    void the_live_start_is_compared_by_its_string_form() {
        Checkpoint typed = new Checkpoint() {
            @Override
            public String asString() {
                return LIVE_START.asString();
            }
        };

        assertThat(GlobalCheckpoint.of(42, typed, 7, 40)).isEqualTo(GlobalCheckpoint.of(42, LIVE_START, 7, 40)).hasSameHashCodeAs(GlobalCheckpoint.of(42, LIVE_START, 7, 40));
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 7, 40)).isNotEqualTo(GlobalCheckpoint.of(42, LIVE_START, 6, 40));
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 7, 40)).isNotEqualTo(GlobalCheckpoint.of(42, LIVE_START, 7, 41));
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 7, 40)).isNotEqualTo(GlobalCheckpoint.of(42, new StringBasedCheckpoint("other"), 7, 40));
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 42, 42)).isNotEqualTo(GlobalCheckpoint.of(42));
    }

    @Test
    void both_string_forms_are_global_checkpoints_once_read_back() {
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(new StringBasedCheckpoint("position:42"))).isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(new StringBasedCheckpoint(GlobalCheckpoint.of(42, LIVE_START, 7, 40).asString()))).isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(LIVE_START)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 43})
    void a_replay_origin_must_lie_between_zero_and_the_position(long replayOrigin) {
        assertThatThrownBy(() -> GlobalCheckpoint.of(42, LIVE_START, replayOrigin, 40)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_replay_end_can_lie_on_either_side_of_the_position_but_not_below_zero() {
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 7, 50).replayTo()).hasValue(50);
        assertThat(GlobalCheckpoint.of(42, LIVE_START, 7, 3).replayTo()).hasValue(3);
        assertThatThrownBy(() -> GlobalCheckpoint.of(42, LIVE_START, 7, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_position_cannot_be_negative() {
        assertThatThrownBy(() -> GlobalCheckpoint.of(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GlobalCheckpoint.of(-1, LIVE_START, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2026-01-01T10:00:00Z",
            "position:",
            "position:x",
            "position:-1",
            "position:42;origin:7",
            "position:42;origin:7;replayTo:40;liveFrom:",
            "position:42;origin:x;replayTo:40;liveFrom:token",
            "position:42;origin:43;replayTo:40;liveFrom:token",
            "position:42;origin:-1;replayTo:40;liveFrom:token",
            "position:42;origin:7;replayTo:x;liveFrom:token",
            "position:42;origin:7;replayTo:-1;liveFrom:token"
    })
    void parse_refuses_anything_that_is_not_one_of_the_two_string_forms(String value) {
        assertThatThrownBy(() -> GlobalCheckpoint.parse(new StringBasedCheckpoint(value))).isInstanceOf(IllegalArgumentException.class);
    }
}
