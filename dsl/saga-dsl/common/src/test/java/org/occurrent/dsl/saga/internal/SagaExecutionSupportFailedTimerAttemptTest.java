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
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SagaExecutionSupport.withFailedTimerAttempt")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaExecutionSupportFailedTimerAttemptTest {

    private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant UPDATED = Instant.parse("2026-01-01T00:05:00Z");
    private static final Instant ATTEMPTED_AT = Instant.parse("2026-01-01T01:00:00Z");

    private static final TimerEntry FAILED = new TimerEntry("failed", 1_000);
    private static final TimerEntry OTHER = new TimerEntry("other", 2_000);

    private static SagaEnvelope<String> active(List<TimerEntry> timers) {
        return envelope(SagaStatus.ACTIVE, timers);
    }

    private static SagaEnvelope<String> envelope(SagaStatus status, List<TimerEntry> timers) {
        return new SagaEnvelope<>("s1", "state", status, 7, timers, Map.of("stream", 3L), 11L, CREATED, UPDATED, null, null);
    }

    @Test
    void marks_only_the_timer_that_failed_and_leaves_the_others_untouched() {
        SagaEnvelope<String> marked = SagaExecutionSupport.withFailedTimerAttempt(active(List.of(OTHER, FAILED)), FAILED, ATTEMPTED_AT);

        assertThat(marked).isNotNull();
        assertThat(marked.timers()).containsExactly(OTHER, new TimerEntry("failed", 1_000, ATTEMPTED_AT.toEpochMilli()));
    }

    @Test
    void bumps_the_version_by_one_and_keeps_updatedAt_and_everything_else() {
        SagaEnvelope<String> current = active(List.of(FAILED));

        SagaEnvelope<String> marked = SagaExecutionSupport.withFailedTimerAttempt(current, FAILED, ATTEMPTED_AT);

        assertThat(marked).isNotNull();
        assertAll(
                () -> assertThat(marked.version()).isEqualTo(8),
                () -> assertThat(marked.updatedAt()).isEqualTo(UPDATED),
                () -> assertThat(marked.createdAt()).isEqualTo(CREATED),
                () -> assertThat(marked.sagaId()).isEqualTo("s1"),
                () -> assertThat(marked.state()).isEqualTo("state"),
                () -> assertThat(marked.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(marked.streamWatermarks()).isEqualTo(Map.of("stream", 3L)),
                () -> assertThat(marked.started()).isEqualTo(current.started()),
                () -> assertThat(marked.failure()).isEqualTo(current.failure())
        );
    }

    @Test
    void does_not_modify_the_envelope_it_was_given() {
        SagaEnvelope<String> current = active(List.of(FAILED));

        SagaExecutionSupport.withFailedTimerAttempt(current, FAILED, ATTEMPTED_AT);

        assertAll(
                () -> assertThat(current.version()).isEqualTo(7),
                () -> assertThat(current.timers()).containsExactly(FAILED)
        );
    }

    @Test
    void marks_a_timer_whose_earlier_marker_is_before_the_attempt() {
        TimerEntry markedEarlier = new TimerEntry("failed", 1_000, ATTEMPTED_AT.toEpochMilli() - 1);

        SagaEnvelope<String> marked = SagaExecutionSupport.withFailedTimerAttempt(active(List.of(markedEarlier)), FAILED, ATTEMPTED_AT);

        assertThat(marked).isNotNull();
        assertThat(marked.timers()).containsExactly(new TimerEntry("failed", 1_000, ATTEMPTED_AT.toEpochMilli()));
    }

    @Test
    void writes_nothing_when_the_marker_is_already_at_the_attempt() {
        TimerEntry alreadyMarked = new TimerEntry("failed", 1_000, ATTEMPTED_AT.toEpochMilli());

        assertThat(SagaExecutionSupport.withFailedTimerAttempt(active(List.of(alreadyMarked)), FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_marker_is_after_the_attempt() {
        TimerEntry markedLater = new TimerEntry("failed", 1_000, ATTEMPTED_AT.toEpochMilli() + 1);

        assertThat(SagaExecutionSupport.withFailedTimerAttempt(active(List.of(markedLater)), FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_instance_is_gone() {
        assertThat(SagaExecutionSupport.<String>withFailedTimerAttempt(null, FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_instance_has_completed() {
        assertThat(SagaExecutionSupport.withFailedTimerAttempt(envelope(SagaStatus.COMPLETED, List.of(FAILED)), FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_instance_is_quarantined() {
        SagaFailure failure = new SagaFailure("s@7", 7L, CREATED, IllegalStateException.class.getName(), "boom");
        SagaEnvelope<String> quarantined = new SagaEnvelope<>("s1", "state", SagaStatus.QUARANTINED, 7, List.of(FAILED), Map.of(), null,
                CREATED, UPDATED, null, null, true, failure);

        assertThat(SagaExecutionSupport.withFailedTimerAttempt(quarantined, FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_timer_was_cancelled() {
        assertThat(SagaExecutionSupport.withFailedTimerAttempt(active(List.of(OTHER)), FAILED, ATTEMPTED_AT)).isNull();
    }

    @Test
    void writes_nothing_when_the_timer_was_re_armed_to_another_firing_time() {
        TimerEntry reArmed = new TimerEntry("failed", 9_000);

        assertThat(SagaExecutionSupport.withFailedTimerAttempt(active(List.of(reArmed)), FAILED, ATTEMPTED_AT)).isNull();
    }
}
