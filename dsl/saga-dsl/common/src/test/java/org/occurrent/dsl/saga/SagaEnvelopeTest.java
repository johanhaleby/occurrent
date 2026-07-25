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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link SagaInstance} view {@link SagaEnvelope} implements directly: {@link SagaEnvelope#nextTimerAt()}
 * (the earliest pending timer, independent of any store) and {@link SagaEnvelope#currentStep()} (read off a flow saga's
 * {@code FlowState}, {@code null} for anything else). Both are pure functions of the envelope, so they need no store at
 * all to test.
 */
@DisplayName("SagaEnvelope as a SagaInstance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaEnvelopeTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private static SagaEnvelope<String> envelopeWithTimers(List<TimerEntry> timers) {
        return new SagaEnvelope<>("s1", "state", SagaStatus.ACTIVE, 1, timers, Map.of(), null, NOW, NOW, null);
    }

    @Nested
    class NextTimerAt {

        @Test
        void is_null_when_there_are_no_pending_timers() {
            assertThat(envelopeWithTimers(List.of()).nextTimerAt()).isNull();
        }

        @Test
        void is_the_timers_firing_time_when_exactly_one_is_pending() {
            SagaEnvelope<String> envelope = envelopeWithTimers(List.of(new TimerEntry("t", NOW.plusSeconds(30).toEpochMilli())));

            assertThat(envelope.nextTimerAt()).isEqualTo(NOW.plusSeconds(30));
        }

        @Test
        void is_the_earliest_firing_time_across_several_pending_timers() {
            SagaEnvelope<String> envelope = envelopeWithTimers(List.of(
                    new TimerEntry("later", NOW.plusSeconds(60).toEpochMilli()),
                    new TimerEntry("earliest", NOW.plusSeconds(10).toEpochMilli()),
                    new TimerEntry("middle", NOW.plusSeconds(30).toEpochMilli())
            ));

            assertThat(envelope.nextTimerAt()).isEqualTo(NOW.plusSeconds(10));
        }
    }

    @Nested
    class CurrentStep {

        @Test
        void is_populated_for_a_flow_saga_whose_state_is_a_FlowState() {
            FlowStateImpl<Object> flowState = new FlowStateImpl<>("awaiting-payment", List.of(), 1, 0, false, null, ActionKind.NONE, -1);
            SagaEnvelope<FlowStateImpl<Object>> envelope = new SagaEnvelope<>("s1", flowState, SagaStatus.ACTIVE, 1,
                    List.of(), Map.of(), null, NOW, NOW, null);

            assertThat(envelope.currentStep()).isEqualTo("awaiting-payment");
        }

        @Test
        void is_null_for_a_saga_written_against_the_core_builder_whose_state_is_not_a_FlowState() {
            assertThat(envelopeWithTimers(List.of()).currentStep()).isNull();
        }

        @Test
        void is_null_when_the_state_itself_is_null() {
            SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", null, SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW, NOW, null);

            assertThat(envelope.currentStep()).isNull();
        }
    }
}
