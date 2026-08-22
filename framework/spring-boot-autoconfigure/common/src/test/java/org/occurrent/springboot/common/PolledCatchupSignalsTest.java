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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.projection.AppliedAppendRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PolledCatchupSignalsTest {

    @Test
    void a_reading_that_turns_from_live_to_catching_up_starts_a_catch_up() {
        RecordingRecorder recorder = new RecordingRecorder();
        AtomicBoolean catchingUp = new AtomicBoolean(false);
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, catchingUp::get);

        signals.getAsBoolean();
        assertThat(recorder.signals()).isEmpty();

        catchingUp.set(true);
        signals.getAsBoolean();

        assertThat(recorder.signals()).containsExactly("catchupStarted:0");
    }

    @Test
    void a_reading_that_turns_back_ends_the_catch_up_it_started() {
        RecordingRecorder recorder = new RecordingRecorder();
        AtomicBoolean catchingUp = new AtomicBoolean(true);
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, catchingUp::get);

        signals.getAsBoolean();
        catchingUp.set(false);
        signals.getAsBoolean();

        assertThat(recorder.signals()).containsExactly("catchupStarted:0", "historyRead:0");
    }

    // Every tick during one catch-up would otherwise announce a new one, which clears the store again and drops
    // whatever the projection recorded in between.
    @Test
    void a_catch_up_that_spans_several_ticks_is_announced_once() {
        RecordingRecorder recorder = new RecordingRecorder();
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, () -> true);

        signals.getAsBoolean();
        signals.getAsBoolean();
        signals.getAsBoolean();

        assertThat(recorder.signals()).containsExactly("catchupStarted:0");
    }

    @Test
    void a_second_catch_up_is_announced_as_a_different_one() {
        RecordingRecorder recorder = new RecordingRecorder();
        AtomicBoolean catchingUp = new AtomicBoolean(true);
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, catchingUp::get);

        signals.getAsBoolean();
        catchingUp.set(false);
        signals.getAsBoolean();
        catchingUp.set(true);
        signals.getAsBoolean();

        assertThat(recorder.signals()).containsExactly("catchupStarted:0", "historyRead:0", "catchupStarted:1");
    }

    // The clear runs after the signal that owes it, so a catch-up that has just been announced is cleared on the
    // same tick rather than a whole interval later.
    @Test
    void a_tick_polls_for_the_clear_after_the_signal_that_owes_it() {
        RecordingRecorder recorder = new RecordingRecorder();
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, () -> true);

        signals.getAsBoolean();

        assertThat(recorder.calls).containsExactly("catchupStarted:0", "pollForClear");
    }

    @Test
    void a_tick_reports_a_running_catch_up_so_the_poll_stays_at_its_fast_interval() {
        RecordingRecorder recorder = new RecordingRecorder();
        AtomicBoolean catchingUp = new AtomicBoolean(true);
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, catchingUp::get);

        assertThat(signals.getAsBoolean()).isTrue();

        catchingUp.set(false);
        assertThat(signals.getAsBoolean()).isFalse();
    }

    @Test
    void a_tick_reports_a_clear_that_is_still_owed_even_with_no_catch_up_running() {
        RecordingRecorder recorder = new RecordingRecorder();
        recorder.clearOwed = true;
        PolledCatchupSignals signals = new PolledCatchupSignals(recorder, () -> false);

        assertThat(signals.getAsBoolean()).isTrue();
    }

    private static final class RecordingRecorder implements AppliedAppendRecorder {
        private final List<String> calls = new ArrayList<>();
        private final List<Object> episodes = new ArrayList<>();
        boolean clearOwed = false;

        // What the recorder was told, without the clear poll every tick also runs.
        List<String> signals() {
            return calls.stream().filter(call -> !call.equals("pollForClear")).toList();
        }

        @Override
        public void catchupStarted(Object episode) {
            calls.add("catchupStarted:" + indexOf(episode));
        }

        @Override
        public void historyRead(Object episode) {
            calls.add("historyRead:" + indexOf(episode));
        }

        @Override
        public void retryPendingClear() {
            calls.add("retryPendingClear");
        }

        @Override
        public boolean pollForClear() {
            calls.add("pollForClear");
            return clearOwed;
        }

        private int indexOf(Object episode) {
            for (int i = 0; i < episodes.size(); i++) {
                if (episodes.get(i) == episode) {
                    return i;
                }
            }
            episodes.add(episode);
            return episodes.size() - 1;
        }
    }
}
