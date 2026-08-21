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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.CatchupPhase;
import org.occurrent.dsl.projection.internal.AppliedAppendRecording;
import org.occurrent.eventstore.api.AppendId;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedAppendRecordingRegistryTest {

    private final AppliedAppendRecordingRegistry registry =
            new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);

    @Test
    void a_zero_or_negative_initial_interval_is_rejected() {
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ZERO, Duration.ofSeconds(5), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initial must be positive");
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ofMillis(-1), Duration.ofSeconds(5), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initial must be positive");
    }

    @Test
    void a_zero_or_negative_max_interval_is_rejected() {
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ZERO, 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max must be positive");
    }

    @Test
    void an_initial_interval_exceeding_max_is_rejected() {
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ofSeconds(10), Duration.ofSeconds(5), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initial cannot exceed max");
    }

    @Test
    void a_multiplier_below_one_is_rejected_because_it_would_shrink_the_interval_to_a_busy_loop() {
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ofSeconds(5), 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier must be at least 1.0");
    }

    @Test
    void a_nan_multiplier_is_rejected() {
        assertThatThrownBy(() -> new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ofSeconds(5), Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier must be at least 1.0");
    }

    @Test
    void a_freshly_registered_projection_is_due_at_the_initial_interval() {
        registry.register("p1", neverReplaying());

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_live_tick_doubles_the_interval() {
        registry.register("p1", neverReplaying());

        registry.tick("p1");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
    }

    @Test
    void repeated_live_ticks_cap_the_interval_at_the_configured_max() {
        registry.register("p1", neverReplaying());

        for (int i = 0; i < 10; i++) {
            registry.tick("p1");
        }

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofSeconds(5).toNanos());
    }

    @Test
    void a_replaying_tick_resets_the_interval_to_the_fast_end_and_calls_replayObserved() {
        AtomicInteger replayObservedCalls = new AtomicInteger();
        AtomicBoolean replaying = new AtomicBoolean(false);
        registry.register("p1", polling(replaying::get, replayObservedCalls::incrementAndGet, () -> {
        }));

        // Grow the interval first, so the reset below is actually observable.
        registry.tick("p1");
        registry.tick("p1");
        assertThat(registry.dueInNanos("p1")).isGreaterThan(Duration.ofMillis(200).toNanos());

        replaying.set(true);
        registry.tick("p1");

        assertThat(replayObservedCalls.get()).isEqualTo(1);
        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_tick_or_dueInNanos_for_an_unregistered_id_throws() {
        assertThatThrownBy(() -> registry.tick("unknown")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.dueInNanos("unknown")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void two_projections_pace_independently() {
        AtomicBoolean p2Replaying = new AtomicBoolean(true);
        registry.register("p1", neverReplaying());
        registry.register("p2", polling(p2Replaying::get, () -> {
        }, () -> {
        }));

        registry.tick("p1");
        registry.tick("p2");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
        assertThat(registry.dueInNanos("p2")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_live_tick_retries_a_pending_clear_through_retryPendingClear() {
        AtomicInteger retryCalls = new AtomicInteger();
        registry.register("p1", polling(() -> false, () -> {
        }, retryCalls::incrementAndGet));

        registry.tick("p1");

        assertThat(retryCalls.get()).isEqualTo(1);
    }

    // tick() no longer asks the phase itself and dispatches from that separate reading: it asks the recorder's own
    // pollReplayPhase(), which re-checks the phase and reacts atomically with that check. A recorder that ignores
    // the phase it was given and only pollReplayPhase() proves the registry never reads the phase on its own.
    @Test
    void tick_drives_entirely_through_pollReplayPhase_not_a_separate_phase_reading() {
        AtomicInteger pollCalls = new AtomicInteger();
        registry.register("p1", new AppliedAppendRecorder() {
            @Override
            public void replayObserved() {
                throw new AssertionError("tick() must not call replayObserved() directly");
            }

            @Override
            public void retryPendingClear() {
                throw new AssertionError("tick() must not call retryPendingClear() directly");
            }

            @Override
            public boolean pollReplayPhase() {
                pollCalls.incrementAndGet();
                return false;
            }
        });

        registry.tick("p1");

        assertThat(pollCalls.get()).isEqualTo(1);
        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
    }

    // The sequence a live tick's retry exists for: a replay is observed and its clear fails (store outage), the
    // replay ends and the phase reports live again, the store recovers, and no live event ever arrives to retry the
    // clear through the wrapper's own update path. Without the live-tick retry, pendingClear stays set forever and
    // hasApplied keeps answering true for the pre-replay appends the reset rule owes removal.
    @Test
    void a_clear_that_failed_during_a_replay_is_retried_on_a_later_live_tick_with_no_deliveries_and_recording_resumes() {
        FlakyClearStore store = new FlakyClearStore();
        AppendId before = AppendId.mint();
        store.recordApplied("orders", before);
        AtomicBoolean replaying = new AtomicBoolean(true);
        AppliedAppendRecording recording = new AppliedAppendRecording("orders", store, () -> replaying.get() ? CatchupPhase.REPLAYING_HISTORY : CatchupPhase.LIVE);
        registry.register("orders", adapting(recording));

        registry.tick("orders");
        assertThat(store.hasApplied("orders", before)).isTrue();

        replaying.set(false);
        store.clearShouldFail = false;

        registry.tick("orders");
        assertThat(store.hasApplied("orders", before)).isFalse();

        AppendId after = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(after));
        assertThat(store.hasApplied("orders", after)).isTrue();
    }

    private static AppliedAppendRecorder neverReplaying() {
        return polling(() -> false, () -> {
        }, () -> {
        });
    }

    // Mirrors what the registry itself used to orchestrate before pollReplayPhase(): reads phase, reacts, and
    // reports what it found, all from one recorder call.
    private static AppliedAppendRecorder polling(BooleanSupplier phase, Runnable onReplayObserved, Runnable onRetryPendingClear) {
        return new AppliedAppendRecorder() {
            @Override
            public void replayObserved() {
                onReplayObserved.run();
            }

            @Override
            public void retryPendingClear() {
                onRetryPendingClear.run();
            }

            @Override
            public boolean pollReplayPhase() {
                boolean isReplaying = phase.getAsBoolean();
                if (isReplaying) {
                    replayObserved();
                } else {
                    retryPendingClear();
                }
                return isReplaying;
            }
        };
    }

    private static AppliedAppendRecorder adapting(AppliedAppendRecording recording) {
        return new AppliedAppendRecorder() {
            @Override
            public void replayObserved() {
                recording.replayObserved();
            }

            @Override
            public void retryPendingClear() {
                recording.retryPendingClear();
            }

            @Override
            public boolean pollReplayPhase() {
                return recording.pollReplayPhase();
            }
        };
    }

    private static EventMetadata metadataWithAppendId(AppendId appendId) {
        return new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()));
    }

    // A store whose clear() fails until told otherwise, for the failed-clear-then-recovery test.
    private static final class FlakyClearStore implements AppliedAppendStore {
        private final AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        boolean clearShouldFail = true;

        @Override
        public void recordApplied(String projectionId, AppendId appendId) {
            delegate.recordApplied(projectionId, appendId);
        }

        @Override
        public boolean hasApplied(String projectionId, AppendId appendId) {
            return delegate.hasApplied(projectionId, appendId);
        }

        @Override
        public void clear(String projectionId) {
            if (clearShouldFail) {
                throw new RuntimeException("clear failed");
            }
            delegate.clear(projectionId);
        }
    }
}
