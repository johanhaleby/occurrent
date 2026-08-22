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
    void a_tick_with_something_to_react_to_resets_the_interval_to_the_fast_end() {
        AtomicBoolean busy = new AtomicBoolean(false);
        registry.register("p1", busy::get);

        // Grow the interval first, so the reset below is actually observable.
        registry.tick("p1");
        registry.tick("p1");
        assertThat(registry.dueInNanos("p1")).isGreaterThan(Duration.ofMillis(200).toNanos());

        busy.set(true);
        registry.tick("p1");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_tick_or_dueInNanos_for_an_unregistered_id_throws() {
        assertThatThrownBy(() -> registry.tick("unknown")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.dueInNanos("unknown")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void two_projections_pace_independently() {
        registry.register("p1", neverReplaying());
        registry.register("p2", (BooleanSupplier) () -> true);

        registry.tick("p1");
        registry.tick("p2");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
        assertThat(registry.dueInNanos("p2")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    // A recorder registered directly is polled for its clear and for nothing else, which is the whole of what a
    // poll can do for a projection whose model tells it when its catch-ups begin and end.
    @Test
    void a_tick_for_a_recorder_asks_it_only_to_poll_for_a_clear() {
        AtomicInteger pollCalls = new AtomicInteger();
        registry.register("p1", new AppliedAppendRecorder() {
            @Override
            public void catchupStarted(Object episode) {
                throw new AssertionError("tick() must not send catch-up signals");
            }

            @Override
            public void historyRead(Object episode) {
                throw new AssertionError("tick() must not send catch-up signals");
            }

            @Override
            public void retryPendingClear() {
                throw new AssertionError("tick() must ask pollForClear(), which also writes what was waiting");
            }

            @Override
            public boolean pollForClear() {
                pollCalls.incrementAndGet();
                return false;
            }
        });

        registry.tick("p1");

        assertThat(pollCalls.get()).isEqualTo(1);
        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
    }

    // The sequence the poll's clear retry exists for: a catch-up starts and the clear it owes fails (store outage),
    // the catch-up ends, the store recovers, and no event ever arrives to retry the clear through the wrapper's own
    // update path. Without the retry, the clear stays owed forever and hasApplied keeps answering true for the
    // appends the rebuild is discarding.
    @Test
    void a_clear_that_failed_during_a_catch_up_is_retried_on_a_later_tick_with_no_deliveries_and_recording_resumes() {
        FlakyClearStore store = new FlakyClearStore();
        AppendId before = AppendId.mint();
        store.recordApplied("orders", before);
        AppliedAppendRecording recording = new AppliedAppendRecording("orders", store);
        registry.register("orders", (BooleanSupplier) recording::pollForClear);
        Object episode = new Object();

        recording.catchupStarted(episode);
        registry.tick("orders");
        assertThat(store.hasApplied("orders", before)).isTrue();

        recording.historyRead(episode);
        store.clearShouldFail = false;

        registry.tick("orders");
        assertThat(store.hasApplied("orders", before)).isFalse();

        AppendId after = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(after));
        assertThat(store.hasApplied("orders", after)).isTrue();
    }

    // A tick that keeps reporting a clear is still owed keeps the poll at its fast end, so a store outage is not
    // waited out at the slow one.
    @Test
    void a_tick_that_still_owes_a_clear_stays_at_the_fast_interval() {
        FlakyClearStore store = new FlakyClearStore();
        AppliedAppendRecording recording = new AppliedAppendRecording("orders", store);
        registry.register("orders", (BooleanSupplier) recording::pollForClear);

        recording.catchupStarted(new Object());
        registry.tick("orders");
        registry.tick("orders");

        assertThat(registry.dueInNanos("orders")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    private static BooleanSupplier neverReplaying() {
        return () -> false;
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
