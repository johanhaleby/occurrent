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

package org.occurrent.dsl.projection.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.CatchupPhase;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.eventstore.api.AppendId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedAppendRecordingTest {

    private static final String PROJECTION_ID = "orders";

    // Reproduces the race a shared lock closes: without one, a live delivery could read "ready" just before an
    // observed replay's clear ran, then write its append back in right after that clear finished, reinstating a
    // record the clear was supposed to remove.
    @Test
    void a_concurrent_clear_cannot_interleave_between_the_readiness_check_and_the_write_it_authorized() throws InterruptedException {
        List<String> order = new ArrayList<>();
        CountDownLatch recordingStarted = new CountDownLatch(1);
        CountDownLatch clearAttempted = new CountDownLatch(1);
        AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        AppliedAppendStore store = new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                order.add("record-start");
                recordingStarted.countDown();
                try {
                    // Gives the concurrent clear attempt below a window to run. It must not be able to: it needs
                    // the same lock this write is still holding.
                    clearAttempted.await(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                delegate.recordApplied(projectionId, appendId);
                order.add("record-end");
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                return delegate.hasApplied(projectionId, appendId);
            }

            @Override
            public void clear(String projectionId) {
                order.add("clear");
                delegate.clear(projectionId);
            }
        };
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, ReplayPhase.neverReplays());

        Thread recorder = new Thread(() -> recording.recordIfReady(metadataWithAppendId(AppendId.mint())));
        recorder.start();
        recordingStarted.await();
        recording.replayObserved();
        clearAttempted.countDown();
        recorder.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(recorder.isAlive()).isFalse();
        assertThat(order).containsExactly("record-start", "record-end", "clear");
    }

    @Test
    void a_delivery_that_discovers_replaying_clears_immediately_rather_than_waiting_for_a_separate_observation() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> CatchupPhase.REPLAYING_HISTORY);

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();
    }

    @Test
    void replayCompleted_attempts_the_clear_directly_so_a_replay_with_no_matching_deliveries_is_not_left_pending() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, ReplayPhase.neverReplays());

        recording.replayStarted();
        // No deliveries in between: a domain-feed replay whose deliveries were all filtered out server-side.
        recording.replayCompleted();

        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();
    }

    // The race a poller closes by calling pollReplayPhase() instead of reading the phase itself first and
    // dispatching to replayObserved()/retryPendingClear() from that earlier reading: between the two, a live
    // delivery can land and record a genuinely live append, which replayObserved() called from the stale reading
    // would then wipe. pollReplayPhase() re-checks the phase itself, so it sees the replay has already ended and
    // leaves the live record alone.
    @Test
    void pollReplayPhase_checks_the_phase_fresh_so_a_live_delivery_recorded_after_an_earlier_reading_survives() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> replaying.get() ? CatchupPhase.REPLAYING_HISTORY : CatchupPhase.LIVE);

        // A poller would have read "replaying" here...
        assertThat(replaying.get()).isTrue();

        // ...but the replay actually ends and a live delivery arrives and records before the poller acts.
        replaying.set(false);
        AppendId liveAppend = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(liveAppend));
        assertThat(store.hasApplied(PROJECTION_ID, liveAppend)).isTrue();

        // The poller now calls pollReplayPhase(), not replayObserved() from its stale earlier reading.
        boolean sawReplaying = recording.pollReplayPhase();

        assertThat(sawReplaying).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, liveAppend)).isTrue();
    }

    // Decision 7 mandates a clear attempt on every delivery seen while replaying, not a repeat store.clear() call on
    // every one of them: a replay of N events all hitting the per-delivery check must clear once, not N times.
    @Test
    void a_replay_episode_clears_the_store_at_most_once_across_every_delivery_seen_while_replaying() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> CatchupPhase.REPLAYING_HISTORY);

        for (int i = 0; i < 1000; i++) {
            recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        }

        assertThat(clears).hasSize(1);
    }

    // The latch that suppresses a repeat clear has to reset once the episode ends, or a genuinely new replay later
    // would find recording still off with nothing left to clear it.
    @Test
    void a_new_replay_episode_after_going_live_is_cleared_again() {
        List<String> clears = new ArrayList<>();
        AtomicBoolean replaying = new AtomicBoolean(true);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> replaying.get() ? CatchupPhase.REPLAYING_HISTORY : CatchupPhase.LIVE);

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(1);

        replaying.set(false);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        replaying.set(true);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(2);
    }

    // The full state machine the per-episode latch has to get right: a failing clear is retried on every delivery
    // until it succeeds (unchanged from before this latch existed), and once it succeeds, every later delivery in
    // the same episode is suppressed rather than retried.
    @Test
    void a_failing_clear_is_retried_every_delivery_until_it_succeeds_then_suppressed_for_the_rest_of_the_episode() {
        List<String> clears = new ArrayList<>();
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, clearShouldFail), () -> CatchupPhase.REPLAYING_HISTORY);

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(2);

        clearShouldFail.set(false);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(3);

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(3);
    }

    @Test
    void replayAbandoned_does_not_attempt_the_clear_but_a_later_delivery_still_retries_it() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, ReplayPhase.neverReplays());

        recording.replayStarted();
        recording.replayAbandoned();
        assertThat(store.hasApplied(PROJECTION_ID, before)).isTrue();

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();
    }

    private static EventMetadata metadataWithAppendId(AppendId appendId) {
        return new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()));
    }

    // Records every clear(..) call in order, failing them while shouldFail is true.
    private static AppliedAppendStore clearCountingStore(List<String> clears, AtomicBoolean shouldFail) {
        AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        return new AppliedAppendStore() {
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
                clears.add(projectionId);
                if (shouldFail.get()) {
                    throw new RuntimeException("clear failed");
                }
                delegate.clear(projectionId);
            }
        };
    }
}
