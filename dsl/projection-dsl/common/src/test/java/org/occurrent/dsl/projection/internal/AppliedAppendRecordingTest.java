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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> true);

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, replaying::get);

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
}
