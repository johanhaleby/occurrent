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
import org.occurrent.dsl.projection.CatchupSnapshot;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.eventstore.api.AppendId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> CatchupSnapshot.readingHistory(1L));

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> replaying.get() ? CatchupSnapshot.readingHistory(1L) : CatchupSnapshot.LIVE);

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> CatchupSnapshot.readingHistory(1L));

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> replaying.get() ? CatchupSnapshot.readingHistory(1L) : CatchupSnapshot.LIVE);

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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, clearShouldFail), () -> CatchupSnapshot.readingHistory(1L));

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

    // The defect this phase exists to fix. A catch-up delivers the events written since it started through the same
    // action it used for the history, and for some of them that is the only delivery, so treating the whole catch-up
    // as a replay loses them.
    @Test
    void an_append_handled_during_the_reconciliation_is_recorded_even_though_the_catch_up_has_not_handed_over() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.REPLAYING_HISTORY);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(phase.get(), phase.get() == CatchupPhase.LIVE ? 0L : 1L));

        AppendId history = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(history));
        assertThat(store.hasApplied(PROJECTION_ID, history)).isFalse();

        phase.set(CatchupPhase.RECONCILING);
        AppendId writtenDuringTheReplay = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(writtenDuringTheReplay));

        assertThat(store.hasApplied(PROJECTION_ID, writtenDuringTheReplay)).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, history)).isFalse();
    }

    // The clear stays a precondition in the reconciliation, so a projection whose history read handled nothing (an
    // empty store, or a filter that matched none of it) still clears before it records anything.
    @Test
    void the_reconciliation_clears_once_before_it_records_and_not_again_per_delivery() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> new CatchupSnapshot(CatchupPhase.RECONCILING, 1L));

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(1);
    }

    // Recording an append while a clear is owed would only give that clear something more to delete, so it waits.
    // Without the wait the append is lost for good, since the reconciliation is the only delivery it gets.
    @Test
    void an_append_handled_while_a_clear_is_owed_is_written_once_that_clear_succeeds() {
        List<String> clears = new ArrayList<>();
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(clears, clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(CatchupPhase.RECONCILING, 1L));

        AppendId first = AppendId.mint();
        AppendId second = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(first));
        recording.recordIfReady(metadataWithAppendId(second));
        assertThat(store.hasApplied(PROJECTION_ID, first)).isFalse();

        clearShouldFail.set(false);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(store.hasApplied(PROJECTION_ID, first)).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, second)).isTrue();
    }

    // The flush cannot be gated on the clear this call ran, because something else can be what made it succeed.
    // retryPendingClear is that something else: it runs the clear and deliberately writes nothing, since it cannot
    // see the phase. The next delivery is what has to notice the buffer is now writable.
    @Test
    void appends_waiting_for_a_clear_are_written_by_the_next_delivery_when_something_else_made_that_clear_succeed() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(CatchupPhase.RECONCILING, 1L));

        AppendId waiting = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(waiting));
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        clearShouldFail.set(false);
        recording.retryPendingClear();
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isTrue();
    }

    // A replay starting again means the read model is being built from scratch, so anything a parked reconciliation
    // was still holding describes events that read model has not been given.
    @Test
    void appends_waiting_for_a_clear_are_dropped_when_a_poll_tick_finds_the_history_being_read_again() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.RECONCILING);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(phase.get(), phase.get() == CatchupPhase.LIVE ? 0L : 1L));

        AppendId waiting = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(waiting));
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        // A poll tick that reads the history branch runs the clear and records nothing.
        phase.set(CatchupPhase.REPLAYING_HISTORY);
        clearShouldFail.set(false);
        recording.pollReplayPhase();

        // Everything the history branch is reached with is dropped, since a replay starting again means the read
        // model is being rebuilt and those appends have not been applied to it.
        phase.set(CatchupPhase.LIVE);
        AppendId live = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(live));

        assertThat(store.hasApplied(PROJECTION_ID, live)).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();
    }

    // A relaunched replay reads the history again, so anything the parked reconciliation was still holding describes
    // a read model that is being built from scratch.
    @Test
    void appends_waiting_for_a_clear_are_dropped_when_the_replay_starts_over_instead_of_going_live() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.RECONCILING);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(phase.get(), phase.get() == CatchupPhase.LIVE ? 0L : 1L));

        AppendId parked = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(parked));

        phase.set(CatchupPhase.REPLAYING_HISTORY);
        clearShouldFail.set(false);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        phase.set(CatchupPhase.RECONCILING);
        AppendId afterTheRelaunch = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(afterTheRelaunch));

        assertThat(store.hasApplied(PROJECTION_ID, parked)).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, afterTheRelaunch)).isTrue();
    }

    // A stop parks the replay mid-reconciliation and start(..) reads the history again. Nothing goes live in
    // between, so the edge from reconciliation back to history is the only thing that can start the next episode.
    @Test
    void a_replay_relaunched_from_the_reconciliation_clears_again_with_no_live_delivery_in_between() {
        List<String> clears = new ArrayList<>();
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.REPLAYING_HISTORY);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> new CatchupSnapshot(phase.get(), phase.get() == CatchupPhase.LIVE ? 0L : 1L));

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        phase.set(CatchupPhase.RECONCILING);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(1);

        phase.set(CatchupPhase.REPLAYING_HISTORY);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(2);
    }

    // A catch-up never goes live and then reconciles again, so a reconciliation seen after a live delivery belongs to
    // a second catch-up. Its history read matching nothing is what makes it invisible, which the poll interval then
    // steps over, so nothing else drops what the first one was holding.
    @Test
    void appends_waiting_for_a_clear_are_dropped_when_a_second_catch_up_reconciles_without_its_history_being_seen() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.RECONCILING);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(phase.get(), phase.get() == CatchupPhase.LIVE ? 0L : 1L));

        AppendId firstCatchup = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(firstCatchup));

        // The first catch-up hands over with its clear still failing, so the buffer survives the live observation.
        phase.set(CatchupPhase.LIVE);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(store.hasApplied(PROJECTION_ID, firstCatchup)).isFalse();

        // A second catch-up whose history read handled nothing, so the first thing seen of it is its reconciliation.
        clearShouldFail.set(false);
        phase.set(CatchupPhase.RECONCILING);
        AppendId secondCatchup = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(secondCatchup));

        assertThat(store.hasApplied(PROJECTION_ID, firstCatchup)).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, secondCatchup)).isTrue();
    }

    // The invariant: every catch-up clears exactly once before its first record, whatever the recorder happened to
    // sample. Two catch-ups in a row look identical to a recorder that only saw a reconciliation in each, because the
    // handover, the live gap and the second history read can all fall between two observations, which the poll
    // routinely misses when that history read matches nothing. The generation is what tells them apart.
    @Test
    void a_second_catch_up_clears_even_when_every_phase_between_the_two_went_unobserved() {
        List<String> clears = new ArrayList<>();
        AtomicReference<CatchupPhase> phase = new AtomicReference<>(CatchupPhase.RECONCILING);
        AtomicLong generation = new AtomicLong(1);
        AppliedAppendStore store = clearCountingStore(clears, new AtomicBoolean(false));
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, () -> new CatchupSnapshot(phase.get(), generation.get()));

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(1);

        // A second catch-up. Nothing observed the handover, the live gap or its history read, so the only thing that
        // changed is which catch-up this is.
        generation.set(2);
        AppendId secondCatchup = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(secondCatchup));

        assertThat(clears).hasSize(2);
        assertThat(store.hasApplied(PROJECTION_ID, secondCatchup)).isTrue();
    }

    // A recorder decides from one reading, so it can never act on a pair that never existed, a reconciliation
    // belonging to a catch-up that has already finished. Constructing that pair by hand is the only way to reach it,
    // and this asserts what the recorder does with the honest pair the models actually hand it.
    @Test
    void a_catch_up_that_has_finished_reads_as_live_and_clears_nothing() {
        List<String> clears = new ArrayList<>();
        AppliedAppendStore store = clearCountingStore(clears, new AtomicBoolean(false));
        AppendId recordedByTheCatchup = AppendId.mint();
        AtomicReference<CatchupSnapshot> snapshot = new AtomicReference<>(new CatchupSnapshot(CatchupPhase.RECONCILING, 7L));
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store, snapshot::get);

        recording.recordIfReady(metadataWithAppendId(recordedByTheCatchup));
        assertThat(store.hasApplied(PROJECTION_ID, recordedByTheCatchup)).isTrue();
        clears.clear();

        // The catch-up finished, which the model reports as one reading rather than as a phase and a number that can
        // disagree.
        snapshot.set(CatchupSnapshot.LIVE);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).isEmpty();
        assertThat(store.hasApplied(PROJECTION_ID, recordedByTheCatchup)).isTrue();
    }

    // Staying in the history read must not start a new episode on every event, which is what the per-episode latch
    // is for. Only the edge from the reconciliation back to history does.
    @Test
    void staying_in_the_history_read_still_clears_only_once() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)), () -> CatchupSnapshot.readingHistory(1L));

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(1);
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
