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

    // Reproduces the race a shared lock closes: without one, a live delivery could read "ready" just before a
    // catch-up's clear ran, then write its append back in right after that clear finished, reinstating a record the
    // clear was supposed to remove.
    @Test
    void a_concurrent_catch_up_cannot_interleave_between_the_readiness_check_and_the_write_it_authorized() throws InterruptedException {
        List<String> order = new ArrayList<>();
        CountDownLatch recordingStarted = new CountDownLatch(1);
        CountDownLatch catchupAnnounced = new CountDownLatch(1);
        AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        AppliedAppendStore store = new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                order.add("record-start");
                recordingStarted.countDown();
                try {
                    // Gives the concurrent catch-up announcement below a window to run. It must not be able to: it
                    // needs the same lock this write is still holding.
                    catchupAnnounced.await(200, TimeUnit.MILLISECONDS);
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
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);

        Thread recorder = new Thread(() -> recording.recordIfReady(metadataWithAppendId(AppendId.mint())));
        recorder.start();
        recordingStarted.await();
        recording.catchupStarted(new Object());
        catchupAnnounced.countDown();
        recorder.join(TimeUnit.SECONDS.toMillis(5));
        recording.pollForClear();

        assertThat(recorder.isAlive()).isFalse();
        assertThat(order).containsExactly("record-start", "record-end", "clear");
    }

    @Test
    void the_first_delivery_after_a_catch_up_starts_runs_the_clear_that_catch_up_owes() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);

        recording.catchupStarted(new Object());
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();
    }

    // A catch-up whose deliveries were all filtered out server-side gives the recorder nothing to run the clear on,
    // so the poll has to.
    @Test
    void a_poll_tick_runs_the_clear_for_a_catch_up_that_delivered_nothing() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);

        recording.catchupStarted(new Object());
        boolean stillOwed = recording.pollForClear();

        assertThat(stillOwed).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();
    }

    // The signal is sent from the thread that registers the catch-up, before anything that could deliver exists, so
    // it must not block that thread on a store round trip.
    @Test
    void catchupStarted_touches_the_store_on_no_thread_of_its_own() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)));

        recording.catchupStarted(new Object());

        assertThat(clears).isEmpty();
    }

    // The defect this design exists to fix (#890). A catch-up delivers the events written since it started through
    // the same action it used for the history, and for some of them that is the only delivery, so treating the whole
    // catch-up as history loses them.
    @Test
    void an_append_delivered_after_the_history_has_been_read_is_recorded_even_though_the_catch_up_has_not_handed_over() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object episode = new Object();

        recording.catchupStarted(episode);
        AppendId history = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(history));
        assertThat(store.hasApplied(PROJECTION_ID, history)).isFalse();

        recording.historyRead(episode);
        AppendId writtenDuringTheCatchup = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(writtenDuringTheCatchup));

        assertThat(store.hasApplied(PROJECTION_ID, writtenDuringTheCatchup)).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, history)).isFalse();
    }

    // The episode token, and the whole reason there is one. A catch-up that lost its subscription can still be
    // running when its replacement starts, and its history-read signal arriving late must not move the replacement
    // past a history the replacement has not read yet.
    @Test
    void a_history_read_from_a_catch_up_that_has_been_replaced_does_not_start_recording_for_the_replacement() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object replaced = new Object();
        Object replacement = new Object();

        recording.catchupStarted(replaced);
        recording.catchupStarted(replacement);
        recording.historyRead(replaced);

        AppendId historyOfTheReplacement = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(historyOfTheReplacement));
        assertThat(store.hasApplied(PROJECTION_ID, historyOfTheReplacement)).isFalse();

        recording.historyRead(replacement);
        AppendId writtenDuringTheReplacement = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(writtenDuringTheReplacement));

        assertThat(store.hasApplied(PROJECTION_ID, writtenDuringTheReplacement)).isTrue();
    }

    // Every delivery seen while the history is being read attempts the clear, but a clear that already succeeded is
    // not run again: a history of N events must clear once, not N times.
    @Test
    void a_catch_up_clears_the_store_at_most_once_across_every_delivery_of_its_history() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)));

        recording.catchupStarted(new Object());
        for (int i = 0; i < 1000; i++) {
            recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        }

        assertThat(clears).hasSize(1);
    }

    @Test
    void a_second_catch_up_clears_again() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)));

        Object first = new Object();
        recording.catchupStarted(first);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(1);

        recording.historyRead(first);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        recording.catchupStarted(new Object());
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(2);
    }

    // A stop parks a catch-up after its history has been read and a start reads that history again. Nothing goes
    // live in between, so the second catch-up's start is the only thing that can say so.
    @Test
    void a_catch_up_relaunched_after_its_history_was_read_clears_again_with_no_live_delivery_in_between() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)));

        Object parked = new Object();
        recording.catchupStarted(parked);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.historyRead(parked);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        assertThat(clears).hasSize(1);

        recording.catchupStarted(new Object());
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(2);
    }

    // The full state machine: a failing clear is retried on every delivery until it succeeds, and once it succeeds,
    // every later delivery in the same catch-up is left alone rather than retried.
    @Test
    void a_failing_clear_is_retried_every_delivery_until_it_succeeds_then_suppressed_for_the_rest_of_the_catch_up() {
        List<String> clears = new ArrayList<>();
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, clearShouldFail));

        recording.catchupStarted(new Object());
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

    // The clear stays a precondition after the history has been read, so a projection whose history read handled
    // nothing (an empty store, or a filter that matched none of it) still clears before it records anything.
    @Test
    void the_events_written_since_a_catch_up_started_clear_once_before_they_record_and_not_again_per_delivery() {
        List<String> clears = new ArrayList<>();
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, clearCountingStore(clears, new AtomicBoolean(false)));
        Object episode = new Object();

        recording.catchupStarted(episode);
        recording.historyRead(episode);
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));
        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(clears).hasSize(1);
    }

    // Recording an append while a clear is owed would only give that clear something more to delete, so it waits.
    // Without the wait the append is lost for good, since this catch-up is the only delivery it gets.
    @Test
    void an_append_handled_while_a_clear_is_owed_is_written_once_that_clear_succeeds() {
        List<String> clears = new ArrayList<>();
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(clears, clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object episode = new Object();

        recording.catchupStarted(episode);
        recording.historyRead(episode);
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

    // retryPendingClear runs the clear and deliberately writes nothing, since it is not told which catch-up the
    // projection is in. The next delivery is what has to notice the buffer is now writable.
    @Test
    void appends_waiting_for_a_clear_are_written_by_the_next_delivery_when_retryPendingClear_made_that_clear_succeed() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object episode = new Object();

        recording.catchupStarted(episode);
        recording.historyRead(episode);
        AppendId waiting = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(waiting));
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        clearShouldFail.set(false);
        recording.retryPendingClear();
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        recording.recordIfReady(metadataWithAppendId(AppendId.mint()));

        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isTrue();
    }

    // pollForClear does know, so it writes them itself. This is what keeps a projection that has gone quiet from
    // holding an append until its next delivery, which may never come.
    @Test
    void appends_waiting_for_a_clear_are_written_by_the_poll_tick_that_makes_that_clear_succeed() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object episode = new Object();

        recording.catchupStarted(episode);
        recording.historyRead(episode);
        AppendId waiting = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(waiting));
        assertThat(recording.pollForClear()).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        clearShouldFail.set(false);

        assertThat(recording.pollForClear()).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isTrue();
    }

    // A catch-up starting means the read model is being built from scratch, so anything a previous one was still
    // holding describes events that read model has not been given.
    @Test
    void appends_waiting_for_a_clear_are_dropped_when_the_next_catch_up_starts() {
        AtomicBoolean clearShouldFail = new AtomicBoolean(true);
        AppliedAppendStore store = clearCountingStore(new ArrayList<>(), clearShouldFail);
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);
        Object first = new Object();

        recording.catchupStarted(first);
        recording.historyRead(first);
        AppendId waiting = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(waiting));
        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();

        clearShouldFail.set(false);
        Object second = new Object();
        recording.catchupStarted(second);
        recording.historyRead(second);
        AppendId afterTheSecondCatchup = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(afterTheSecondCatchup));

        assertThat(store.hasApplied(PROJECTION_ID, waiting)).isFalse();
        assertThat(store.hasApplied(PROJECTION_ID, afterTheSecondCatchup)).isTrue();
    }

    @Test
    void a_projection_that_has_never_caught_up_records_every_append_it_is_given() {
        List<String> clears = new ArrayList<>();
        AppliedAppendStore store = clearCountingStore(clears, new AtomicBoolean(false));
        AppliedAppendRecording recording = new AppliedAppendRecording(PROJECTION_ID, store);

        AppendId first = AppendId.mint();
        AppendId second = AppendId.mint();
        recording.recordIfReady(metadataWithAppendId(first));
        recording.recordIfReady(metadataWithAppendId(second));

        assertThat(clears).isEmpty();
        assertThat(store.hasApplied(PROJECTION_ID, first)).isTrue();
        assertThat(store.hasApplied(PROJECTION_ID, second)).isTrue();
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
