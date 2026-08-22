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

package org.occurrent.dsl.projection.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.eventstore.api.AppendId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingMaterializedViewTest {

    private static final String PROJECTION_ID = "orderStatus";

    @Test
    void records_the_events_appendid_after_the_delegate_update_returns() {
        List<String> order = new ArrayList<>();
        MaterializedView<String> delegate = orderTrackingDelegate(order, "delegate");
        AppliedAppendStore store = orderTrackingStore(AppliedAppendStore.inMemory(), order, "record");

        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(delegate, PROJECTION_ID, store);

        recording.update(metadataWithAppendId(AppendId.mint()), "event");

        assertThat(order).containsExactly("delegate", "record");
    }

    @Test
    void nothing_is_recorded_while_a_catch_up_is_reading_its_history() {
        List<String> events = new ArrayList<>();
        MaterializedView<String> delegate = recordingDelegate(events);
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(delegate, PROJECTION_ID, store);

        recording.catchupStarted(new Object());
        recording.update(metadataWithAppendId(appendId), "event");

        assertThat(events).containsExactly("event");
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void nothing_is_recorded_when_the_delegate_throws() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        MaterializedView<String> delegate = throwingDelegate();

        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(delegate, PROJECTION_ID, store);

        assertThatCode(() -> recording.update(metadataWithAppendId(appendId), "event")).isInstanceOf(RuntimeException.class);
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void an_event_with_no_appendid_extension_is_skipped_without_throwing() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);

        assertThatCode(() -> recording.update(EventMetadata.empty(), "event")).doesNotThrowAnyException();
    }

    @Test
    void an_event_with_a_malformed_non_uuid_appendid_is_skipped_without_throwing() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);
        EventMetadata malformed = new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, "not-a-uuid"));

        assertThatCode(() -> recording.update(malformed, "event")).doesNotThrowAnyException();
    }

    @Test
    void a_repeated_appendid_is_written_once() {
        List<String> storeCalls = new ArrayList<>();
        AppliedAppendStore store = orderTrackingStore(AppliedAppendStore.inMemory(), storeCalls, "recordApplied");
        AppendId appendId = AppendId.mint();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);

        recording.update(metadataWithAppendId(appendId), "event1");
        recording.update(metadataWithAppendId(appendId), "event2");

        assertThat(storeCalls).containsExactly("recordApplied");
    }

    @Test
    void a_catch_up_clears_and_recording_resumes_once_its_history_has_been_read() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);
        Object episode = new Object();

        recording.catchupStarted(episode);
        recording.pollForClear();

        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();

        recording.historyRead(episode);
        AppendId after = AppendId.mint();
        recording.update(metadataWithAppendId(after), "event");

        assertThat(store.hasApplied(PROJECTION_ID, after)).isTrue();
    }

    @Test
    void a_failing_clear_leaves_the_recorder_non_recording_and_a_later_successful_clear_re_enables_it() {
        FlakyClearStore store = new FlakyClearStore();
        AppendId first = AppendId.mint();

        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);

        Object episode = new Object();
        recording.catchupStarted(episode);
        recording.historyRead(episode);
        recording.pollForClear(); // first clear attempt, which fails
        recording.update(metadataWithAppendId(first), "event-during-failed-clear");
        assertThat(store.hasApplied(PROJECTION_ID, first)).isFalse();

        store.clearShouldFail = false;
        AppendId second = AppendId.mint();
        recording.update(metadataWithAppendId(second), "event-after-successful-clear");

        assertThat(store.hasApplied(PROJECTION_ID, second)).isTrue();
    }

    @Test
    void the_dedup_slot_is_reset_across_a_successful_clear() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, store);

        recording.update(metadataWithAppendId(appendId), "event1");
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isTrue();

        Object episode = new Object();
        recording.catchupStarted(episode);
        recording.historyRead(episode);
        recording.pollForClear(); // clears the store and the dedup slot
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();

        // The same id, now absent from the store, must be written again rather than skipped as "already recorded".
        recording.update(metadataWithAppendId(appendId), "event2");
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isTrue();
    }

    @Test
    void nothing_is_recorded_for_a_delegate_that_reports_skipping_the_event() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(skippingDelegate(), PROJECTION_ID, store);

        recording.update(metadataWithAppendId(appendId), "event");

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void an_applied_event_is_still_recorded_when_the_delegate_can_report_skipping() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(applyingDelegate(), PROJECTION_ID, store);

        recording.update(metadataWithAppendId(appendId), "event");

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isTrue();
    }

    @Test
    void replayAware_lifecycle_is_forwarded_to_a_replayAware_delegate() {
        List<String> calls = new ArrayList<>();
        MaterializedView<String> delegate = new ReplayAwareDelegate(calls);
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(delegate, PROJECTION_ID, AppliedAppendStore.inMemory());

        recording.replayStarted();
        recording.replayCompleted();
        recording.replayAbandoned();

        assertThat(calls).containsExactly("started", "completed", "abandoned");
    }

    @Test
    void replayAware_lifecycle_does_not_throw_for_a_plain_delegate() {
        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(noopDelegate(), PROJECTION_ID, AppliedAppendStore.inMemory());

        assertThatCode(() -> {
            recording.replayStarted();
            recording.replayCompleted();
            recording.replayAbandoned();
        }).doesNotThrowAnyException();
    }

    private static EventMetadata metadataWithAppendId(AppendId appendId) {
        return new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()));
    }

    private static MaterializedView<String> noopDelegate() {
        return new MaterializedView<>() {
            @Override
            public void update(String event) {
            }
        };
    }

    // Mirrors CoalescingMaterializedView's own null-id skip, without needing a real ViewStateRepository.
    private static MaterializedView<String> skippingDelegate() {
        return new SkippableDelegate(false);
    }

    private static MaterializedView<String> applyingDelegate() {
        return new SkippableDelegate(true);
    }

    private static final class SkippableDelegate implements MaterializedView<String>, SkippableUpdate<String> {
        private final boolean applies;

        private SkippableDelegate(boolean applies) {
            this.applies = applies;
        }

        @Override
        public void update(String event) {
        }

        @Override
        public boolean applyReportingWhetherApplied(EventMetadata metadata, String event) {
            return applies;
        }
    }

    private static MaterializedView<String> throwingDelegate() {
        return new MaterializedView<>() {
            @Override
            public void update(String event) {
                throw new RuntimeException("delegate failed");
            }
        };
    }

    private static MaterializedView<String> recordingDelegate(List<String> events) {
        return new MaterializedView<>() {
            @Override
            public void update(String event) {
                events.add(event);
            }
        };
    }

    private static MaterializedView<String> orderTrackingDelegate(List<String> order, String marker) {
        return new MaterializedView<>() {
            @Override
            public void update(String event) {
                order.add(marker);
            }
        };
    }

    // Wraps a real AppliedAppendStore, appending marker to order whenever recordApplied is called, so ordering
    // against the delegate's own marker can be asserted.
    private static AppliedAppendStore orderTrackingStore(AppliedAppendStore delegate, List<String> order, String marker) {
        return new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
                order.add(marker);
                delegate.recordApplied(projectionId, appendId);
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                return delegate.hasApplied(projectionId, appendId);
            }

            @Override
            public void clear(String projectionId) {
                delegate.clear(projectionId);
            }
        };
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

    private static final class ReplayAwareDelegate implements MaterializedView<String>, ReplayAware {
        private final List<String> calls;

        private ReplayAwareDelegate(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public void update(String event) {
        }

        @Override
        public void replayStarted() {
            calls.add("started");
        }

        @Override
        public void replayCompleted() {
            calls.add("completed");
        }

        @Override
        public void replayAbandoned() {
            calls.add("abandoned");
        }
    }
}
