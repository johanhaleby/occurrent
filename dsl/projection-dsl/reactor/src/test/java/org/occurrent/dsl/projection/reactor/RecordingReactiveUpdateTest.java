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

package org.occurrent.dsl.projection.reactor;

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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingReactiveUpdateTest {

    private static final String PROJECTION_ID = "orderStatus";

    @Test
    void records_the_events_appendid_after_the_delegates_mono_completes() {
        List<String> order = new ArrayList<>();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = orderTrackingDelegate(order, "delegate");
        AppliedAppendStore store = orderTrackingStore(AppliedAppendStore.inMemory(), order, "record");

        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(delegate, PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(metadataWithAppendId(AppendId.mint()), "event")).verifyComplete();

        assertThat(order).containsExactly("delegate", "record");
    }

    @Test
    void nothing_is_recorded_while_the_phase_says_replaying() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, () -> CatchupSnapshot.readingHistory(1L));

        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event")).verifyComplete();

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void nothing_is_recorded_when_the_delegates_mono_errors() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = (metadata, event) -> Mono.error(new RuntimeException("delegate failed"));
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(delegate, PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event")).verifyError(RuntimeException.class);

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void an_event_with_no_appendid_extension_is_skipped_without_erroring() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(EventMetadata.empty(), "event")).verifyComplete();
    }

    @Test
    void an_event_with_a_malformed_non_uuid_appendid_is_skipped_without_erroring() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());
        EventMetadata malformed = new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, "not-a-uuid"));

        StepVerifier.create(recording.apply(malformed, "event")).verifyComplete();
    }

    @Test
    void a_repeated_appendid_is_written_once() {
        List<String> storeCalls = new ArrayList<>();
        AppliedAppendStore store = orderTrackingStore(AppliedAppendStore.inMemory(), storeCalls, "recordApplied");
        AppendId appendId = AppendId.mint();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event1")).verifyComplete();
        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event2")).verifyComplete();

        assertThat(storeCalls).containsExactly("recordApplied");
    }

    @Test
    void replayObserved_clears_and_recording_resumes() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId before = AppendId.mint();
        store.recordApplied(PROJECTION_ID, before);
        AtomicBoolean replaying = new AtomicBoolean(false);
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, () -> replaying.get() ? CatchupSnapshot.readingHistory(1L) : CatchupSnapshot.LIVE);

        recording.replayObserved();

        assertThat(store.hasApplied(PROJECTION_ID, before)).isFalse();

        AppendId after = AppendId.mint();
        StepVerifier.create(recording.apply(metadataWithAppendId(after), "event")).verifyComplete();

        assertThat(store.hasApplied(PROJECTION_ID, after)).isTrue();
    }

    @Test
    void a_failing_clear_leaves_the_recorder_non_recording_and_a_later_successful_clear_re_enables_it() {
        FlakyClearStore store = new FlakyClearStore();
        AppendId first = AppendId.mint();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(noopDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());

        recording.replayObserved(); // marks pendingClear, first clear attempt fails
        StepVerifier.create(recording.apply(metadataWithAppendId(first), "event-during-failed-clear")).verifyComplete();
        assertThat(store.hasApplied(PROJECTION_ID, first)).isFalse();

        store.clearShouldFail = false;
        AppendId second = AppendId.mint();
        StepVerifier.create(recording.apply(metadataWithAppendId(second), "event-after-successful-clear")).verifyComplete();

        assertThat(store.hasApplied(PROJECTION_ID, second)).isTrue();
    }

    @Test
    void nothing_is_recorded_for_a_delegate_that_reports_skipping_the_event() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(skippingDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event")).verifyComplete();

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
    }

    @Test
    void an_applied_event_is_still_recorded_when_the_delegate_can_report_skipping() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();
        RecordingReactiveUpdate<String> recording = new RecordingReactiveUpdate<>(applyingDelegate(), PROJECTION_ID, store, ReplayPhase.neverReplays());

        StepVerifier.create(recording.apply(metadataWithAppendId(appendId), "event")).verifyComplete();

        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isTrue();
    }

    @Test
    void replayCompleted_chains_the_delegates_mono_first() {
        List<String> order = new ArrayList<>();
        ReactiveReplayAware delegate = new ReactiveReplayAware() {
            @Override
            public void replayStarted() {
                order.add("delegate-started");
            }

            @Override
            public Mono<Void> replayCompleted() {
                return Mono.<Void>empty().doOnSuccess(ignored -> order.add("delegate-completed"));
            }

            @Override
            public void replayAbandoned() {
                order.add("delegate-abandoned");
            }
        };
        BiFunction<EventMetadata, String, Mono<Void>> update = (metadata, event) -> Mono.empty();
        RecordingReactiveUpdate<String> recording = newRecordingWithDelegateLifecycle(update, delegate);

        recording.replayStarted();
        StepVerifier.create(recording.replayCompleted()).verifyComplete();
        recording.replayAbandoned();

        assertThat(order).containsExactly("delegate-started", "delegate-completed", "delegate-abandoned");
    }

    private static RecordingReactiveUpdate<String> newRecordingWithDelegateLifecycle(BiFunction<EventMetadata, String, Mono<Void>> update, ReactiveReplayAware lifecycle) {
        BiFunction<EventMetadata, String, Mono<Void>> delegate = new DelegateWithLifecycle(update, lifecycle);
        return new RecordingReactiveUpdate<>(delegate, PROJECTION_ID, AppliedAppendStore.inMemory(), ReplayPhase.neverReplays());
    }

    private static final class DelegateWithLifecycle implements BiFunction<EventMetadata, String, Mono<Void>>, ReactiveReplayAware {
        private final BiFunction<EventMetadata, String, Mono<Void>> update;
        private final ReactiveReplayAware lifecycle;

        private DelegateWithLifecycle(BiFunction<EventMetadata, String, Mono<Void>> update, ReactiveReplayAware lifecycle) {
            this.update = update;
            this.lifecycle = lifecycle;
        }

        @Override
        public Mono<Void> apply(EventMetadata metadata, String event) {
            return update.apply(metadata, event);
        }

        @Override
        public void replayStarted() {
            lifecycle.replayStarted();
        }

        @Override
        public Mono<Void> replayCompleted() {
            return lifecycle.replayCompleted();
        }

        @Override
        public void replayAbandoned() {
            lifecycle.replayAbandoned();
        }
    }

    private static EventMetadata metadataWithAppendId(AppendId appendId) {
        return new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()));
    }

    private static BiFunction<EventMetadata, String, Mono<Void>> noopDelegate() {
        return (metadata, event) -> Mono.empty();
    }

    // Mirrors CoalescingMaterializedUpdate's own null-id skip, without needing a real ViewStateRepository.
    private static BiFunction<EventMetadata, String, Mono<Void>> skippingDelegate() {
        return new SkippableDelegate(false);
    }

    private static BiFunction<EventMetadata, String, Mono<Void>> applyingDelegate() {
        return new SkippableDelegate(true);
    }

    private static final class SkippableDelegate implements BiFunction<EventMetadata, String, Mono<Void>>, SkippableUpdate<String> {
        private final boolean applies;

        private SkippableDelegate(boolean applies) {
            this.applies = applies;
        }

        @Override
        public Mono<Void> apply(EventMetadata metadata, String event) {
            return Mono.empty();
        }

        @Override
        public Mono<Boolean> applyReportingWhetherApplied(EventMetadata metadata, String event) {
            return Mono.just(applies);
        }
    }

    private static BiFunction<EventMetadata, String, Mono<Void>> orderTrackingDelegate(List<String> order, String marker) {
        return (metadata, event) -> Mono.<Void>empty().doOnSuccess(ignored -> order.add(marker));
    }

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
