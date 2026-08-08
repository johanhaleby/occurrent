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
import org.occurrent.dsl.projection.AppliedPositionStorage;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingReactiveUpdateTest {

    @Test
    void a_live_update_advances_storage_with_the_events_position_after_the_delegate_mono_completes() {
        List<String> calls = new ArrayList<>();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = recordingDelegate(calls);
        AppliedPositionStorage storage = recordingStorage(calls);
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");

        recording.apply(metadataWithPosition(42), "event-1").block();

        assertThat(calls).containsExactly("delegate:event-1", "advance:42");
        assertThat(storage.appliedPosition("orders")).hasValue(42L);
    }

    @Test
    void an_event_with_no_position_errors_and_never_reaches_the_delegate() {
        List<String> calls = new ArrayList<>();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = recordingDelegate(calls);
        AppliedPositionStorage storage = recordingStorage(calls);
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");

        assertThatThrownBy(() -> recording.apply(EventMetadata.empty(), "event-1").block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders");
        assertThat(calls).isEmpty();
    }

    @Test
    void a_replay_buffers_the_highest_position_seen_and_advances_storage_once_in_replayCompleted_after_the_delegate_flushes() {
        List<String> calls = new ArrayList<>();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = recordingDelegate(calls);
        AppliedPositionStorage storage = recordingStorage(calls);
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");
        ReplayAwareMaterializedView replayAware = (ReplayAwareMaterializedView) recording;

        replayAware.replayStarted();
        recording.apply(metadataWithPosition(10), "event-1").block();
        recording.apply(metadataWithPosition(30), "event-2").block();
        recording.apply(metadataWithPosition(20), "event-3").block();
        assertThat(calls).containsExactly("delegate:event-1", "delegate:event-2", "delegate:event-3");
        replayAware.replayCompleted().block();

        assertThat(calls).containsExactly("delegate:event-1", "delegate:event-2", "delegate:event-3", "advance:30");
        assertThat(storage.appliedPosition("orders")).hasValue(30L);
    }

    @Test
    void a_replay_that_is_abandoned_discards_the_buffered_position_instead_of_advancing_storage() {
        List<String> calls = new ArrayList<>();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = recordingDelegate(calls);
        AppliedPositionStorage storage = recordingStorage(calls);
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");
        ReplayAwareMaterializedView replayAware = (ReplayAwareMaterializedView) recording;

        replayAware.replayStarted();
        recording.apply(metadataWithPosition(10), "event-1").block();
        replayAware.replayAbandoned();

        assertThat(calls).containsExactly("delegate:event-1");
        assertThat(storage.appliedPosition("orders")).isEmpty();
    }

    @Test
    void a_delegate_that_is_itself_replay_aware_is_flushed_before_storage_advances() {
        List<String> calls = new ArrayList<>();
        DelegateWithReplayAwareness delegate = new DelegateWithReplayAwareness(calls);
        AppliedPositionStorage storage = recordingStorage(calls);
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");
        ReplayAwareMaterializedView replayAware = (ReplayAwareMaterializedView) recording;

        replayAware.replayStarted();
        recording.apply(metadataWithPosition(10), "event-1").block();
        replayAware.replayCompleted().block();

        assertThat(calls).containsExactly("delegate:replayStarted", "delegate:event-1", "delegate:replayCompleted", "advance:10");
    }

    @Test
    void advance_never_moves_the_recorded_position_backwards() {
        AppliedPositionStorage storage = AppliedPositionStorage.inMemory();
        BiFunction<EventMetadata, String, Mono<Void>> delegate = recordingDelegate(new ArrayList<>());
        BiFunction<EventMetadata, String, Mono<Void>> recording = Projections.recordingAppliedPosition(delegate, storage, "orders");

        recording.apply(metadataWithPosition(50), "event-1").block();
        recording.apply(metadataWithPosition(10), "event-2").block();

        assertThat(storage.appliedPosition("orders")).hasValue(50L);
    }

    private static EventMetadata metadataWithPosition(long position) {
        return new EventMetadata(Map.of("position", position));
    }

    private static BiFunction<EventMetadata, String, Mono<Void>> recordingDelegate(List<String> calls) {
        return (metadata, event) -> Mono.fromRunnable(() -> calls.add("delegate:" + event));
    }

    private static final class DelegateWithReplayAwareness implements BiFunction<EventMetadata, String, Mono<Void>>, ReplayAwareMaterializedView {
        private final List<String> calls;

        DelegateWithReplayAwareness(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public Mono<Void> apply(EventMetadata metadata, String event) {
            return Mono.fromRunnable(() -> calls.add("delegate:" + event));
        }

        @Override
        public void replayStarted() {
            calls.add("delegate:replayStarted");
        }

        @Override
        public Mono<Void> replayCompleted() {
            return Mono.fromRunnable(() -> calls.add("delegate:replayCompleted"));
        }

        @Override
        public void replayAbandoned() {
            calls.add("delegate:replayAbandoned");
        }
    }

    // A storage that both persists in memory and appends "advance:<position>" to the shared call log, so ordering
    // against the delegate's own log entries can be asserted.
    private static AppliedPositionStorage recordingStorage(List<String> calls) {
        AppliedPositionStorage inMemory = AppliedPositionStorage.inMemory();
        return new AppliedPositionStorage() {
            @Override
            public OptionalLong appliedPosition(String projectionId) {
                return inMemory.appliedPosition(projectionId);
            }

            @Override
            public void advance(String projectionId, long position) {
                calls.add("advance:" + position);
                inMemory.advance(projectionId, position);
            }
        };
    }
}
