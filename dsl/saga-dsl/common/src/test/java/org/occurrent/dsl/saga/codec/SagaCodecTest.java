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

package org.occurrent.dsl.saga.codec;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaCodecTest {

    private static final Instant CREATED = Instant.ofEpochMilli(1_700_000_000_000L);
    private static final Instant UPDATED = Instant.ofEpochMilli(1_700_000_001_000L);
    private static final Instant COMPLETED = Instant.ofEpochMilli(1_700_000_002_000L);
    private static final Instant FAILED_AT = Instant.ofEpochMilli(1_700_000_003_000L);
    private static final Instant RELEASED_AT = Instant.ofEpochMilli(1_700_000_004_000L);

    private final SagaCodec<String> codec = SagaCodec.of(new PrefixingStateCodec(), null);

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class RoundTrip {

        @Test
        void an_envelope_with_every_component_set_decodes_to_itself() {
            SagaEnvelope<String> envelope = everythingSet();

            assertThat(codec.decode(codec.encode(envelope))).isEqualTo(envelope);
        }

        @Test
        void an_envelope_with_every_optional_component_unset_decodes_to_itself() {
            SagaEnvelope<String> envelope = bare("s1", "state");

            assertThat(codec.decode(codec.encode(envelope))).isEqualTo(envelope);
        }

        @Test
        void the_state_goes_through_the_state_codec() {
            Map<String, Object> fields = codec.encode(bare("s1", "state")).encoded().fields();

            assertThat(fields).containsEntry(SagaCodec.STATE, "enc:state");
        }

        @Test
        void the_timers_keep_their_order_and_the_next_attempt_only_when_one_is_set() {
            SagaEnvelope<String> envelope = withTimers(bare("s1", "state"), new TimerEntry("b", 20L, 25L), new TimerEntry("a", 10L, null));

            StoredSaga stored = codec.encode(envelope);

            assertThat(codec.decode(stored).timers()).containsExactly(new TimerEntry("b", 20L, 25L), new TimerEntry("a", 10L, null));
            assertThat(stored.encoded().fields().get(SagaCodec.TIMERS)).isEqualTo(List.of(
                    Map.of("name", "b", "firesAtEpochMilli", 20L, "nextAttemptAtEpochMilli", 25L), Map.of("name", "a", "firesAtEpochMilli", 10L)));
        }

        @Test
        void the_stream_watermarks_are_written_with_their_keys_sorted() {
            Map<String, Long> watermarks = new LinkedHashMap<>();
            watermarks.put("z-stream", 3L);
            watermarks.put("a-stream", 1L);
            watermarks.put("m-stream", 2L);
            SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", "state", SagaStatus.ACTIVE, 0, List.of(), watermarks, null, null, null, null, null, true, null, Map.of(), null);

            Object stored = codec.encode(envelope).encoded().fields().get(SagaCodec.STREAM_WATERMARKS);

            assertThat(((Map<?, ?>) stored).keySet().stream().map(String::valueOf).toList()).containsExactly("a-stream", "m-stream", "z-stream");
        }

        @Test
        void the_stream_marks_are_written_with_their_streams_types_and_pending_sorted() {
            Map<String, TypeMark> byType = new LinkedHashMap<>();
            byType.put("z-type", new TypeMark(5, List.of(1L, 2L), 0));
            byType.put("a-type", new TypeMark(4, List.of(), 0));
            Map<String, List<Long>> pending = new LinkedHashMap<>();
            pending.put("z-type", List.of(9L));
            pending.put("a-type", List.of(8L));
            Map<String, StreamMarks> marks = new LinkedHashMap<>();
            marks.put("z-stream", new StreamMarks(1, byType, pending));
            marks.put("a-stream", new StreamMarks(1, Map.of()));
            SagaEnvelope<String> envelope = withMarks(bare("s1", "state"), marks);

            List<?> streams = (List<?>) codec.encode(envelope).encoded().fields().get(SagaCodec.STREAM_MARKS);

            Map<?, ?> zStream = (Map<?, ?>) streams.get(1);
            assertThat(streams.stream().map(stream -> String.valueOf(((Map<?, ?>) stream).get("streamId")))).containsExactly("a-stream", "z-stream");
            assertThat(((List<?>) zStream.get("types")).stream().map(type -> String.valueOf(((Map<?, ?>) type).get("type")))).containsExactly("a-type", "z-type");
            assertThat(((List<?>) zStream.get("pending")).stream().map(type -> String.valueOf(((Map<?, ?>) type).get("type")))).containsExactly("a-type", "z-type");
        }

        @Test
        void every_field_of_the_stream_marks_round_trips() {
            Map<String, StreamMarks> marks = Map.of("stream-1", new StreamMarks(7,
                    Map.of("t1", new TypeMark(12, List.of(9L, 10L), 8), "t2", new TypeMark(3, List.of(), Long.MIN_VALUE)),
                    Map.of("t1", List.of(11L, 13L)),
                    List.of(14L, 15L),
                    Map.of(11L, Instant.ofEpochMilli(1_700_000_005_000L), 13L, Instant.ofEpochMilli(1_700_000_006_000L))));
            SagaEnvelope<String> envelope = withMarks(bare("s1", "state"), marks);

            assertThat(codec.decode(codec.encode(envelope)).streamMarks()).isEqualTo(marks);
        }

        @Test
        void a_pending_since_with_sub_millisecond_precision_is_stored_and_read_back_at_milliseconds() {
            Instant since = Instant.ofEpochSecond(1_700_000_005L, 123_456_789L);
            Map<String, StreamMarks> marks = Map.of("stream-1", new StreamMarks(1, Map.of(), Map.of("t", List.of(2L)), List.of(), Map.of(2L, since)));
            StoredSaga stored = codec.encode(withMarks(bare("s1", "state"), marks));

            Instant truncated = Instant.ofEpochSecond(1_700_000_005L, 123_000_000L);
            Map<?, ?> stream = (Map<?, ?>) ((List<?>) stored.encoded().fields().get(SagaCodec.STREAM_MARKS)).get(0);
            Map<?, ?> entry = (Map<?, ?>) ((List<?>) stream.get("pendingSince")).get(0);
            assertThat(entry.get("since")).isEqualTo(truncated);
            assertThat(codec.decode(stored).streamMarks().get("stream-1").pendingSince()).containsEntry(2L, truncated);
        }

        @Test
        void started_is_written_only_when_it_is_false() {
            SagaEnvelope<String> started = bare("s1", "state");
            SagaEnvelope<String> notStarted = new SagaEnvelope<>("s1", "state", SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null, false, null, Map.of(), null);

            assertThat(codec.encode(started).encoded().fields()).doesNotContainKey(SagaCodec.STARTED);
            assertThat(codec.encode(notStarted).encoded().fields()).containsEntry(SagaCodec.STARTED, false);
            assertThat(codec.decode(codec.encode(notStarted)).started()).isFalse();
        }

        @Test
        void a_failure_with_every_field_set_round_trips_and_is_written_field_by_field() {
            SagaFailure failure = new SagaFailure("input-1", 42L, FAILED_AT, "java.lang.IllegalStateException", "boom", RELEASED_AT, "q-1");
            SagaEnvelope<String> envelope = withFailure(bare("s1", "state"), failure);

            StoredSaga stored = codec.encode(envelope);

            assertThat(codec.decode(stored).failure()).isEqualTo(failure);
            assertThat(stored.encoded().fields())
                    .containsEntry(SagaCodec.FAILURE_INPUT, "input-1")
                    .containsEntry(SagaCodec.FAILURE_POSITION, 42L)
                    .containsEntry(SagaCodec.FAILURE_FIRST_FAILED_AT, FAILED_AT.toEpochMilli())
                    .containsEntry(SagaCodec.FAILURE_TYPE, "java.lang.IllegalStateException")
                    .containsEntry(SagaCodec.FAILURE_MESSAGE, "boom")
                    .containsEntry(SagaCodec.FAILURE_RELEASED_AT, RELEASED_AT.toEpochMilli())
                    .containsEntry(SagaCodec.FAILURE_QUARANTINE_ID, "q-1");
        }

        @Test
        void a_failure_with_its_optional_fields_unset_writes_none_of_them() {
            SagaFailure failure = new SagaFailure("input-1", null, FAILED_AT, "java.lang.IllegalStateException", null, null, null);
            SagaEnvelope<String> envelope = withFailure(bare("s1", "state"), failure);

            StoredSaga stored = codec.encode(envelope);

            assertThat(codec.decode(stored).failure()).isEqualTo(failure);
            assertThat(stored.encoded().fields()).doesNotContainKeys(SagaCodec.FAILURE_POSITION, SagaCodec.FAILURE_MESSAGE,
                    SagaCodec.FAILURE_RELEASED_AT, SagaCodec.FAILURE_QUARANTINE_ID);
        }

        @Test
        void no_failure_writes_no_failure_field() {
            Map<String, Object> fields = codec.encode(bare("s1", "state")).encoded().fields();

            assertThat(fields.keySet()).noneMatch(key -> key.startsWith("failure"));
            assertThat(codec.decode(codec.encode(bare("s1", "state"))).failure()).isNull();
        }

        @Test
        void the_quarantine_skip_count_round_trips_and_is_absent_when_unset() {
            SagaEnvelope<String> counted = withSkipCount(bare("s1", "state"), 6L);

            assertThat(codec.decode(codec.encode(counted)).quarantineSkipCount()).isEqualTo(6L);
            assertThat(codec.encode(counted).encoded().fields()).containsEntry(SagaCodec.QUARANTINE_SKIP_COUNT, 6L);
            assertThat(codec.encode(bare("s1", "state")).encoded().fields()).doesNotContainKey(SagaCodec.QUARANTINE_SKIP_COUNT);
        }

        @Test
        void the_position_watermark_and_the_instants_are_stored_as_epoch_millis() {
            Map<String, Object> fields = codec.encode(everythingSet()).encoded().fields();

            assertThat(fields)
                    .containsEntry(SagaCodec.POSITION_WATERMARK, 77L)
                    .containsEntry(SagaCodec.CREATED_AT, CREATED.toEpochMilli())
                    .containsEntry(SagaCodec.UPDATED_AT, UPDATED.toEpochMilli())
                    .containsEntry(SagaCodec.COMPLETED_AT, COMPLETED.toEpochMilli())
                    .containsEntry(SagaCodec.VERSION, 3L);
        }

        @Test
        void the_version_of_the_stored_saga_is_the_version_of_the_envelope() {
            assertThat(codec.encode(everythingSet()).version()).isEqualTo(3L);
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class StoredStatus {

        @Test
        void a_quarantine_with_an_id_is_stored_as_quarantined_recording_and_reads_back_as_quarantined() {
            SagaEnvelope<String> envelope = withSkipCount(quarantined("q-1"), 0L);

            StoredSaga stored = codec.encode(envelope);

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.STATUS, SagaCodec.QUARANTINED_RECORDING);
            assertThat(codec.decode(stored).status()).isEqualTo(SagaStatus.QUARANTINED);
        }

        @Test
        void a_quarantine_without_an_id_keeps_the_plain_status_name() {
            StoredSaga stored = codec.encode(quarantined(null));

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.STATUS, "QUARANTINED");
            assertThat(codec.decode(stored).status()).isEqualTo(SagaStatus.QUARANTINED);
        }

        @Test
        void decode_refuses_a_quarantined_recording_instance_that_lacks_its_quarantine_id() {
            StoredSaga stored = withoutField(codec.encode(quarantined("q-1")), "s1", SagaCodec.FAILURE_QUARANTINE_ID);

            Throwable thrown = catchThrowable(() -> codec.decode(stored));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("s1").hasMessageContaining("failureQuarantineId");
        }

        @Test
        void decode_refuses_a_quarantined_recording_instance_that_lacks_its_failure_input() {
            StoredSaga stored = withoutField(codec.encode(quarantined("q-1")), "s1", SagaCodec.FAILURE_INPUT);

            Throwable thrown = catchThrowable(() -> codec.decode(stored));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("s1").hasMessageContaining(SagaCodec.FAILURE_INPUT);
        }

        @Test
        void decode_refuses_a_quarantined_recording_instance_that_lacks_its_skip_count() {
            StoredSaga stored = withoutField(codec.encode(withSkipCount(quarantined("q-1"), 0L)), "s1", SagaCodec.QUARANTINE_SKIP_COUNT);

            Throwable thrown = catchThrowable(() -> codec.decode(stored));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("s1").hasMessageContaining("quarantineSkipCount");
        }

        @Test
        void decodeWithoutState_refuses_a_quarantined_recording_instance_that_lacks_its_skip_count() {
            StoredSaga stored = withoutField(codec.encode(withSkipCount(quarantined("q-1"), 0L)), "s1", SagaCodec.QUARANTINE_SKIP_COUNT);

            Throwable thrown = catchThrowable(() -> codec.decodeWithoutState(stored));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("s1").hasMessageContaining("quarantineSkipCount");
        }

        @Test
        void a_complete_quarantined_recording_instance_still_decodes() {
            SagaEnvelope<String> envelope = withSkipCount(quarantined("q-1"), 0L);
            StoredSaga stored = codec.encode(envelope);

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.STATUS, SagaCodec.QUARANTINED_RECORDING);
            assertThat(codec.decode(stored)).isEqualTo(envelope);
        }

        @Test
        void a_plain_quarantined_instance_without_an_id_or_skip_count_still_decodes() {
            StoredSaga stored = codec.encode(quarantined(null));

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.STATUS, "QUARANTINED").doesNotContainKeys(SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.QUARANTINE_SKIP_COUNT);
            assertThat(codec.decode(stored).status()).isEqualTo(SagaStatus.QUARANTINED);
            assertThat(codec.decodeWithoutState(stored).status()).isEqualTo(SagaStatus.QUARANTINED);
        }

        @Test
        void an_active_instance_with_stream_marks_is_stored_as_active_marked_and_reads_back_as_active() {
            SagaEnvelope<String> envelope = withMarks(bare("s1", "state"), Map.of("stream-1", new StreamMarks(1, Map.of())));

            StoredSaga stored = codec.encode(envelope);

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.STATUS, SagaCodec.ACTIVE_MARKED);
            assertThat(codec.decode(stored).status()).isEqualTo(SagaStatus.ACTIVE);
        }

        @Test
        void the_status_is_stored_under_the_field_name_status() {
            assertThat(codec.encode(bare("s1", "state")).encoded().fields()).containsEntry("status", "ACTIVE");
        }

        @Test
        void an_active_instance_without_stream_marks_keeps_the_plain_status_name() {
            assertThat(codec.encode(bare("s1", "state")).encoded().fields()).containsEntry(SagaCodec.STATUS, "ACTIVE");
        }

        @Test
        void a_completed_instance_with_stream_marks_keeps_the_plain_status_name() {
            SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", "state", SagaStatus.COMPLETED, 0, List.of(), Map.of(), null, null, null, null, null, true, null,
                    Map.of("stream-1", new StreamMarks(1, Map.of())), null);

            assertThat(codec.encode(envelope).encoded().fields()).containsEntry(SagaCodec.STATUS, "COMPLETED");
        }

        @Test
        void a_quarantine_with_an_id_and_stream_marks_is_stored_as_quarantined_recording() {
            SagaFailure failure = new SagaFailure("i", null, FAILED_AT, "t", null, null, "q-1");
            SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", "state", SagaStatus.QUARANTINED, 0, List.of(), Map.of(), null, null, null, null, null, true, failure,
                    Map.of("stream-1", new StreamMarks(1, Map.of())), null);

            assertThat(codec.encode(envelope).encoded().fields()).containsEntry(SagaCodec.STATUS, SagaCodec.QUARANTINED_RECORDING);
        }

        @Test
        void the_two_sentinels_are_the_strings_a_0_34_0_node_does_not_know() {
            assertThat(SagaCodec.QUARANTINED_RECORDING).isEqualTo("QUARANTINED_RECORDING");
            assertThat(SagaCodec.ACTIVE_MARKED).isEqualTo("ACTIVE_MARKED");
            assertThat(SagaStatus.values()).extracting(Enum::name).doesNotContain(SagaCodec.QUARANTINED_RECORDING, SagaCodec.ACTIVE_MARKED);
        }

        @Test
        void a_stored_status_that_is_neither_a_status_nor_a_sentinel_is_refused() {
            StoredSaga stored = stored("s1", Map.of(SagaCodec.STATUS, "NO_SUCH_STATUS", SagaCodec.VERSION, 0L));

            assertThatThrownBy(() -> codec.decode(stored)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void storedStatuses_names_both_stored_forms_of_active_and_of_quarantined_and_one_of_each_other_status() {
            assertThat(SagaCodec.storedStatuses(SagaStatus.ACTIVE)).containsExactlyInAnyOrder("ACTIVE", SagaCodec.ACTIVE_MARKED);
            assertThat(SagaCodec.storedStatuses(SagaStatus.QUARANTINED)).containsExactlyInAnyOrder("QUARANTINED", SagaCodec.QUARANTINED_RECORDING);
            assertThat(SagaCodec.storedStatuses(SagaStatus.COMPLETED)).containsExactly("COMPLETED");
        }

        @Test
        void every_status_is_found_under_the_stored_statuses_it_encodes_to() {
            for (SagaStatus status : SagaStatus.values()) {
                SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", "state", status, 0, List.of(), Map.of(), null, null, null, null, null, true, null, Map.of(), null);

                assertThat(SagaCodec.storedStatuses(status)).contains(codec.encode(envelope).index().storedStatus());
            }
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class Index {

        @Test
        void the_index_carries_the_stored_status_the_earliest_due_timer_and_the_update_time() {
            SagaEnvelope<String> envelope = withTimers(withUpdatedAt(bare("s1", "state"), UPDATED), new TimerEntry("late", 50L, null), new TimerEntry("soon", 10L, 30L));

            SagaIndex index = codec.encode(envelope).index();

            assertThat(index.storedStatus()).isEqualTo("ACTIVE");
            assertThat(index.nextTimerDueAtEpochMilli()).isEqualTo(30L);
            assertThat(index.updatedAtEpochMilli()).isEqualTo(UPDATED.toEpochMilli());
            assertThat(index.releasing()).isFalse();
            assertThat(index.quarantineId()).isNull();
        }

        @Test
        void an_instance_without_timers_or_update_time_has_neither_in_its_index() {
            SagaIndex index = codec.encode(bare("s1", "state")).index();

            assertThat(index.nextTimerDueAtEpochMilli()).isNull();
            assertThat(index.updatedAtEpochMilli()).isNull();
        }

        @Test
        void a_quarantine_that_has_a_release_time_is_releasing_and_names_its_quarantine_id() {
            SagaFailure failure = new SagaFailure("i", null, FAILED_AT, "t", null, RELEASED_AT, "q-1");

            SagaIndex index = codec.encode(withFailure(quarantined("q-1"), failure)).index();

            assertThat(index.releasing()).isTrue();
            assertThat(index.quarantineId()).isEqualTo("q-1");
        }

        @Test
        void a_quarantine_without_a_release_time_is_not_releasing() {
            SagaFailure failure = new SagaFailure("i", null, FAILED_AT, "t", null, null, "q-1");

            assertThat(codec.encode(withFailure(quarantined("q-1"), failure)).index().releasing()).isFalse();
        }

        @Test
        void an_instance_that_is_not_quarantined_is_not_releasing_even_with_a_release_time() {
            SagaFailure failure = new SagaFailure("i", null, FAILED_AT, "t", null, RELEASED_AT, null);

            assertThat(codec.encode(withFailure(bare("s1", "state"), failure)).index().releasing()).isFalse();
        }

        @Test
        void indexOf_the_stored_fields_of_a_releasing_quarantine_gives_its_stored_status_due_timer_update_time_and_quarantine_id() {
            StoredSaga stored = codec.encode(withFailure(withTimers(withUpdatedAt(quarantined("q-1"), UPDATED), new TimerEntry("t", 10L, null)),
                    new SagaFailure("i", null, FAILED_AT, "t", null, RELEASED_AT, "q-1")));

            SagaIndex index = SagaCodec.indexOf(stored.encoded());

            assertThat(index).isEqualTo(new SagaIndex("QUARANTINED_RECORDING", 10L, 1_700_000_001_000L, true, "q-1"));
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class WithoutState {

        @Test
        void the_encoding_without_state_writes_neither_the_state_nor_the_current_step() {
            SagaCodec<FlowState<Tick>> flowCodec = SagaCodec.of(SagaStateCodec.identity(), new TickConverter());
            SagaEnvelope<FlowState<Tick>> envelope = flowEnvelope(flowState());

            assertThat(flowCodec.encode(envelope).encoded().fields()).containsKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP);
            Map<String, Object> fields = flowCodec.encodeWithoutState(envelope).encoded().fields();

            assertThat(fields).doesNotContainKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP);
        }

        @Test
        void the_encoding_without_state_keeps_every_other_field() {
            SagaEnvelope<String> envelope = everythingSet();

            Map<String, Object> full = codec.encode(envelope).encoded().fields();
            Map<String, Object> without = codec.encodeWithoutState(envelope).encoded().fields();

            Map<String, Object> expected = new LinkedHashMap<>(full);
            expected.remove(SagaCodec.STATE);
            expected.remove(SagaCodec.CURRENT_STEP);
            assertThat(without).isEqualTo(expected);
        }

        @Test
        void the_encoding_without_state_has_the_same_index_as_the_full_encoding() {
            SagaEnvelope<String> envelope = everythingSet();

            assertThat(codec.encodeWithoutState(envelope).index()).isEqualTo(codec.encode(envelope).index());
        }

        @Test
        void decodeWithoutState_gives_no_state_even_when_the_stored_state_is_present_and_would_not_decode() {
            SagaCodec<String> refusing = SagaCodec.of(new SagaStateCodec<>() {
                @Override
                public Object encode(String state) {
                    return state;
                }

                @Override
                public String decode(Object encoded) {
                    throw new IllegalStateException("the state is no longer readable");
                }
            }, null);
            StoredSaga stored = codec.encode(bare("s1", "state"));

            assertThat(refusing.decodeWithoutState(stored).state()).isNull();
            assertThatThrownBy(() -> refusing.decode(stored)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void a_decode_of_a_document_that_has_no_state_gives_a_null_state_and_the_stored_current_step() {
            Map<String, Object> fields = new LinkedHashMap<>(codec.encode(bare("s1", "state")).encoded().fields());
            fields.remove(SagaCodec.STATE);
            fields.put(SagaCodec.CURRENT_STEP, "waiting");

            SagaEnvelope<String> decoded = codec.decode(stored("s1", fields));

            assertThat(decoded.state()).isNull();
            assertThat(decoded.currentStep()).isEqualTo("waiting");
        }

        @Test
        void the_fields_to_read_without_the_state_are_the_instance_fields_and_the_watermarks_and_marks() {
            assertThat(SagaCodec.fieldsWithoutState()).doesNotContain(SagaCodec.STATE)
                    .containsAll(SagaCodec.instanceFields())
                    .contains(SagaCodec.STREAM_WATERMARKS, SagaCodec.POSITION_WATERMARK, SagaCodec.STREAM_MARKS)
                    .doesNotHaveDuplicates();
            assertThat(SagaCodec.instanceFields()).doesNotContain(SagaCodec.STATE, SagaCodec.STREAM_WATERMARKS, SagaCodec.POSITION_WATERMARK, SagaCodec.STREAM_MARKS);
        }

        @Test
        void the_fields_a_save_without_the_state_sets_exclude_the_state_and_the_current_step() {
            assertThat(SagaCodec.fieldsExceptState()).doesNotContain(SagaCodec.STATE, SagaCodec.CURRENT_STEP)
                    .contains(SagaCodec.STATUS, SagaCodec.VERSION, SagaCodec.TIMERS, SagaCodec.STREAM_MARKS, SagaCodec.QUARANTINE_SKIP_COUNT,
                            SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.STARTED)
                    .doesNotHaveDuplicates();
        }

        @Test
        void every_field_the_encoding_without_state_can_write_is_one_of_the_fields_a_save_without_the_state_sets() {
            SagaEnvelope<String> envelope = withSkipCount(withFailure(withMarks(withTimers(everythingSet(), new TimerEntry("t", 1L, null)),
                            Map.of("stream-1", new StreamMarks(1, Map.of()))),
                    new SagaFailure("i", 1L, FAILED_AT, "t", "m", RELEASED_AT, "q")), 2L);
            SagaEnvelope<String> notStarted = new SagaEnvelope<>(envelope.sagaId(), envelope.state(), envelope.status(), envelope.version(), envelope.timers(),
                    envelope.streamWatermarks(), envelope.positionWatermark(), envelope.createdAt(), envelope.updatedAt(), envelope.completedAt(), null, false,
                    envelope.failure(), envelope.streamMarks(), envelope.quarantineSkipCount());

            assertThat(SagaCodec.fieldsExceptState()).containsAll(codec.encodeWithoutState(notStarted).encoded().fields().keySet());
        }

        @Test
        void the_instance_fields_and_the_watermarks_marks_and_state_cover_every_field_of_a_fully_populated_encoding() {
            Set<String> covered = new java.util.HashSet<>(SagaCodec.instanceFields());
            covered.addAll(Set.of(SagaCodec.STATE, SagaCodec.STREAM_WATERMARKS, SagaCodec.POSITION_WATERMARK, SagaCodec.STREAM_MARKS));

            assertThat(covered).containsAll(codec.encode(everythingSet()).encoded().fields().keySet());
        }

        @Test
        void the_fields_to_read_without_the_state_and_the_state_cover_every_field_of_a_fully_populated_encoding() {
            Set<String> covered = new java.util.HashSet<>(SagaCodec.fieldsWithoutState());
            covered.add(SagaCodec.STATE);

            assertThat(covered).containsAll(codec.encode(everythingSet()).encoded().fields().keySet());
        }

        @Test
        void the_field_lists_cover_every_field_of_a_not_started_flow_saga_encoding() {
            SagaEnvelope<FlowState<Tick>> flow = flowEnvelope(flowState());
            SagaEnvelope<FlowState<Tick>> notStarted = new SagaEnvelope<>(flow.sagaId(), flow.state(), SagaStatus.QUARANTINED, 3, List.of(new TimerEntry("t", 10L, 15L)),
                    Map.of("stream-1", 5L), 77L, CREATED, UPDATED, COMPLETED, null, false, new SagaFailure("i", 42L, FAILED_AT, "t", "m", RELEASED_AT, "q-1"),
                    Map.of("stream-1", new StreamMarks(1, Map.of())), 4L);
            SagaCodec<FlowState<Tick>> flowCodec = SagaCodec.of(SagaStateCodec.identity(), new TickConverter());
            Set<String> keys = flowCodec.encode(notStarted).encoded().fields().keySet();
            Set<String> fromInstanceFields = new java.util.HashSet<>(SagaCodec.instanceFields());
            fromInstanceFields.addAll(Set.of(SagaCodec.STATE, SagaCodec.STREAM_WATERMARKS, SagaCodec.POSITION_WATERMARK, SagaCodec.STREAM_MARKS));
            Set<String> fromFieldsWithoutState = new java.util.HashSet<>(SagaCodec.fieldsWithoutState());
            fromFieldsWithoutState.add(SagaCodec.STATE);

            assertThat(fromInstanceFields).containsAll(keys);
            assertThat(fromFieldsWithoutState).containsAll(keys);
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class Release034 {

        @Test
        void a_document_written_by_0_34_0_decodes_with_the_defaults_of_the_fields_it_does_not_have() {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put(SagaCodec.STATE, "enc:old");
            fields.put(SagaCodec.STATUS, "QUARANTINED");
            fields.put(SagaCodec.VERSION, 4L);
            fields.put(SagaCodec.TIMERS, List.of());
            fields.put(SagaCodec.STREAM_WATERMARKS, Map.of("stream-1", 3L));
            fields.put(SagaCodec.FAILURE_INPUT, "input-1");
            fields.put(SagaCodec.FAILURE_FIRST_FAILED_AT, FAILED_AT.toEpochMilli());
            fields.put(SagaCodec.FAILURE_TYPE, "java.lang.IllegalStateException");

            SagaEnvelope<String> decoded = codec.decode(stored("s1", fields));

            assertThat(decoded.state()).isEqualTo("old");
            assertThat(decoded.status()).isEqualTo(SagaStatus.QUARANTINED);
            assertThat(decoded.started()).isTrue();
            assertThat(decoded.streamMarks()).isEmpty();
            assertThat(decoded.quarantineSkipCount()).isNull();
            assertThat(decoded.failure()).isEqualTo(new SagaFailure("input-1", null, FAILED_AT, "java.lang.IllegalStateException", null, null, null));
            assertThat(decoded.streamWatermarks()).containsExactly(Map.entry("stream-1", 3L));
        }

        @Test
        void a_document_with_started_false_decodes_as_not_started() {
            Map<String, Object> fields = Map.of(SagaCodec.STATUS, "ACTIVE", SagaCodec.VERSION, 0L, SagaCodec.STARTED, false);

            assertThat(codec.decode(stored("s1", fields)).started()).isFalse();
        }

        @Test
        void a_document_with_no_timers_no_watermarks_and_no_dates_decodes_to_empty_collections_and_no_instants() {
            SagaEnvelope<String> decoded = codec.decode(stored("s1", Map.of(SagaCodec.STATUS, "ACTIVE", SagaCodec.VERSION, 0L)));

            assertThat(decoded.timers()).isEmpty();
            assertThat(decoded.streamWatermarks()).isEmpty();
            assertThat(decoded.createdAt()).isNull();
            assertThat(decoded.updatedAt()).isNull();
            assertThat(decoded.completedAt()).isNull();
            assertThat(decoded.positionWatermark()).isNull();
        }

        @Test
        void a_stream_stored_before_pending_and_legacy_failures_existed_decodes_with_nothing_outstanding() {
            Map<String, Object> type = Map.of("type", "t", "mark", 4L, "applied", List.of(2L));
            Map<String, Object> stream = Map.of("streamId", "stream-1", "floor", 1L, "types", List.of(type));
            Map<String, Object> fields = Map.of(SagaCodec.STATUS, "ACTIVE_MARKED", SagaCodec.VERSION, 0L, SagaCodec.STREAM_MARKS, List.of(stream));

            StreamMarks marks = codec.decode(stored("s1", fields)).streamMarks().get("stream-1");

            assertThat(marks).isEqualTo(new StreamMarks(1, Map.of("t", new TypeMark(4, List.of(2L), Long.MIN_VALUE)), Map.of(), List.of(), Map.of()));
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class FlowStates {

        private final SagaCodec<FlowState<Tick>> flowCodec = SagaCodec.of(SagaStateCodec.identity(), new TickConverter());

        @Test
        void a_flow_state_is_written_field_by_field_with_its_received_events_as_cloud_events() {
            StoredSaga stored = flowCodec.encode(flowEnvelope(flowState()));

            Map<?, ?> state = (Map<?, ?>) stored.encoded().fields().get(SagaCodec.STATE);
            assertThat((List<?>) state.get(SagaCodec.FLOW_RECEIVED)).allSatisfy(event -> assertThat(event).isInstanceOf(CloudEvent.class));
            assertThat(((List<?>) state.get(SagaCodec.FLOW_RECEIVED)).stream().map(event -> ((CloudEvent) event).getId())).containsExactly("e1", "e2");
        }

        @Test
        void the_encoded_flow_state_cannot_be_changed_at_any_depth() {
            StoredSaga stored = flowCodec.encode(flowEnvelope(flowState()));

            Map<?, ?> state = (Map<?, ?>) stored.encoded().fields().get(SagaCodec.STATE);
            @SuppressWarnings("unchecked") Map<String, Object> mutableView = (Map<String, Object>) state;
            @SuppressWarnings("unchecked") List<Object> received = (List<Object>) state.get(SagaCodec.FLOW_RECEIVED);
            assertThatThrownBy(() -> mutableView.put("other", 1)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> received.clear()).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void a_flow_state_round_trips_with_every_field_set() {
            SagaEnvelope<FlowState<Tick>> envelope = flowEnvelope(flowState());

            assertThat(flowCodec.decode(flowCodec.encode(envelope))).isEqualTo(envelope);
        }

        @Test
        void a_flow_state_with_its_optional_fields_unset_round_trips() {
            FlowStateImpl<Tick> state = new FlowStateImpl<>(null, List.of(), 0, 0, false, null, -1, ActionKind.NONE, -1, null);
            SagaEnvelope<FlowState<Tick>> envelope = flowEnvelope(state);

            assertThat(flowCodec.decode(flowCodec.encode(envelope))).isEqualTo(envelope);
        }

        @Test
        void a_stored_flow_state_without_the_fields_added_later_decodes_with_their_defaults() {
            Map<String, Object> state = new LinkedHashMap<>();
            state.put(SagaCodec.FLOW_RECEIVED, List.of());
            state.put("currentStep", "waiting");
            state.put("lastAction", "NONE");
            Map<String, Object> fields = Map.of(SagaCodec.STATUS, "ACTIVE", SagaCodec.VERSION, 0L, SagaCodec.STATE, state);

            FlowStateImpl<?> decoded = (FlowStateImpl<?>) flowCodec.decode(stored("s1", fields)).state();

            assertThat(decoded.windowStart()).isEqualTo(1);
            assertThat(decoded.stepEntryIndex()).isZero();
            assertThat(decoded.completed()).isFalse();
            assertThat(decoded.previousStepEntryIndex()).isEqualTo(-1);
            assertThat(decoded.matchedBranchIndex()).isEqualTo(-1);
            assertThat(decoded.stepConditionProgress()).isNull();
        }

        @Test
        void a_flow_state_that_is_not_the_flow_executors_is_refused() {
            FlowState<Tick> foreign = new FlowState<>() {
                @Override
                public String currentStep() {
                    return "waiting";
                }

                @Override
                public List<Tick> received() {
                    return List.of();
                }

                @Override
                public boolean completed() {
                    return false;
                }
            };

            assertThatThrownBy(() -> flowCodec.encode(flowEnvelope(foreign)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("FlowStateImpl");
        }

        @Test
        void the_current_step_of_the_envelope_is_the_step_of_the_flow_state() {
            StoredSaga stored = flowCodec.encode(flowEnvelope(flowState()));

            assertThat(stored.encoded().fields()).containsEntry(SagaCodec.CURRENT_STEP, "waiting");
        }

        @Test
        void a_codec_without_a_flow_converter_refuses_a_flow_state() {
            SagaCodec<FlowState<Tick>> plain = SagaCodec.of(SagaStateCodec.identity(), null);

            assertThatThrownBy(() -> plain.encode(flowEnvelope(flowState())))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("s1")
                    .hasMessageContaining("CloudEventConverter");
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class EncodedSagaCopies {

        @Test
        void a_list_or_map_the_caller_changes_after_creating_it_leaves_the_encoded_saga_as_it_was() {
            List<Object> timers = new ArrayList<>(List.of(new LinkedHashMap<>(Map.of("name", "a", "firesAtEpochMilli", 10L))));
            Map<String, Object> watermarks = new LinkedHashMap<>(Map.of("stream-1", 3L));
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put(SagaCodec.TIMERS, timers);
            fields.put(SagaCodec.STREAM_WATERMARKS, watermarks);
            EncodedSaga encoded = new EncodedSaga(fields);

            ((Map<String, Object>) timers.getFirst()).put("name", "changed");
            timers.clear();
            watermarks.clear();
            fields.clear();

            assertThat(encoded.fields()).containsOnlyKeys(SagaCodec.TIMERS, SagaCodec.STREAM_WATERMARKS);
            assertThat(encoded.fields().get(SagaCodec.TIMERS)).isEqualTo(List.of(Map.of("name", "a", "firesAtEpochMilli", 10L)));
            assertThat(encoded.fields().get(SagaCodec.STREAM_WATERMARKS)).isEqualTo(Map.of("stream-1", 3L));
        }

        @Test
        void the_lists_and_maps_of_the_encoded_saga_cannot_be_changed_at_any_depth() {
            Map<String, Object> inner = new LinkedHashMap<>(Map.of("type", "t"));
            EncodedSaga encoded = new EncodedSaga(Map.of(SagaCodec.STREAM_MARKS, new ArrayList<>(List.of(new LinkedHashMap<>(Map.of("types", new ArrayList<>(List.of(inner))))))));

            List<?> marks = (List<?>) encoded.fields().get(SagaCodec.STREAM_MARKS);
            Map<?, ?> stream = (Map<?, ?>) marks.getFirst();
            List<?> types = (List<?>) stream.get("types");
            Map<?, ?> type = (Map<?, ?>) types.getFirst();

            assertThatThrownBy(() -> ((List<Object>) marks).add("x")).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> ((Map<Object, Object>) stream).put("x", 1)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> ((List<Object>) types).clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> ((Map<Object, Object>) type).remove("type")).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void null_entries_in_a_list_or_map_are_kept() {
            List<Object> list = new ArrayList<>();
            list.add(null);
            list.add("x");
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("absent", null);

            EncodedSaga encoded = new EncodedSaga(Map.of("list", list, "map", map));

            @SuppressWarnings("unchecked") List<Object> keptList = (List<Object>) encoded.fields().get("list");
            @SuppressWarnings("unchecked") Map<Object, Object> keptMap = (Map<Object, Object>) encoded.fields().get("map");
            assertThat(keptList).containsExactly(null, "x");
            assertThat(keptMap).containsOnlyKeys("absent").containsEntry("absent", null);
        }

        @Test
        void the_state_field_is_kept_as_the_state_codec_returned_it() {
            List<String> state = new ArrayList<>(List.of("a"));

            EncodedSaga encoded = new EncodedSaga(Map.of(SagaCodec.STATE, state));

            assertThat(encoded.fields().get(SagaCodec.STATE)).isSameAs(state);
        }
    }

    @Nested
    @DisplayNameGeneration(ReplaceUnderscores.class)
    class Nulls {

        @Test
        void the_codec_refuses_a_null_state_codec() {
            assertThatNullPointerException().isThrownBy(() -> SagaCodec.of(null, null)).withMessageContaining("stateCodec");
        }

        @Test
        void encode_and_encodeWithoutState_refuse_a_null_envelope() {
            assertThatNullPointerException().isThrownBy(() -> codec.encode(null)).withMessage("envelope cannot be null");
            assertThatNullPointerException().isThrownBy(() -> codec.encodeWithoutState(null)).withMessage("envelope cannot be null");
        }

        @Test
        void decode_and_decodeWithoutState_refuse_a_null_stored_saga() {
            assertThatNullPointerException().isThrownBy(() -> codec.decode(null)).withMessageContaining("stored");
            assertThatNullPointerException().isThrownBy(() -> codec.decodeWithoutState(null)).withMessageContaining("stored");
        }

        @Test
        void forget_storedStatuses_and_indexOf_refuse_null() {
            assertThatNullPointerException().isThrownBy(() -> codec.forget(null)).withMessageContaining("sagaId");
            assertThatNullPointerException().isThrownBy(() -> SagaCodec.storedStatuses(null)).withMessageContaining("status");
            assertThatNullPointerException().isThrownBy(() -> SagaCodec.indexOf(null)).withMessageContaining("encoded");
        }

        @Test
        void a_state_codec_that_encodes_a_state_as_null_is_refused_naming_the_saga_and_the_codec() {
            SagaCodec<String> nullEncoding = SagaCodec.of(new NullEncodingStateCodec(), null);

            assertThatNullPointerException().isThrownBy(() -> nullEncoding.encode(bare("order-7", "state")))
                    .withMessageContaining("order-7")
                    .withMessageContaining(NullEncodingStateCodec.class.getName());
        }

        @Test
        void a_state_codec_that_encodes_a_state_as_null_is_not_asked_by_the_encoding_without_state() {
            SagaCodec<String> nullEncoding = SagaCodec.of(new NullEncodingStateCodec(), null);

            StoredSaga stored = nullEncoding.encodeWithoutState(bare("order-7", "state"));

            assertThat(stored.encoded().fields()).doesNotContainKey(SagaCodec.STATE);
        }

        @Test
        void a_stored_saga_refuses_a_null_part() {
            SagaIndex index = new SagaIndex("ACTIVE", null, null, false, null);
            EncodedSaga encoded = new EncodedSaga(Map.of());

            assertThatNullPointerException().isThrownBy(() -> new StoredSaga(null, 0, encoded, index));
            assertThatNullPointerException().isThrownBy(() -> new StoredSaga("s1", 0, null, index));
            assertThatNullPointerException().isThrownBy(() -> new StoredSaga("s1", 0, encoded, null));
        }

        @Test
        void a_stored_saga_that_the_state_codec_would_see_as_a_set_is_not_confused_with_a_flow_state() {
            SagaCodec<Set<String>> sets = SagaCodec.of(SagaStateCodec.identity(), null);
            SagaEnvelope<Set<String>> envelope = new SagaEnvelope<>("s1", Set.of("a"), SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null, true, null, Map.of(), null);

            assertThat(sets.decode(sets.encode(envelope)).state()).containsExactly("a");
        }
    }

    private static StoredSaga stored(String sagaId, Map<String, Object> fields) {
        EncodedSaga encoded = new EncodedSaga(fields);
        long version = fields.get(SagaCodec.VERSION) instanceof Number n ? n.longValue() : 0L;
        return new StoredSaga(sagaId, version, encoded, SagaCodec.indexOf(encoded));
    }

    private static StoredSaga withoutField(StoredSaga stored, String sagaId, String field) {
        Map<String, Object> fields = new LinkedHashMap<>(stored.encoded().fields());
        assertThat(fields.remove(field)).as("the field %s that the test removes", field).isNotNull();
        return stored(sagaId, fields);
    }

    private static SagaEnvelope<String> bare(String sagaId, String state) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> everythingSet() {
        Map<String, StreamMarks> marks = Map.of("stream-1", new StreamMarks(2, Map.of("t", new TypeMark(5, List.of(3L), 1)), Map.of("t", List.of(6L)), List.of(7L),
                Map.of(6L, Instant.ofEpochMilli(1_700_000_005_000L))));
        SagaFailure failure = new SagaFailure("input-1", 42L, FAILED_AT, "java.lang.IllegalStateException", "boom", RELEASED_AT, "q-1");
        return new SagaEnvelope<>("s1", "state", SagaStatus.QUARANTINED, 3, List.of(new TimerEntry("t", 10L, 15L)), Map.of("stream-1", 5L), 77L,
                CREATED, UPDATED, COMPLETED, "waiting", false, failure, marks, 4L);
    }

    private static SagaEnvelope<String> quarantined(String quarantineId) {
        SagaFailure failure = new SagaFailure("i", null, FAILED_AT, "t", null, null, quarantineId);
        return new SagaEnvelope<>("s1", "state", SagaStatus.QUARANTINED, 0, List.of(), Map.of(), null, null, null, null, null, true, failure, Map.of(), null);
    }

    private static SagaEnvelope<String> withTimers(SagaEnvelope<String> e, TimerEntry... timers) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), List.of(timers), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaEnvelope<String> withMarks(SagaEnvelope<String> e, Map<String, StreamMarks> marks) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), e.timers(), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), marks, e.quarantineSkipCount());
    }

    private static SagaEnvelope<String> withFailure(SagaEnvelope<String> e, SagaFailure failure) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), e.timers(), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), failure, e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaEnvelope<String> withSkipCount(SagaEnvelope<String> e, Long count) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), e.timers(), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), count);
    }

    private static SagaEnvelope<String> withUpdatedAt(SagaEnvelope<String> e, Instant updatedAt) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), e.timers(), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                updatedAt, e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaEnvelope<FlowState<Tick>> flowEnvelope(FlowState<Tick> state) {
        return new SagaEnvelope<>("s1", state, SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null, true, null, Map.of(), null);
    }

    private static FlowStateImpl<Tick> flowState() {
        return new FlowStateImpl<>("waiting", List.of(new Tick("e1"), new Tick("e2")), 1, 2, true, "previous", 1, ActionKind.BRANCH, 3,
                new StepConditionProgress("fingerprint", List.of(1, 2)));
    }

    private record Tick(String eventId) {
    }

    private static final class TickConverter implements CloudEventConverter<Tick> {
        @Override
        public CloudEvent toCloudEvent(Tick event) {
            return CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("urn:test")).withType("tick").build();
        }

        @Override
        public Tick toDomainEvent(CloudEvent cloudEvent) {
            return new Tick(cloudEvent.getId());
        }

        @Override
        public String getCloudEventType(Class<? extends Tick> type) {
            return "tick";
        }
    }

    private static final class NullEncodingStateCodec implements SagaStateCodec<String> {
        @Override
        @SuppressWarnings("NullAway")
        public Object encode(String state) {
            return null;
        }

        @Override
        public String decode(Object encoded) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class PrefixingStateCodec implements SagaStateCodec<String> {
        @Override
        public Object encode(String state) {
            return "enc:" + state;
        }

        @Override
        public String decode(Object encoded) {
            return ((String) encoded).substring("enc:".length());
        }
    }
}
