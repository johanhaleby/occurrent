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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaIndex;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The in-memory store through the {@link SagaStateStore} SPI, with stored sagas built by hand so no envelope-level
 * helper stands between the test and what the store is handed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreSpiTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaCodec<String> codec = SagaCodec.of(store.stateCodec(), null);

    @Test
    void a_save_without_the_state_leaves_the_stored_state_and_current_step_alone() {
        store.compareAndSave(withCurrentStep(codec.encode(active("s1", "original", 1)), "waiting"), 0);

        boolean saved = store.compareAndSaveWithoutState(codec.encodeWithoutState(quarantined("s1", "ignored", 2)), 1);

        Map<String, Object> fields = store.find("s1").orElseThrow().encoded().fields();
        assertThat(saved).isTrue();
        assertThat(fields).containsEntry(SagaCodec.STATE, "original").containsEntry(SagaCodec.CURRENT_STEP, "waiting");
        assertThat(fields).containsEntry(SagaCodec.STATUS, "QUARANTINED").containsEntry(SagaCodec.VERSION, 2L);
    }

    @Test
    void a_save_without_the_state_takes_every_other_field_from_what_it_is_handed() {
        store.compareAndSave(codec.encode(withTimer(active("s1", "state", 1), new TimerEntry("old", 10L, null))), 0);

        store.compareAndSaveWithoutState(codec.encodeWithoutState(withTimer(active("s1", "state", 2), new TimerEntry("new", 20L, null))), 1);

        SagaEnvelope<String> found = codec.decode(store.find("s1").orElseThrow());
        assertThat(found.timers()).containsExactly(new TimerEntry("new", 20L, null));
        assertThat(found.version()).isEqualTo(2L);
    }

    @Test
    void a_save_without_the_state_does_not_invent_a_state_the_instance_never_had() {
        EncodedSaga stateless = new EncodedSaga(new LinkedHashMap<>(codec.encodeWithoutState(active("s1", "state", 1)).encoded().fields()));
        store.compareAndSave(new StoredSaga("s1", 1, stateless, SagaCodec.indexOf(stateless)), 0);

        store.compareAndSaveWithoutState(codec.encodeWithoutState(active("s1", "state", 2)), 1);

        assertThat(store.find("s1").orElseThrow().encoded().fields()).doesNotContainKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP);
    }

    @Test
    void a_save_without_the_state_at_version_0_is_refused_naming_the_instance_and_stores_nothing() {
        StoredSaga insert = codec.encodeWithoutState(active("s1", "state", 0));

        assertThatThrownBy(() -> store.compareAndSaveWithoutState(insert, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s1")
                .hasMessageContaining("compareAndSave");
        assertThat(store.find("s1")).isEmpty();
    }

    @Test
    void a_save_without_the_state_at_a_stale_version_is_lost_and_changes_nothing() {
        store.compareAndSave(codec.encode(active("s1", "original", 1)), 0);

        boolean saved = store.compareAndSaveWithoutState(codec.encodeWithoutState(quarantined("s1", "ignored", 3)), 2);

        assertThat(saved).isFalse();
        assertThat(codec.decode(store.find("s1").orElseThrow())).isEqualTo(active("s1", "original", 1));
    }

    @Test
    void a_save_without_the_state_of_an_instance_that_is_not_stored_is_lost() {
        assertThat(store.compareAndSaveWithoutState(codec.encodeWithoutState(active("s1", "state", 2)), 1)).isFalse();
        assertThat(store.find("s1")).isEmpty();
    }

    @Test
    void the_index_is_derived_from_the_stored_fields_and_not_taken_from_the_caller() {
        StoredSaga encoded = codec.encode(withTimer(active("s1", "state", 1), new TimerEntry("t", 10L, null)));
        SagaIndex wrong = new SagaIndex("COMPLETED", null, null, true, "bogus");

        store.compareAndSave(new StoredSaga("s1", 1, encoded.encoded(), wrong), 0);

        assertThat(store.find("s1").orElseThrow().index()).isEqualTo(SagaCodec.indexOf(encoded.encoded()));
    }

    @Test
    void a_wrong_index_from_the_caller_has_no_effect_on_the_timer_status_and_release_queries() {
        StoredSaga encoded = codec.encode(withTimer(active("s1", "state", 1), new TimerEntry("t", 10L, null)));
        SagaIndex wrong = new SagaIndex("COMPLETED", null, Instant.parse("2100-01-01T00:00:00Z").toEpochMilli(), true, "bogus");

        store.compareAndSave(new StoredSaga("s1", 1, encoded.encoded(), wrong), 0);

        assertThat(store.findWithDueTimers(Instant.ofEpochMilli(10L), 10)).extracting(StoredSaga::sagaId).containsExactly("s1");
        assertThat(store.findByStatus(SagaStatus.COMPLETED, Instant.parse("2100-01-01T00:00:00Z"), 10)).isEmpty();
        assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10)).extracting(StoredSaga::sagaId).containsExactly("s1");
        assertThat(store.findReleasingSagaIds(null, 10)).isEmpty();
    }

    @Test
    void a_wrong_quarantine_id_from_the_caller_does_not_decide_which_skipped_inputs_a_delete_removes() {
        StoredSaga encoded = codec.encode(active("s1", "state", 1));
        store.compareAndSave(new StoredSaga("s1", 1, encoded.encoded(), new SagaIndex("ACTIVE", null, null, false, "bogus")), 0);
        store.compareAndSaveSkipped(codec.encodeWithoutState(active("s1", "state", 2)), 1, "bogus", new SkippedInput(1L, "k"));

        store.delete("s1");

        assertThat(store.findSkippedInputs("s1", "bogus", 10)).hasSize(1);
    }

    @Test
    void instants_are_kept_at_millisecond_precision_whatever_the_envelope_holds() {
        Instant withNanos = Instant.ofEpochSecond(1_700_000_000L, 123_456_789L);
        SagaEnvelope<String> envelope = new SagaEnvelope<>("s1", "state", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, withNanos, withNanos, null, null, true, null, Map.of(), null);

        store.compareAndSave(codec.encode(envelope), 0);

        Instant millis = Instant.ofEpochSecond(1_700_000_000L, 123_000_000L);
        assertThat(codec.decode(store.find("s1").orElseThrow()).createdAt()).isEqualTo(millis);
        assertThat(codec.decode(store.find("s1").orElseThrow()).updatedAt()).isEqualTo(millis);
    }

    @Test
    void the_state_codec_hands_the_state_object_through_unchanged() {
        Object state = new Object();
        InMemorySagaStateStore<Object> objects = new InMemorySagaStateStore<>();
        SagaEnvelope<Object> envelope = new SagaEnvelope<>("s1", state, SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null, true, null, Map.of(), null);

        StoredSaga stored = SagaCodec.of(objects.stateCodec(), null).encode(envelope);

        assertThat(stored.encoded().fields().get(SagaCodec.STATE)).isSameAs(state);
    }

    @Test
    void changing_the_nested_lists_and_maps_handed_to_a_save_afterwards_leaves_the_stored_saga_and_its_index_unchanged() {
        Map<String, Object> fields = mutableCopyOf(codec.encode(withNestedData("s1")).encoded().fields());
        EncodedSaga handed = new EncodedSaga(fields);
        store.compareAndSave(new StoredSaga("s1", 1, handed, SagaCodec.indexOf(handed)), 0);
        StoredSaga before = store.find("s1").orElseThrow();

        ((List<?>) fields.get(SagaCodec.TIMERS)).clear();
        ((Map<?, ?>) fields.get(SagaCodec.STREAM_WATERMARKS)).clear();
        ((Map<?, ?>) ((List<?>) fields.get(SagaCodec.STREAM_MARKS)).getFirst()).clear();
        fields.clear();

        StoredSaga after = store.find("s1").orElseThrow();
        assertThat(after).isEqualTo(before);
        assertThat(codec.decode(after)).isEqualTo(withNestedData("s1"));
        assertThat(store.findWithDueTimers(Instant.ofEpochMilli(10L), 10)).extracting(StoredSaga::sagaId).containsExactly("s1");
    }

    @Test
    void changing_the_nested_lists_and_maps_of_what_a_find_returned_leaves_the_stored_saga_unchanged() {
        store.compareAndSave(codec.encode(withNestedData("s1")), 0);

        Map<String, Object> read = store.find("s1").orElseThrow().encoded().fields();
        List<?> timers = (List<?>) read.get(SagaCodec.TIMERS);
        Map<?, ?> watermarks = (Map<?, ?>) read.get(SagaCodec.STREAM_WATERMARKS);
        List<?> marks = (List<?>) read.get(SagaCodec.STREAM_MARKS);
        Map<?, ?> firstStream = (Map<?, ?>) marks.getFirst();
        List<?> types = (List<?>) firstStream.get("types");

        assertThatThrownBy(() -> ((List<Object>) timers).clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((Map<Object, Object>) ((Map<?, ?>) timers.getFirst())).put("name", "x")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((Map<Object, Object>) watermarks).clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((List<Object>) marks).clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((Map<Object, Object>) firstStream).clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((List<Object>) types).clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(codec.decode(store.find("s1").orElseThrow())).isEqualTo(withNestedData("s1"));
    }

    @Test
    void a_state_codec_that_encodes_a_state_as_null_fails_the_save_naming_the_instance_and_stores_nothing() {
        SagaCodec<String> nullEncoding = SagaCodec.of(new NullEncodingStateCodec(), null);

        assertThatNullPointerException().isThrownBy(() -> store.compareAndSave(nullEncoding.encode(active("s1", "state", 1)), 0))
                .withMessageContaining("s1");
        assertThat(store.find("s1")).isEmpty();
    }

    @Test
    void a_state_codec_that_encodes_a_state_as_null_fails_an_update_and_leaves_the_previous_version() {
        store.compareAndSave(codec.encode(active("s1", "original", 1)), 0);
        SagaCodec<String> nullEncoding = SagaCodec.of(new NullEncodingStateCodec(), null);

        assertThatNullPointerException().isThrownBy(() -> store.compareAndSave(nullEncoding.encode(active("s1", "next", 2)), 1))
                .withMessageContaining("s1");
        assertThat(codec.decode(store.find("s1").orElseThrow())).isEqualTo(active("s1", "original", 1));
    }

    @Test
    void reads_and_writes_refuse_null() {
        assertThatNullPointerException().isThrownBy(() -> store.find(null));
        assertThatNullPointerException().isThrownBy(() -> store.findWithoutState(null));
        assertThatNullPointerException().isThrownBy(() -> store.compareAndSave(null, 0));
        assertThatNullPointerException().isThrownBy(() -> store.compareAndSaveWithoutState(null, 1));
        assertThatNullPointerException().isThrownBy(() -> store.findWithDueTimers(null, 1));
        assertThatNullPointerException().isThrownBy(() -> store.findByStatus(null, NOW, 1));
        assertThatNullPointerException().isThrownBy(() -> store.findByStatus(SagaStatus.ACTIVE, null, 1));
        assertThatNullPointerException().isThrownBy(() -> store.delete(null));
    }

    // Every list and map copied into a mutable one, so a test can change what it handed a save
    private static Map<String, Object> mutableCopyOf(Map<String, Object> fields) {
        Map<String, Object> copy = new LinkedHashMap<>();
        fields.forEach((name, value) -> copy.put(name, mutable(value)));
        return copy;
    }

    private static Object mutable(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            map.forEach((key, entry) -> copy.put(key, mutable(entry)));
            return copy;
        }
        if (value instanceof List<?> list) {
            return new ArrayList<>(list.stream().map(InMemorySagaStateStoreSpiTest::mutable).toList());
        }
        return value;
    }

    private static SagaEnvelope<String> withNestedData(String sagaId) {
        Map<String, SagaEnvelope.StreamMarks> marks = Map.of("stream-1", new SagaEnvelope.StreamMarks(2,
                Map.of("t", new SagaEnvelope.TypeMark(5, List.of(3L), 1)), Map.of("t", List.of(6L)), List.of(7L), Map.of(6L, NOW)));
        return new SagaEnvelope<>(sagaId, "state", SagaStatus.ACTIVE, 1, List.of(new TimerEntry("t", 10L, null)), Map.of("stream-1", 3L), null, NOW, NOW, null, null, true, null,
                marks, null);
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

    private static StoredSaga withCurrentStep(StoredSaga stored, String step) {
        Map<String, Object> fields = new LinkedHashMap<>(stored.encoded().fields());
        fields.put(SagaCodec.CURRENT_STEP, step);
        EncodedSaga encoded = new EncodedSaga(fields);
        return new StoredSaga(stored.sagaId(), stored.version(), encoded, SagaCodec.indexOf(encoded));
    }

    private static SagaEnvelope<String> active(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> quarantined(String sagaId, String state, long version) {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", null, null);
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), null);
    }

    private static SagaEnvelope<String> withTimer(SagaEnvelope<String> e, TimerEntry timer) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version(), List.of(timer), e.streamWatermarks(), e.positionWatermark(), e.createdAt(),
                e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }
}
