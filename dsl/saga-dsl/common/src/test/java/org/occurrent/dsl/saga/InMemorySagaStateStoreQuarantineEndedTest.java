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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey.RecordKey;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The in-memory store ends a quarantine without writing the stored state, so a release ends on a state that no longer
 * decodes, and reads the seq and key of the first record of an instance without its event.
 */
@DisplayName("InMemorySagaStateStore ending a quarantine and reading the key of the first record")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreQuarantineEndedTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String QUARANTINE_ID = "q1";

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaCodec<String> codec = SagaCodec.of(store.stateCodec(), null);

    @Test
    void ending_a_quarantine_leaves_the_stored_state_alone_and_writes_every_other_field() {
        store.compareAndSave(codec.encode(releasing("s1", "original", 1)), 0);
        StoredSaga handed = codec.encodeWithoutState(active("s1", "ignored", 2));

        boolean ended = store.compareAndSaveQuarantineEnded(handed, 1, QUARANTINE_ID);

        Map<String, Object> fields = store.find("s1").orElseThrow().encoded().fields();
        SagaEnvelope<String> found = codec.decode(store.find("s1").orElseThrow());
        assertAll(
                () -> assertThat(ended).isTrue(),
                () -> assertThat(fields).as("the stored state").containsEntry(SagaCodec.STATE, "original"),
                () -> assertThat(fields).as("every field it was handed").containsAllEntriesOf(handed.encoded().fields()),
                () -> assertThat(found.state()).isEqualTo("original"),
                () -> assertThat(found.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(found.version()).isEqualTo(2L),
                () -> assertThat(found.failure()).as("the failure the quarantine left").isNull(),
                () -> assertThat(found.quarantineId()).isNull(),
                () -> assertThat(found.quarantineSkipCount()).isNull(),
                () -> assertThat(store.find("s1").orElseThrow().index().quarantineId()).as("the index follows the stored fields").isNull()
        );
    }

    @Test
    void ending_a_quarantine_leaves_the_stored_state_alone_even_when_handed_another() {
        store.compareAndSave(codec.encode(releasing("s1", "original", 1)), 0);

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encode(active("s1", "handed another state", 2)), 1, QUARANTINE_ID);

        assertAll(
                () -> assertThat(ended).isTrue(),
                () -> assertThat(store.find("s1").orElseThrow().encoded().fields()).containsEntry(SagaCodec.STATE, "original"),
                () -> assertThat(codec.decode(store.find("s1").orElseThrow()).status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void ending_a_quarantine_does_not_invent_a_state_the_instance_never_had() {
        SagaEnvelope<String> withoutAState = releasing("s1", "state", 1);
        store.compareAndSave(codec.encodeWithoutState(withoutAState), 0);

        store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("s1", "ignored", 2)), 1, QUARANTINE_ID);

        assertThat(store.find("s1").orElseThrow().encoded().fields()).doesNotContainKey(SagaCodec.STATE);
    }

    @Test
    void ending_a_quarantine_is_lost_while_a_record_is_left_and_changes_nothing() {
        store.compareAndSave(codec.encode(releasing("s1", "original", 1)), 0);
        store.compareAndSaveSkipped(codec.encodeWithoutState(releasing("s1", "original", 2)), 1, QUARANTINE_ID, new SkippedInput(1L, "k", heldEvent("k")));

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("s1", "ignored", 3)), 2, QUARANTINE_ID);

        assertAll(
                () -> assertThat(ended).isFalse(),
                () -> assertThat(store.findSkippedInputs("s1", QUARANTINE_ID, 10)).hasSize(1),
                () -> assertThat(codec.decode(store.find("s1").orElseThrow()).status()).isEqualTo(SagaStatus.QUARANTINED)
        );
    }

    @Test
    void the_key_read_returns_the_seq_and_key_of_the_first_record_by_seq_and_then_key() {
        store.compareAndSave(codec.encode(releasing("s1", "state", 1)), 0);
        skip("s1", 2, 5, "c@3");
        skip("s1", 3, 3, "b@2");
        skip("s1", 4, 3, "a@1");

        Optional<StoredSagaWithFirstSkippedInputKey> read = store.findWithFirstSkippedInputKey("s1");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().first()).isEqualTo(new RecordKey(3, "a@1")),
                () -> assertThat(read.orElseThrow().saga().version()).as("the instance is at the version the last write left").isEqualTo(4L)
        );
    }

    @Test
    void the_key_read_agrees_with_the_read_of_the_first_record() {
        store.compareAndSave(codec.encode(releasing("s1", "state", 1)), 0);
        skip("s1", 2, 7, "x@9");
        skip("s1", 3, 6, "y@8");

        @Nullable RecordKey key = store.findWithFirstSkippedInputKey("s1").orElseThrow().first();
        SkippedInput whole = store.findWithFirstSkippedInput("s1").orElseThrow().firstSkippedInput();

        assertThat(key).isEqualTo(RecordKey.of(whole));
    }

    @Test
    void the_key_read_returns_no_record_for_an_instance_without_a_quarantine_id() {
        store.compareAndSave(codec.encode(active("s1", "state", 1)), 0);
        assertThat(store.compareAndSaveSkipped(codec.encodeWithoutState(active("s1", "state", 2)), 1, QUARANTINE_ID, new SkippedInput(1L, "left@1", heldEvent("left@1")))).isTrue();
        assertThat(store.findSkippedInputs("s1", QUARANTINE_ID, 10)).as("control: a record is held under the id").hasSize(1);

        Optional<StoredSagaWithFirstSkippedInputKey> read = store.findWithFirstSkippedInputKey("s1");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().first()).isNull()
        );
    }

    @Test
    void the_key_read_returns_no_record_when_none_is_left_under_the_quarantine_id() {
        store.compareAndSave(codec.encode(releasing("s1", "state", 1)), 0);

        assertThat(store.findWithFirstSkippedInputKey("s1").orElseThrow().first()).isNull();
    }

    @Test
    void the_key_read_returns_empty_for_an_instance_the_store_does_not_hold() {
        assertThat(store.findWithFirstSkippedInputKey("nobody")).isEmpty();
    }

    @Test
    void the_key_read_refuses_a_null_saga_id() {
        assertThatThrownBy(() -> store.findWithFirstSkippedInputKey(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("sagaId");
    }

    private void skip(String sagaId, long version, long seq, String key) {
        assertThat(store.compareAndSaveSkipped(codec.encodeWithoutState(releasing(sagaId, "state", version)), version - 1, QUARANTINE_ID, new SkippedInput(seq, key, heldEvent(key)))).isTrue();
    }

    private static SagaEnvelope<String> active(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> releasing(String sagaId, String state, long version) {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static CloudEvent heldEvent(String key) {
        return CloudEventBuilder.v1().withId(key).withSource(URI.create("urn:test")).withType("test").build();
    }
}
