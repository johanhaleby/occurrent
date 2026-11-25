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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey.RecordKey;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set. {@code findWithFirstSkippedInputKey} returns the instance without its state and the
 * {@code seq} and {@code key} of its first record, without decoding the event of the record or reading its chunks, so a
 * record whose event no longer decodes, or whose chunks are gone, is still read by its key. And
 * {@code compareAndSaveQuarantineEnded} writes every field of the instance but its state.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore ending a quarantine and reading the key of the first record on a replica set")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreQuarantineEndedTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-quarantine-ended";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<String> store;
    private SagaEnvelopes<String> envelopes;
    private SagaCodec<String> codec;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-quarantine-ended-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
        envelopes = SagaEnvelopes.of(store, null);
        codec = SagaCodec.of(store.stateCodec(), null);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<String> releasing(String sagaId, String state, long version) {
        SagaFailure failure = new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, List.of(), Map.of("order-1", 6L), null, NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static SagaEnvelope<String> active(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of("order-1", 6L), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
    }

    private static CloudEvent heldEvent(String key) {
        return CloudEventBuilder.v1().withId(key).withSource(URI.create("urn:test")).withType("test").build();
    }

    private void storedReleasing(String sagaId, String state) {
        assertThat(envelopes.compareAndSave(sagaId, releasing(sagaId, state, 1), 0)).isTrue();
    }

    private void skipped(String sagaId, long seq, String key) {
        SagaEnvelope<String> current = envelopes.find(sagaId).orElseThrow();
        assertThat(envelopes.compareAndSaveSkipped(store, releasing(sagaId, current.state(), current.version() + 1), current.version(), QUARANTINE_ID, new SkippedInput(seq, key, heldEvent(key)))).isTrue();
    }

    private Document rawInstance(String sagaId) {
        return requireNonNull(template.getCollection(COLLECTION).find(new Document("_id", sagaId)).first());
    }

    private void corruptTheEventOf(String sagaId, String key) {
        assertThat(template.getCollection(SKIPPED_COLLECTION).updateOne(new Document("sagaId", sagaId).append("key", key), Updates.set("event", "{ this is not a CloudEvent")).getModifiedCount()).isEqualTo(1L);
    }

    @Test
    void the_key_read_returns_the_seq_and_key_of_the_first_record_by_seq_and_then_key() {
        storedReleasing("order-1", "state");
        skipped("order-1", 5, "c@3");
        skipped("order-1", 3, "b@2");
        skipped("order-1", 3, "a@1");

        Optional<StoredSagaWithFirstSkippedInputKey> read = store.findWithFirstSkippedInputKey("order-1");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().first()).isEqualTo(new RecordKey(3, "a@1")),
                () -> assertThat(read.orElseThrow().saga().version()).as("the instance is the one the record was read with").isEqualTo(4L)
        );
    }

    @Test
    void the_key_read_returns_the_instance_without_its_state() {
        storedReleasing("order-1", "state");
        skipped("order-1", 1, "a@1");

        StoredSagaWithFirstSkippedInputKey read = store.findWithFirstSkippedInputKey("order-1").orElseThrow();

        assertAll(
                () -> assertThat(read.saga().encoded().fields()).doesNotContainKey(SagaCodec.STATE),
                () -> assertThat(codec.decodeWithoutState(read.saga()).quarantineId()).isEqualTo(QUARANTINE_ID)
        );
    }

    @Test
    void the_key_read_returns_the_first_record_when_its_event_no_longer_decodes_where_the_read_of_the_record_throws() {
        storedReleasing("order-1", "state");
        skipped("order-1", 1, "a@1");
        skipped("order-1", 2, "b@2");
        corruptTheEventOf("order-1", "a@1");
        AtomicReference<@Nullable Optional<StoredSagaWithFirstSkippedInputKey>> read = new AtomicReference<>();

        Throwable wholeRead = catchThrowable(() -> store.findWithFirstSkippedInput("order-1"));
        Throwable keyRead = catchThrowable(() -> read.set(store.findWithFirstSkippedInputKey("order-1")));

        assertAll(
                () -> assertThat(wholeRead).as("the event no longer decodes").isNotNull(),
                () -> assertThat(keyRead).as("the key read decodes nothing of the event").isNull(),
                () -> assertThat(read.get()).isNotNull().isPresent(),
                () -> assertThat(read.get().orElseThrow().first()).isEqualTo(new RecordKey(1, "a@1"))
        );
    }

    @Test
    void the_key_read_returns_the_first_record_when_it_is_split_across_chunks_that_are_gone_where_the_read_of_the_record_throws() {
        storedReleasing("order-1", "state");
        skipped("order-1", 1, "a@1");
        skipped("order-1", 2, "b@2");
        assertThat(template.getCollection(SKIPPED_COLLECTION).updateOne(new Document("sagaId", "order-1").append("key", "a@1"), Updates.combine(Updates.set("chunks", 2), Updates.set("size", 10))).getModifiedCount()).isEqualTo(1L);
        AtomicReference<@Nullable Optional<StoredSagaWithFirstSkippedInputKey>> read = new AtomicReference<>();

        Throwable wholeRead = catchThrowable(() -> store.findWithFirstSkippedInput("order-1"));
        Throwable keyRead = catchThrowable(() -> read.set(store.findWithFirstSkippedInputKey("order-1")));

        assertAll(
                () -> assertThat(wholeRead).as("the chunks of the record are gone").isNotNull().hasMessageContaining("chunks"),
                () -> assertThat(keyRead).as("the key read reads no chunk").isNull(),
                () -> assertThat(read.get()).isNotNull().isPresent(),
                () -> assertThat(read.get().orElseThrow().first()).isEqualTo(new RecordKey(1, "a@1"))
        );
    }

    @Test
    void the_key_read_returns_no_record_for_an_instance_without_a_quarantine_id() {
        assertThat(envelopes.compareAndSave("order-2", active("order-2", "state", 1), 0)).isTrue();

        Optional<StoredSagaWithFirstSkippedInputKey> read = store.findWithFirstSkippedInputKey("order-2");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().first()).isNull()
        );
    }

    @Test
    void the_key_read_returns_no_record_when_none_is_left_under_the_quarantine_id() {
        storedReleasing("order-3", "state");

        Optional<StoredSagaWithFirstSkippedInputKey> read = store.findWithFirstSkippedInputKey("order-3");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().first()).isNull()
        );
    }

    @Test
    void the_key_read_returns_no_record_when_the_only_records_are_under_another_quarantine_id() {
        storedReleasing("order-4", "state");
        skipped("order-4", 1, "a@1");
        assertThat(template.getCollection(SKIPPED_COLLECTION).updateOne(new Document("sagaId", "order-4"), Updates.set("quarantineId", "q-other")).getModifiedCount()).isEqualTo(1L);

        assertThat(store.findWithFirstSkippedInputKey("order-4").orElseThrow().first()).isNull();
    }

    @Test
    void the_key_read_returns_empty_for_an_instance_that_exists_nowhere() {
        assertThat(store.findWithFirstSkippedInputKey("nobody")).isEmpty();
    }

    @Test
    void ending_a_quarantine_leaves_the_stored_state_alone_even_when_handed_another_and_writes_every_other_field() {
        storedReleasing("order-1", "original");
        Document before = rawInstance("order-1");

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encode(active("order-1", "handed another state", 2)), 1, QUARANTINE_ID);

        Document after = rawInstance("order-1");
        SagaEnvelope<String> found = envelopes.findWithoutState("order-1").orElseThrow();
        assertAll(
                () -> assertThat(ended).isTrue(),
                () -> assertThat(after.get(SagaCodec.STATE)).as("the stored state").isNotNull().isEqualTo(before.get(SagaCodec.STATE)),
                () -> assertThat(envelopes.find("order-1").orElseThrow().state()).isEqualTo("original"),
                () -> assertThat(found.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(found.version()).isEqualTo(2L),
                () -> assertThat(found.failure()).as("the failure the quarantine left").isNull(),
                () -> assertThat(found.quarantineId()).isNull(),
                () -> assertThat(found.quarantineSkipCount()).isNull()
        );
    }

    @Test
    void ending_a_quarantine_with_a_saga_that_holds_no_state_leaves_the_stored_state_alone_too() {
        storedReleasing("order-1", "original");
        Document before = rawInstance("order-1");

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("order-1", "ignored", 2)), 1, QUARANTINE_ID);

        assertAll(
                () -> assertThat(ended).isTrue(),
                () -> assertThat(rawInstance("order-1").get(SagaCodec.STATE)).as("the stored state").isNotNull().isEqualTo(before.get(SagaCodec.STATE)),
                () -> assertThat(envelopes.findWithoutState("order-1").orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void ending_a_quarantine_is_lost_while_a_record_is_left_and_writes_nothing() {
        storedReleasing("order-1", "original");
        skipped("order-1", 1, "a@1");
        Document before = rawInstance("order-1");

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("order-1", "ignored", 3)), 2, QUARANTINE_ID);

        assertAll(
                () -> assertThat(ended).isFalse(),
                () -> assertThat(rawInstance("order-1")).isEqualTo(before),
                () -> assertThat(template.getCollection(SKIPPED_COLLECTION).countDocuments()).isEqualTo(1L)
        );
    }

    @Test
    void ending_a_quarantine_at_a_stale_version_is_lost_and_writes_nothing() {
        storedReleasing("order-1", "original");
        Document before = rawInstance("order-1");

        boolean ended = store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("order-1", "ignored", 6)), 5, QUARANTINE_ID);

        assertAll(
                () -> assertThat(ended).isFalse(),
                () -> assertThat(rawInstance("order-1")).isEqualTo(before)
        );
    }

    @Test
    void ending_a_quarantine_of_an_instance_that_is_not_stored_is_refused_naming_the_instance_when_it_would_insert_it() {
        assertThatThrownBy(() -> store.compareAndSaveQuarantineEnded(codec.encodeWithoutState(active("order-9", "ignored", 1)), 0, QUARANTINE_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("order-9");
        assertThat(template.getCollection(COLLECTION).countDocuments()).isZero();
    }
}
