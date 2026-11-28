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
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set that accepts MongoDB's {@code failCommand} fail point. A record over 16 MiB is split
 * across chunks in a collection of its own, written before the record and deleted after it in one transaction. Checks
 * what is stored, that a record whose chunks don't add up is never read as a whole event, and that a
 * failure between the chunk writes and the record write, or between the record delete and the chunk delete, does not leave
 * anything behind in the store's own transaction and nothing misread in a caller's.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class SpringMongoSagaStateStoreSkippedInputChunksTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-chunks";
    private static final String RECORDS = COLLECTION + "_skipped_inputs";
    private static final String CHUNKS = COLLECTION + "_skipped_input_chunks";
    private static final String QUARANTINE_ID = "q-1";
    private static final String SAGA_ID = "order-1";
    private static final String KEY = "big@1";
    private static final int UNKNOWN_ERROR = 8;
    private static final int OVER_A_DOCUMENT = 17 * 1024 * 1024;
    private static final int CHUNK_SIZE = 8 * 1024 * 1024;

    private final List<String> sent = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<String> store;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-chunks-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonValue target = event.getCommand().get(event.getCommandName());
                sent.add(event.getCommandName() + " " + (target != null && target.isString() ? target.asString().getValue() : ""));
            }
        }).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class, null, RetryStrategy.none());
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(SAGA_ID, quarantined(), 0)).isTrue();
    }

    @AfterEach
    void close() {
        client.close();
    }

    @Test
    void an_event_over_16_MiB_is_held_in_chunks_of_8_MiB_and_its_record_holds_only_the_counts() {
        byte[] bytes = bytes(OVER_A_DOCUMENT, 1);

        assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).isTrue();

        Document record = template.getCollection(RECORDS).find().first();
        List<Document> chunks = chunks();
        assertAll(
                () -> assertThat(requireNonNull(record).keySet()).containsExactlyInAnyOrder("_id", "sagaId", "quarantineId", "key", "seq", "chunks", "size"),
                () -> assertThat(requireNonNull(record).getInteger("chunks")).isEqualTo(3),
                () -> assertThat(chunks).extracting(chunk -> chunk.getInteger("chunk")).containsExactly(0, 1, 2),
                () -> assertThat(chunks).extracting(chunk -> ((Binary) chunk.get("bytes")).length()).containsExactly(CHUNK_SIZE, CHUNK_SIZE, requireNonNull(record).getInteger("size") - 2 * CHUNK_SIZE),
                () -> assertThat(chunks).allSatisfy(chunk -> assertThat(chunk).containsEntry("sagaId", SAGA_ID).containsEntry("quarantineId", QUARANTINE_ID).containsEntry("key", KEY))
        );
    }

    @Test
    void the_chunks_have_a_unique_index_on_the_saga_id_the_quarantine_id_the_key_and_the_chunk() {
        List<Document> indexes = template.getCollection(CHUNKS).listIndexes().into(new ArrayList<>());

        assertThat(indexes).filteredOn(index -> Boolean.TRUE.equals(index.getBoolean("unique"))).extracting(index -> ((Document) index.get("key")).keySet())
                .containsExactly(Set.of("sagaId", "quarantineId", "key", "chunk"));
    }

    @Test
    void the_chunks_are_written_before_the_record_that_points_at_them() {
        sent.clear();

        assertThat(skip(new SkippedInput(1, KEY, event(bytes(OVER_A_DOCUMENT, 8))))).isTrue();

        assertThat(sent.stream().filter(command -> command.startsWith("update "))).containsExactly("update " + COLLECTION, "update " + CHUNKS, "update " + CHUNKS, "update " + CHUNKS, "update " + RECORDS);
    }

    @Test
    void the_record_is_deleted_before_its_chunks() {
        SkippedInput input = new SkippedInput(1, KEY, event(bytes(OVER_A_DOCUMENT, 9)));
        assertThat(skip(input)).isTrue();
        sent.clear();

        assertThat(replayed(input)).isTrue();

        assertThat(sent.stream().filter(command -> command.startsWith("delete "))).containsExactly("delete " + RECORDS, "delete " + CHUNKS);
    }

    @Test
    void a_record_with_a_small_event_has_no_chunks() {
        assertThat(skip(new SkippedInput(1, KEY, event(new byte[]{1, 2, 3})))).isTrue();

        assertThat(chunks()).isEmpty();
    }

    @Test
    void a_collection_name_ending_in_the_suffix_of_the_chunks_collection_is_refused_naming_the_suffix() {
        Throwable thrown = catchThrowable(() -> new SpringMongoSagaStateStore<>(template, "orders_skipped_input_chunks", String.class));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'orders_skipped_input_chunks'").hasMessageContaining("ends in '_skipped_input_chunks'");
    }

    @Test
    void a_collection_name_ending_in_the_suffix_of_the_skipped_inputs_collection_is_refused_naming_the_suffix() {
        Throwable thrown = catchThrowable(() -> new SpringMongoSagaStateStore<>(template, "orders_skipped_inputs", String.class));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'orders_skipped_inputs'").hasMessageContaining("ends in '_skipped_inputs'");
    }

    @Nested
    class A_record_whose_chunks_do_not_add_up {

        private byte[] bytes;

        @BeforeEach
        void aChunkedRecord() {
            bytes = bytes(OVER_A_DOCUMENT, 2);
            assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).isTrue();
            assertThat(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10)).as("the record reads as the event while its chunks are there").hasSize(1);
        }

        @Test
        void throws_IllegalStateException_when_a_chunk_is_missing() {
            template.getCollection(CHUNKS).deleteOne(new Document("chunk", 2));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + KEY + "'").hasMessageContaining("split across 3 chunks");
        }

        @Test
        void throws_IllegalStateException_when_every_chunk_is_missing() {
            template.getCollection(CHUNKS).deleteMany(new Document());

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("the chunks found under it are 0 with 0 bytes");
        }

        @Test
        void throws_IllegalStateException_when_the_total_size_is_one_byte_more_than_the_chunks_hold() {
            template.getCollection(RECORDS).updateOne(new Document(), new Document("$inc", new Document("size", 1)));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + KEY + "'");
        }

        @Test
        void throws_IllegalStateException_when_the_total_size_is_one_byte_less_than_the_chunks_hold() {
            template.getCollection(RECORDS).updateOne(new Document(), new Document("$inc", new Document("size", -1)));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + KEY + "'");
        }

        @Test
        void throws_IllegalStateException_when_the_chunk_count_is_one_more_than_the_chunks_stored() {
            template.getCollection(RECORDS).updateOne(new Document(), new Document("$inc", new Document("chunks", 1)));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("split across 4 chunks");
        }

        @Test
        void throws_IllegalStateException_when_the_chunk_count_is_one_fewer_than_the_chunks_stored() {
            template.getCollection(RECORDS).updateOne(new Document(), new Document("$inc", new Document("chunks", -1)));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("split across 2 chunks");
        }

        @Test
        void throws_IllegalStateException_when_the_chunks_are_not_numbered_from_0_in_order() {
            template.getCollection(CHUNKS).updateOne(new Document("chunk", 1), new Document("$set", new Document("chunk", -1)));

            Throwable thrown = catchThrowable(() -> store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10));

            assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + KEY + "'");
        }

        @Test
        void is_not_read_as_a_whole_event_by_a_release() {
            template.getCollection(CHUNKS).deleteOne(new Document("chunk", 0));

            Throwable thrown = catchThrowable(() -> store.findWithFirstSkippedInput(SAGA_ID));

            assertThat(thrown).as("the first record of a release is not handed over as a shorter event").isNotNull();
        }
    }

    @Nested
    class Chunks_that_no_record_points_at {

        @BeforeEach
        void orphans() {
            for (int index = 0; index < 4; index++) {
                template.getCollection(CHUNKS).insertOne(new Document("sagaId", SAGA_ID).append("quarantineId", QUARANTINE_ID).append("key", KEY).append("chunk", index).append("bytes", new byte[]{9, 9, 9}));
            }
        }

        @Test
        void are_ignored_by_a_read() {
            assertThat(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10)).isEmpty();
        }

        @Test
        void are_overwritten_by_a_later_skip_of_the_same_key_and_a_chunk_beyond_its_count_is_ignored() {
            byte[] bytes = bytes(OVER_A_DOCUMENT, 3);

            assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).isTrue();

            List<SkippedInput> read = store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10);
            assertAll(
                    () -> assertThat(read).hasSize(1),
                    () -> assertThat(requireNonNull(read.getFirst().event().getData()).toBytes()).isEqualTo(bytes),
                    () -> assertThat(chunks()).as("the chunk beyond the count of the record is left").hasSize(4)
            );
        }

        @Test
        void are_removed_by_a_replay_of_a_record_of_the_same_key() {
            assertThat(skip(new SkippedInput(1, KEY, event(new byte[]{1, 2, 3})))).isTrue();
            assertThat(chunks()).as("the record is small, so the orphans are left").hasSize(4);

            assertThat(replayed(new SkippedInput(1, KEY, event(new byte[]{1, 2, 3})))).isTrue();

            assertAll(
                    () -> assertThat(template.getCollection(RECORDS).countDocuments()).isZero(),
                    () -> assertThat(chunks()).isEmpty()
            );
        }

        @Test
        void are_removed_when_the_quarantine_ends() {
            assertThat(ended()).isTrue();

            assertThat(chunks()).isEmpty();
        }

        @Test
        void are_removed_when_the_instance_is_deleted() {
            store.delete(SAGA_ID);

            assertThat(chunks()).isEmpty();
        }
    }

    @Nested
    class A_failure_between_the_chunk_writes_and_the_record_write {

        // The instance, the three chunks and then the record, which is the fifth update
        private static final int UPDATES_BEFORE_THE_RECORD = 4;

        @Test
        void in_the_transaction_of_the_store_leaves_nothing_and_a_later_skip_of_the_same_input_is_held() {
            byte[] bytes = bytes(OVER_A_DOCUMENT, 4);

            Throwable thrown;
            try (FailPoint ignored = FailPoint.failingAfter(client, "update", UPDATES_BEFORE_THE_RECORD, UNKNOWN_ERROR)) {
                thrown = catchThrowable(() -> skip(new SkippedInput(1, KEY, event(bytes))));
            }

            assertAll(
                    () -> assertThat(thrown).as("the failure reaches the caller").isNotNull(),
                    () -> assertThat(chunks()).as("no chunk is left").isEmpty(),
                    () -> assertThat(template.getCollection(RECORDS).countDocuments()).as("no record is left").isZero(),
                    () -> assertThat(SagaEnvelopes.of(store, null).find(SAGA_ID).orElseThrow().version()).as("the instance is not written").isEqualTo(1L),
                    () -> assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).as("the same skip applies once the failure is gone").isTrue(),
                    () -> assertThat(requireNonNull(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10).getFirst().event().getData()).toBytes()).isEqualTo(bytes)
            );
        }

        @Test
        void in_the_transaction_of_a_caller_that_catches_it_and_commits_anyway_is_rolled_back_and_leaves_nothing() {
            byte[] bytes = bytes(OVER_A_DOCUMENT, 5);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Throwable commit;
            try (FailPoint ignored = FailPoint.failingAfter(client, "update", UPDATES_BEFORE_THE_RECORD, UNKNOWN_ERROR)) {
                commit = catchThrowable(() -> callersTransaction().executeWithoutResult(status -> failure.set(catchThrowable(() -> skip(new SkippedInput(1, KEY, event(bytes)))))));
            }

            assertAll(
                    () -> assertThat(failure.get()).as("the failure reaches the caller").isNotNull(),
                    () -> assertThat(commit).as("the store marked the transaction of the caller rollback-only, so the commit is refused").isInstanceOf(UnexpectedRollbackException.class),
                    () -> assertThat(chunks()).as("no chunk is left").isEmpty(),
                    () -> assertThat(template.getCollection(RECORDS).countDocuments()).as("no record is left").isZero(),
                    () -> assertThat(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10)).as("a release reads nothing").isEmpty()
            );
        }
    }

    @Nested
    class A_failure_between_the_record_delete_and_the_chunk_delete {

        @Test
        void in_the_transaction_of_the_store_leaves_the_record_and_its_chunks_and_the_replay_applies_once_the_failure_is_gone() {
            byte[] bytes = bytes(OVER_A_DOCUMENT, 6);
            assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).isTrue();

            Throwable thrown;
            try (FailPoint ignored = FailPoint.failingAfter(client, "delete", 1, UNKNOWN_ERROR)) {
                thrown = catchThrowable(() -> replayed(new SkippedInput(1, KEY, event(bytes))));
            }

            assertAll(
                    () -> assertThat(thrown).as("the failure reaches the caller").isNotNull(),
                    () -> assertThat(template.getCollection(RECORDS).countDocuments()).as("the record is left").isEqualTo(1),
                    () -> assertThat(chunks()).as("its chunks are left").hasSize(3),
                    () -> assertThat(requireNonNull(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10).getFirst().event().getData()).toBytes()).as("the record reads as the event").isEqualTo(bytes),
                    () -> assertThat(replayed(new SkippedInput(1, KEY, event(bytes)))).as("the same replay applies once the failure is gone").isTrue(),
                    () -> assertThat(chunks()).as("and removes the chunks").isEmpty()
            );
        }

        @Test
        void in_the_transaction_of_a_caller_that_catches_it_and_commits_anyway_is_rolled_back_and_leaves_the_record_and_its_chunks() {
            byte[] bytes = bytes(OVER_A_DOCUMENT, 7);
            assertThat(skip(new SkippedInput(1, KEY, event(bytes)))).isTrue();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Throwable commit;
            try (FailPoint ignored = FailPoint.failingAfter(client, "delete", 1, UNKNOWN_ERROR)) {
                commit = catchThrowable(() -> callersTransaction().executeWithoutResult(status -> failure.set(catchThrowable(() -> replayed(new SkippedInput(1, KEY, event(bytes)))))));
            }

            assertAll(
                    () -> assertThat(failure.get()).as("the failure reaches the caller").isNotNull(),
                    () -> assertThat(commit).as("the store marked the transaction of the caller rollback-only, so the commit is refused").isInstanceOf(UnexpectedRollbackException.class),
                    () -> assertThat(template.getCollection(RECORDS).countDocuments()).as("the record is left").isEqualTo(1),
                    () -> assertThat(chunks()).as("its chunks are left").hasSize(3),
                    () -> assertThat(requireNonNull(store.findSkippedInputs(SAGA_ID, QUARANTINE_ID, 10).getFirst().event().getData()).toBytes()).as("the record reads as the event").isEqualTo(bytes)
            );
        }
    }

    private TransactionTemplate callersTransaction() {
        return new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory()));
    }

    private List<Document> chunks() {
        return template.getCollection(CHUNKS).find().sort(new Document("chunk", 1)).into(new ArrayList<>());
    }

    private static byte[] bytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static CloudEvent event(byte[] bytes) {
        return CloudEventBuilder.v1().withId(KEY).withSource(URI.create("urn:test")).withType("big").withSubject(SAGA_ID).withData(BytesCloudEventData.wrap(bytes)).build();
    }

    private static SagaEnvelope<String> quarantined() {
        return new SagaEnvelope<>(SAGA_ID, "placed", SagaStatus.QUARANTINED, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true,
                new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID), Map.of(), 0L);
    }

    private SagaEnvelope<String> next() {
        SagaEnvelope<String> e = SagaEnvelopes.of(store, null).find(SAGA_ID).orElseThrow();
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version() + 1, e.timers(), e.streamWatermarks(), null, e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private long version() {
        return SagaEnvelopes.of(store, null).find(SAGA_ID).orElseThrow().version();
    }

    private boolean skip(SkippedInput input) {
        return SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, next(), version(), QUARANTINE_ID, input);
    }

    private boolean replayed(SkippedInput input) {
        return SagaEnvelopes.of(store, null).compareAndSaveReplayed(store, next(), version(), QUARANTINE_ID, input);
    }

    private boolean ended() {
        return SagaEnvelopes.of(store, null).compareAndSaveQuarantineEnded(store, next(), version(), QUARANTINE_ID);
    }
}
