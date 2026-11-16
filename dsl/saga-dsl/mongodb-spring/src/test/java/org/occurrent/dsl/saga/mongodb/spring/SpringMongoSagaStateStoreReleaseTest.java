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
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. Checks against real MongoDB that the release time, the quarantine id and the skip count survive a save,
 * reach the reads that skip the state and go away when a later write clears them. It also checks that the records of
 * skipped inputs, which live in a collection of their own, are read in seq order and deleted with the instance. A
 * document written before 0.35.0 reads back as an instance that is not releasing and has no quarantine id.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreReleaseTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant RELEASED_AT = NOW.plusSeconds(30);
    private static final String COLLECTION = "saga-release";
    private static final String QUARANTINE_ID = "q-1";

    record Payment(long amount) {
    }

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-release-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    private static SpringMongoSagaStateStore<Payment> store(MongoOperations mongoOperations) {
        return new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
    }

    private static SagaFailure failure(@Nullable Instant releasedAt, @Nullable String quarantineId) {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", releasedAt, quarantineId);
    }

    private static SagaEnvelope<Payment> envelope(String sagaId, SagaStatus status, long version, @Nullable SagaFailure failure,
                                                  @Nullable Long skipCount) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), status, version, List.of(), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), skipCount);
    }

    private static SagaEnvelope<Payment> releasing(String sagaId) {
        return envelope(sagaId, SagaStatus.QUARANTINED, 1, failure(RELEASED_AT, QUARANTINE_ID), 4L);
    }

    private static Document stored(MongoOperations mongoOperations, String sagaId) {
        return requireNonNull(mongoOperations.findOne(Query.query(where("_id").is(sagaId)), Document.class, COLLECTION));
    }

    // The document 0.34.0 wrote for a quarantined instance, with its failure record and none of the 0.35.0 fields
    private static Document quarantinedBy034(String sagaId) {
        return new Document("_id", sagaId).append("status", "QUARANTINED").append("version", 1L)
                .append("state", new Document("amount", 12L)).append("timers", List.of()).append("started", true)
                .append("createdAt", NOW.minusSeconds(600).toEpochMilli()).append("updatedAt", NOW.toEpochMilli())
                .append("failureInput", "order-1@7").append("failurePosition", 7L)
                .append("failureFirstFailedAt", NOW.minusSeconds(300).toEpochMilli())
                .append("failureType", IllegalStateException.class.getName()).append("failureMessage", "boom");
    }

    @Test
    void a_releasing_instance_round_trips_with_its_release_time_its_quarantine_id_and_its_skip_count() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.compareAndSave("order-1", releasing("order-1"), 0);

        SagaEnvelope<Payment> read = store.find("order-1").orElseThrow();

        assertAll(
                () -> assertThat(read.failure()).isEqualTo(failure(RELEASED_AT, QUARANTINE_ID)),
                () -> assertThat(read.isReleasing()).isTrue(),
                () -> assertThat(read.quarantineId()).isEqualTo(QUARANTINE_ID),
                () -> assertThat(read.quarantineSkipCount()).isEqualTo(4L)
        );
    }

    @Test
    void a_skip_count_of_0_reads_back_as_0_and_not_as_no_count() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.compareAndSave("order-2", envelope("order-2", SagaStatus.QUARANTINED, 1, failure(null, QUARANTINE_ID), 0L), 0);

        SagaEnvelope<Payment> read = store.find("order-2").orElseThrow();

        assertAll(
                () -> assertThat(read.quarantineSkipCount()).isEqualTo(0L),
                () -> assertThat(read.quarantineId()).isEqualTo(QUARANTINE_ID),
                () -> assertThat(read.isReleasing()).isFalse()
        );
    }

    @Test
    void a_quarantined_instance_with_a_quarantine_id_is_stored_as_QUARANTINED_RECORDING_and_one_without_as_QUARANTINED() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("with-id", envelope("with-id", SagaStatus.QUARANTINED, 1, failure(null, QUARANTINE_ID), 0L), 0);
        store.compareAndSave("without-id", envelope("without-id", SagaStatus.QUARANTINED, 1, failure(null, null), null), 0);
        store.compareAndSave("active", envelope("active", SagaStatus.ACTIVE, 1, null, null), 0);

        assertAll(
                () -> assertThat(stored(mongoOperations, "with-id").getString("status")).isEqualTo("QUARANTINED_RECORDING"),
                () -> assertThat(stored(mongoOperations, "without-id").getString("status")).isEqualTo("QUARANTINED"),
                () -> assertThat(stored(mongoOperations, "active").getString("status")).isEqualTo("ACTIVE"),
                () -> assertThat(store.find("with-id").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(store.find("without-id").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED)
        );
    }

    @Test
    void both_status_strings_read_back_as_QUARANTINED_whatever_wrote_them() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        mongoOperations.insert(quarantinedBy034("by-034"), COLLECTION);
        mongoOperations.insert(quarantinedBy034("recording").append("status", "QUARANTINED_RECORDING").append("failureQuarantineId", QUARANTINE_ID)
                .append("quarantineSkipCount", 2L), COLLECTION);

        assertAll(
                () -> assertThat(store.find("by-034").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(store.find("recording").orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(store.find("recording").orElseThrow().quarantineId()).isEqualTo(QUARANTINE_ID)
        );
    }

    @Test
    void findByStatus_returns_the_instances_stored_under_either_quarantined_string() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("with-id", envelope("with-id", SagaStatus.QUARANTINED, 1, failure(null, QUARANTINE_ID), 0L), 0);
        mongoOperations.insert(quarantinedBy034("by-034"), COLLECTION);
        store.compareAndSave("active", envelope("active", SagaStatus.ACTIVE, 1, null, null), 0);

        List<String> found = store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(60), 10).stream().map(SagaEnvelope::sagaId).sorted().toList();

        assertThat(found).containsExactly("by-034", "with-id");
    }

    @Test
    void the_releasing_query_finds_only_quarantined_instances_with_a_release_time_in_id_order_under_either_string() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("order-c", releasing("order-c"), 0);
        store.compareAndSave("order-a", releasing("order-a"), 0);
        mongoOperations.insert(quarantinedBy034("order-b").append("failureReleasedAt", RELEASED_AT.toEpochMilli()), COLLECTION);
        store.compareAndSave("order-0", envelope("order-0", SagaStatus.QUARANTINED, 1, failure(null, QUARANTINE_ID), 0L), 0);
        store.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, 1, failure(RELEASED_AT, null), null), 0);

        assertAll(
                () -> assertThat(store.findReleasingSagaIds(null, 10)).containsExactly("order-a", "order-b", "order-c"),
                () -> assertThat(store.findReleasingSagaIds(null, 2)).containsExactly("order-a", "order-b")
        );
    }

    @Test
    void the_releasing_query_lists_only_the_ids_strictly_after_the_given_one_in_id_order_up_to_the_limit() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        for (String id : List.of("order-c", "order-a", "order-d", "order-b")) {
            store.compareAndSave(id, releasing(id), 0);
        }
        store.compareAndSave("order-bb", envelope("order-bb", SagaStatus.ACTIVE, 1, null, null), 0);

        assertAll(
                () -> assertThat(store.findReleasingSagaIds("order-a", 10)).containsExactly("order-b", "order-c", "order-d"),
                () -> assertThat(store.findReleasingSagaIds("order-a", 2)).containsExactly("order-b", "order-c"),
                () -> assertThat(store.findReleasingSagaIds("order-b", 1)).containsExactly("order-c"),
                () -> assertThat(store.findReleasingSagaIds("order-bb", 10)).containsExactly("order-c", "order-d"),
                () -> assertThat(store.findReleasingSagaIds("order-d", 10)).isEmpty()
        );
    }

    @Test
    void the_releasing_query_refuses_a_limit_mongo_would_read_as_no_limit() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());

        assertThatThrownBy(() -> store.findReleasingSagaIds(null, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit must be positive");
    }

    @Test
    void the_reads_without_the_state_carry_the_release_fields_of_an_instance_whose_state_no_longer_decodes() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("order-3", releasing("order-3"), 0);
        mongoOperations.updateFirst(Query.query(where("_id").is("order-3")),
                new Update().set("state", new Document("amount", "lots")), COLLECTION);

        SagaEnvelope<Payment> byStatus = store.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(60), 10).getFirst();
        SagaEnvelope<Payment> withoutState = store.findWithoutState("order-3").orElseThrow();

        assertAll(
                () -> assertThat(byStatus.failure()).isEqualTo(failure(RELEASED_AT, QUARANTINE_ID)),
                () -> assertThat(byStatus.quarantineSkipCount()).isEqualTo(4L),
                () -> assertThat(withoutState.failure()).isEqualTo(failure(RELEASED_AT, QUARANTINE_ID)),
                () -> assertThat(withoutState.quarantineSkipCount()).isEqualTo(4L)
        );
    }

    @Test
    void a_save_without_the_state_removes_the_release_fields_an_instance_made_active_no_longer_carries() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("order-4", releasing("order-4"), 0);

        boolean saved = store.compareAndSaveWithoutState("order-4", envelope("order-4", SagaStatus.ACTIVE, 2, null, null), 1);

        Document stored = stored(mongoOperations, "order-4");
        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(stored).doesNotContainKeys("failureReleasedAt", "failureQuarantineId", "quarantineSkipCount"),
                () -> assertThat(stored.getString("status")).isEqualTo("ACTIVE"),
                () -> assertThat(store.findReleasingSagaIds(null, 10)).isEmpty()
        );
    }

    @Test
    void a_save_without_the_state_that_clears_the_release_time_keeps_the_quarantine_id_and_the_skip_count_and_stops_the_instance_releasing() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        store.compareAndSave("order-5", releasing("order-5"), 0);

        store.compareAndSaveWithoutState("order-5", envelope("order-5", SagaStatus.QUARANTINED, 2, failure(null, QUARANTINE_ID), 4L), 1);

        SagaEnvelope<Payment> read = store.find("order-5").orElseThrow();
        assertAll(
                () -> assertThat(stored(mongoOperations, "order-5")).doesNotContainKey("failureReleasedAt"),
                () -> assertThat(read.isReleasing()).isFalse(),
                () -> assertThat(read.quarantineId()).isEqualTo(QUARANTINE_ID),
                () -> assertThat(read.quarantineSkipCount()).isEqualTo(4L),
                () -> assertThat(store.findReleasingSagaIds(null, 10)).isEmpty()
        );
    }

    @Test
    void a_quarantined_document_written_before_0_35_0_is_not_releasing_and_has_no_quarantine_id_and_no_skip_count() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations);
        mongoOperations.insert(quarantinedBy034("order-6"), COLLECTION);

        SagaEnvelope<Payment> read = store.find("order-6").orElseThrow();

        assertAll(
                () -> assertThat(read.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(read.failure()).isEqualTo(failure(null, null)),
                () -> assertThat(read.isReleasing()).isFalse(),
                () -> assertThat(read.quarantineId()).isNull(),
                () -> assertThat(read.quarantineSkipCount()).isNull(),
                () -> assertThat(store.findReleasingSagaIds(null, 10)).isEmpty()
        );
    }

    @Test
    void records_are_read_in_seq_order_whatever_order_they_were_written_in() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-7", "q-1", new SkippedInput(7, "b@2"));
        store.recordSkippedInput("order-7", "q-1", new SkippedInput(-1, "a@1"));
        store.recordSkippedInput("order-7", "q-1", new SkippedInput(3, "c@3"));

        assertThat(store.findSkippedInputs("order-7", "q-1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(3, "c@3"), new SkippedInput(7, "b@2"));
    }

    @Test
    void records_of_the_same_seq_are_read_in_key_order_whatever_order_they_were_written_in_and_in_the_same_order_on_every_read() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(-1, "b@2"));
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(3, "d@4"));
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(-1, "a@1"));
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(3, "c@3"));

        List<SkippedInput> first = store.findSkippedInputs("order-9", "q-1", 10);
        List<SkippedInput> second = store.findSkippedInputs("order-9", "q-1", 10);

        assertAll(
                () -> assertThat(first).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(-1, "b@2"), new SkippedInput(3, "c@3"), new SkippedInput(3, "d@4")),
                () -> assertThat(second).isEqualTo(first)
        );
    }

    @Test
    void a_second_record_of_a_key_keeps_the_first_record_and_its_seq() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-8", "q-1", new SkippedInput(2, "a@1"));

        store.recordSkippedInput("order-8", "q-1", new SkippedInput(5, "a@1"));

        assertThat(store.findSkippedInputs("order-8", "q-1", 10)).containsExactly(new SkippedInput(2, "a@1"));
    }

    @Test
    void records_are_read_for_one_saga_and_quarantine_id_up_to_the_limit() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-9", "q-1", new SkippedInput(1, "b@2"));
        store.recordSkippedInput("order-9", "q-2", new SkippedInput(0, "c@3"));
        store.recordSkippedInput("order-10", "q-1", new SkippedInput(0, "d@4"));

        assertAll(
                () -> assertThat(store.findSkippedInputs("order-9", "q-1", 10)).containsExactly(new SkippedInput(0, "a@1"), new SkippedInput(1, "b@2")),
                () -> assertThat(store.findSkippedInputs("order-9", "q-1", 1)).containsExactly(new SkippedInput(0, "a@1"))
        );
    }

    @Test
    void records_cannot_be_read_with_a_limit_below_1_since_mongo_reads_0_as_no_limit() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());

        assertThatThrownBy(() -> store.findSkippedInputs("order-11", "q-1", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_record_is_inserted_when_the_key_has_none_and_it_is_recorded_at_the_head() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-12", "q-1", new SkippedInput(0, "b@2"));

        store.recordSkippedInputAtHead("order-12", "q-1", new SkippedInput(-1, "a@1"));

        assertThat(store.findSkippedInputs("order-12", "q-1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(0, "b@2"));
    }

    @Test
    void the_record_of_a_key_moves_to_the_seq_it_is_recorded_at_the_head_with_and_stays_one_record() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-moved", "q-1", new SkippedInput(4, "a@1"));
        store.recordSkippedInput("order-moved", "q-1", new SkippedInput(5, "b@2"));

        store.recordSkippedInputAtHead("order-moved", "q-1", new SkippedInput(-1, "a@1"));

        assertThat(store.findSkippedInputs("order-moved", "q-1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(5, "b@2"));
    }

    @Test
    void a_record_of_a_key_at_a_later_seq_moves_to_the_head_when_the_key_is_recorded_at_the_head() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-head-wins", "q-1", new SkippedInput(7, "a@1"));

        store.recordSkippedInputAtHead("order-head-wins", "q-1", new SkippedInput(-1, "a@1"));

        assertThat(store.findSkippedInputs("order-head-wins", "q-1", 10)).containsExactly(new SkippedInput(-1, "a@1"));
    }

    @Test
    void the_head_record_of_a_key_is_the_one_left_when_another_caller_records_it_at_a_later_seq_at_once() throws Exception {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 100; round++) {
                String sagaId = "order-race-" + round;
                CountDownLatch bothReady = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<?>> writers = List.of(
                        executor.submit(() -> {
                            bothReady.countDown();
                            go.await();
                            store.recordSkippedInputAtHead(sagaId, "q-1", new SkippedInput(-1, "a@1"));
                            return null;
                        }),
                        executor.submit(() -> {
                            bothReady.countDown();
                            go.await();
                            store.recordSkippedInput(sagaId, "q-1", new SkippedInput(7, "a@1"));
                            return null;
                        }));
                assertThat(bothReady.await(10, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                for (Future<?> writer : writers) {
                    writer.get(20, TimeUnit.SECONDS);
                }

                assertThat(store.findSkippedInputs(sagaId, "q-1", 10)).hasSize(1).singleElement()
                        .isEqualTo(new SkippedInput(-1, "a@1"));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void a_record_is_deleted_exactly_when_its_key_and_its_seq_both_match() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-16", "q-1", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-16", "q-1", new SkippedInput(1, "b@2"));

        store.deleteSkippedInput("order-16", "q-1", new SkippedInput(0, "a@1"));

        assertThat(store.findSkippedInputs("order-16", "q-1", 10)).containsExactly(new SkippedInput(1, "b@2"));
    }

    @Test
    void a_record_of_the_same_key_with_another_seq_is_kept_when_deleted_exactly() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-17", "q-1", new SkippedInput(-1, "a@1"));

        store.deleteSkippedInput("order-17", "q-1", new SkippedInput(3, "a@1"));

        assertThat(store.findSkippedInputs("order-17", "q-1", 10)).containsExactly(new SkippedInput(-1, "a@1"));
    }

    @Test
    void a_record_of_the_same_key_and_seq_under_another_quarantine_id_or_instance_is_kept_when_deleted_exactly() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-18", "q-1", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-18", "q-2", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-19", "q-1", new SkippedInput(0, "a@1"));

        store.deleteSkippedInput("order-18", "q-1", new SkippedInput(0, "a@1"));

        assertAll(
                () -> assertThat(store.findSkippedInputs("order-18", "q-1", 10)).isEmpty(),
                () -> assertThat(store.findSkippedInputs("order-18", "q-2", 10)).containsExactly(new SkippedInput(0, "a@1")),
                () -> assertThat(store.findSkippedInputs("order-19", "q-1", 10)).containsExactly(new SkippedInput(0, "a@1"))
        );
    }

    @Test
    void delete_removes_the_instance_and_its_records_under_its_quarantine_id_only_and_no_other_instance_records() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.compareAndSave("order-13", releasing("order-13"), 0);
        store.recordSkippedInput("order-13", "q-1", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-13", "q-2", new SkippedInput(0, "b@1"));
        store.recordSkippedInput("order-14", "q-1", new SkippedInput(0, "c@1"));

        store.delete("order-13");

        assertAll(
                () -> assertThat(store.find("order-13")).isEmpty(),
                () -> assertThat(store.findSkippedInputs("order-13", "q-1", 10)).isEmpty(),
                () -> assertThat(store.findSkippedInputs("order-13", "q-2", 10)).containsExactly(new SkippedInput(0, "b@1")),
                () -> assertThat(store.findSkippedInputs("order-14", "q-1", 10)).containsExactly(new SkippedInput(0, "c@1"))
        );
    }

    @Test
    void delete_keeps_the_records_of_an_instance_created_with_the_same_id_between_the_removal_of_the_instance_and_the_delete_of_its_records() {
        MongoOperations mongoOperations = mongoOperations();
        SpringMongoSagaStateStore<Payment> direct = store(mongoOperations);
        direct.compareAndSave("order-20", releasing("order-20"), 0);
        direct.recordSkippedInput("order-20", "q-1", new SkippedInput(0, "a@1"));
        MongoOperations recreatingAfterTheRemoval = afterTheFirstFindAndRemove(mongoOperations, () -> {
            direct.compareAndSave("order-20", envelope("order-20", SagaStatus.QUARANTINED, 1, failure(null, "q-2"), 1L), 0);
            direct.recordSkippedInput("order-20", "q-2", new SkippedInput(0, "b@1"));
        });

        store(recreatingAfterTheRemoval).delete("order-20");

        assertAll(
                () -> assertThat(direct.find("order-20")).hasValueSatisfying(recreated -> assertThat(recreated.quarantineId()).isEqualTo("q-2")),
                () -> assertThat(direct.findSkippedInputs("order-20", "q-1", 10)).isEmpty(),
                () -> assertThat(direct.findSkippedInputs("order-20", "q-2", 10)).containsExactly(new SkippedInput(0, "b@1"))
        );
    }

    // Runs the hook once, right after the instance was removed and before the caller goes on to the records
    private static MongoOperations afterTheFirstFindAndRemove(MongoOperations delegate, Runnable hook) {
        AtomicBoolean ran = new AtomicBoolean();
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(), new Class<?>[]{MongoOperations.class}, (proxy, method, args) -> {
            Object result;
            try {
                result = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (method.getName().equals("findAndRemove") && ran.compareAndSet(false, true)) {
                hook.run();
            }
            return result;
        });
    }

    @Test
    void the_bulk_delete_removes_the_records_of_one_quarantine_id_only() {
        SpringMongoSagaStateStore<Payment> store = store(mongoOperations());
        store.recordSkippedInput("order-15", "q-1", new SkippedInput(0, "a@1"));
        store.recordSkippedInput("order-15", "q-2", new SkippedInput(0, "b@1"));
        store.recordSkippedInput("order-16", "q-1", new SkippedInput(0, "c@1"));

        store.deleteSkippedInputs("order-15", "q-1");

        assertAll(
                () -> assertThat(store.findSkippedInputs("order-15", "q-1", 10)).isEmpty(),
                () -> assertThat(store.findSkippedInputs("order-15", "q-2", 10)).containsExactly(new SkippedInput(0, "b@1")),
                () -> assertThat(store.findSkippedInputs("order-16", "q-1", 10)).containsExactly(new SkippedInput(0, "c@1"))
        );
    }

    @Test
    void a_store_cannot_be_created_on_a_collection_name_ending_in_the_suffix_of_the_skipped_inputs_collection() {
        MongoOperations mongoOperations = mongoOperations();

        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION + "_skipped_inputs", Payment.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'" + COLLECTION + "_skipped_inputs'")
                .hasMessageContaining("a store on '" + COLLECTION + "'");
    }
}
