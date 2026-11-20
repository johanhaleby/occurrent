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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;
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
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set that accepts MongoDB's {@code failCommand} fail point. Every transition of the skipped
 * inputs SPI writes the instance and its records in one transaction, so a failure of the second write must leave both
 * as they were, a lost compare-and-set must write nothing, and the transaction must carry the durable write concern, be
 * retried when the server says it may be, and join one the caller started instead of retrying inside it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreTransitionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-transition";
    private static final String QUARANTINE_ID = "q-1";
    private static final int UNKNOWN_ERROR = 8;
    private static final int WRITE_CONFLICT = 112;
    private static final int SHUTDOWN_IN_PROGRESS = 91;
    private static final String TRANSIENT = "TransientTransactionError";

    record Payment(long amount) {
    }

    record Sent(String command, String collection, boolean inTransaction, BsonDocument writeConcern) {
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private String database;
    private MongoTemplate template;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-tx-" + UUID.randomUUID().toString().substring(0, 8)));
        database = requireNonNull(connectionString.getDatabase());
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.W1)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonDocument command = event.getCommand();
                        BsonValue target = command.get(event.getCommandName());
                        sent.add(new Sent(event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : "", command.containsKey("txnNumber"),
                                BsonDocument.parse(command.getDocument("writeConcern", new BsonDocument()).toJson())));
                    }
                }).build());
        template = new MongoTemplate(client, database);
        template.setWriteConcern(WriteConcern.W1);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private SpringMongoSagaStateStore<Payment> store(RetryStrategy retryStrategy) {
        return new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, retryStrategy);
    }

    private static SagaEnvelope<Payment> releasing(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.QUARANTINED, 1, List.of(), Map.of("order-1", 6L), null, NOW.minusSeconds(600), NOW, null, null, true,
                new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID), Map.of(), 4L);
    }

    private static SagaEnvelope<Payment> atVersion(SagaEnvelope<Payment> e, long version) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), version, e.timers(), e.streamWatermarks(), null, e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaEnvelope<Payment> next(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        return atVersion(SagaEnvelopes.of(store, null).find(sagaId).orElseThrow(), versionOf(store, sagaId) + 1);
    }

    private static long versionOf(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        return SagaEnvelopes.of(store, null).find(sagaId).orElseThrow().version();
    }

    private static boolean skip(SpringMongoSagaStateStore<Payment> store, String sagaId, SkippedInput input) {
        return SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, next(store, sagaId), versionOf(store, sagaId), QUARANTINE_ID, input);
    }

    private static boolean startRelease(SpringMongoSagaStateStore<Payment> store, String sagaId, SkippedInput head) {
        return SagaEnvelopes.of(store, null).compareAndSaveReleaseStarted(store, next(store, sagaId), versionOf(store, sagaId), QUARANTINE_ID, head);
    }

    private static boolean replayed(SpringMongoSagaStateStore<Payment> store, String sagaId, SkippedInput input) {
        return SagaEnvelopes.of(store, null).compareAndSaveReplayed(store, next(store, sagaId), versionOf(store, sagaId), QUARANTINE_ID, input);
    }

    private static boolean ended(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        return SagaEnvelopes.of(store, null).compareAndSaveQuarantineEnded(store, next(store, sagaId), versionOf(store, sagaId), QUARANTINE_ID);
    }

    private static SagaEnvelope<Payment> completing(SagaEnvelope<Payment> e) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), SagaStatus.COMPLETED, e.version(), List.of(), e.streamWatermarks(), null, e.createdAt(), NOW, NOW, null, true, null, Map.of(), null);
    }

    // The write of a release that completes the instance, which deletes the records whatever is left
    private static boolean endedByCompletion(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        return SagaEnvelopes.of(store, null).compareAndSaveQuarantineEnded(store, completing(next(store, sagaId)), versionOf(store, sagaId), QUARANTINE_ID);
    }

    // An instance at version 2 with the record of a@1, so that a stale version is a version that was once current
    private static void aQuarantinedInstanceWithTheRecordOfA(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(sagaId, releasing(sagaId), 0)).isTrue();
        assertThat(skip(store, sagaId, new SkippedInput(1, "a@1"))).isTrue();
    }

    private static State stateOf(SpringMongoSagaStateStore<Payment> store, String sagaId) {
        return new State(SagaEnvelopes.of(store, null).find(sagaId).map(SagaEnvelope::version).orElse(-1L), store.findSkippedInputs(sagaId, QUARANTINE_ID, 100));
    }

    record State(long version, List<SkippedInput> records) {
    }

    private long countOf(String command, String collection) {
        return sent.stream().filter(s -> s.command().equals(command) && s.collection().equals(collection)).count();
    }

    @Nested
    class AFailureOfTheSecondWrite {

        private void leavesTheInstanceAndItsRecordsAsTheyWere(String sagaId, SpringMongoSagaStateStore<Payment> store, FailPoint failure, BooleanSupplier transition) {
            State before = stateOf(store, sagaId);

            Throwable thrown;
            try (failure) {
                thrown = catchThrowable(transition::getAsBoolean);
            }

            assertAll(
                    () -> assertThat(thrown).as("the failure of the second write must reach the caller").isNotNull(),
                    () -> assertThat(stateOf(store, sagaId)).as("neither the instance nor its records may change").isEqualTo(before),
                    () -> assertThat(transition.getAsBoolean()).as("the same transition applies once the failure is gone, so it was the failure that stopped it").isTrue()
            );
        }

        @Test
        void of_a_skip_leaves_the_instance_and_its_records_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");

            leavesTheInstanceAndItsRecordsAsTheyWere("order-1", store, FailPoint.failingAfter(client, "update", 1, UNKNOWN_ERROR), () -> skip(store, "order-1", new SkippedInput(2, "b@2")));
        }

        @Test
        void of_the_start_of_a_release_leaves_the_instance_and_its_records_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");

            leavesTheInstanceAndItsRecordsAsTheyWere("order-2", store, FailPoint.failingAfter(client, "update", 1, UNKNOWN_ERROR), () -> startRelease(store, "order-2", new SkippedInput(0, "a@1")));
        }

        @Test
        void of_a_replay_leaves_the_instance_and_its_records_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-3");

            leavesTheInstanceAndItsRecordsAsTheyWere("order-3", store, FailPoint.failing(client, "delete", 1, UNKNOWN_ERROR), () -> replayed(store, "order-3", new SkippedInput(1, "a@1")));
        }

        @Test
        void of_the_end_of_a_quarantine_leaves_the_instance_and_its_records_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-4");

            leavesTheInstanceAndItsRecordsAsTheyWere("order-4", store, FailPoint.failing(client, "delete", 1, UNKNOWN_ERROR), () -> endedByCompletion(store, "order-4"));
        }

        @Test
        void of_a_delete_leaves_the_instance_and_its_records_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-5");

            // The first delete command is the instance delete that needs no transaction and matches nothing, the second removes the records
            leavesTheInstanceAndItsRecordsAsTheyWere("order-5", store, FailPoint.failingAfter(client, "delete", 1, UNKNOWN_ERROR), () -> {
                store.delete("order-5");
                return true;
            });
        }
    }

    @Nested
    class ALostVersionCompareAndSet {

        // The instance is at version 2, so version 1 was current once and no write at it matches
        private void writesNothing(String sagaId, SpringMongoSagaStateStore<Payment> store, java.util.function.Function<SagaEnvelope<Payment>, Boolean> transitionAtVersion1) {
            State before = stateOf(store, sagaId);

            boolean applied = transitionAtVersion1.apply(next(store, sagaId));

            assertAll(
                    () -> assertThat(applied).isFalse(),
                    () -> assertThat(stateOf(store, sagaId)).as("a lost compare-and-set must write no record and delete none").isEqualTo(before)
            );
        }

        @Test
        void of_a_skip_writes_no_record() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");

            writesNothing("order-1", store, e -> SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, e, 1, QUARANTINE_ID, new SkippedInput(2, "b@2")));
        }

        @Test
        void of_the_start_of_a_release_writes_no_record() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");

            writesNothing("order-2", store, e -> SagaEnvelopes.of(store, null).compareAndSaveReleaseStarted(store, e, 1, QUARANTINE_ID, new SkippedInput(0, "b@2")));
        }

        @Test
        void of_a_replay_deletes_no_record() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-3");

            writesNothing("order-3", store, e -> SagaEnvelopes.of(store, null).compareAndSaveReplayed(store, e, 1, QUARANTINE_ID, new SkippedInput(1, "a@1")));
        }

        @Test
        void of_the_end_of_a_quarantine_deletes_no_record() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-4");

            writesNothing("order-4", store, e -> SagaEnvelopes.of(store, null).compareAndSaveQuarantineEnded(store, completing(e), 1, QUARANTINE_ID));
        }
    }

    @Nested
    class AQuarantineEndedByAnInstanceThatIsNotCompleted {

        @Test
        void is_refused_while_a_record_is_under_the_quarantine_id_and_leaves_the_instance_and_the_record_as_they_were() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");
            StoredSaga instanceBefore = store.find("order-1").orElseThrow();
            State before = stateOf(store, "order-1");

            boolean ended = ended(store, "order-1");

            assertAll(
                    () -> assertThat(ended).as("an instance that is not completed ends its quarantine only with no record left").isFalse(),
                    () -> assertThat(store.find("order-1")).as("the instance, version and fields").contains(instanceBefore),
                    () -> assertThat(stateOf(store, "order-1")).isEqualTo(before),
                    () -> assertThat(before.records()).containsExactly(new SkippedInput(1, "a@1"))
            );
        }

        @Test
        void succeeds_once_the_record_is_gone() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");
            assertThat(ended(store, "order-2")).as("refused while the record is there").isFalse();
            assertThat(replayed(store, "order-2", new SkippedInput(1, "a@1"))).isTrue();
            long version = versionOf(store, "order-2");

            boolean ended = ended(store, "order-2");

            assertAll(
                    () -> assertThat(ended).isTrue(),
                    () -> assertThat(versionOf(store, "order-2")).isEqualTo(version + 1)
            );
        }

        @Test
        void that_completes_the_instance_deletes_every_record_under_the_quarantine_id() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-3");
            assertThat(skip(store, "order-3", new SkippedInput(2, "b@2"))).isTrue();

            boolean ended = endedByCompletion(store, "order-3");

            assertAll(
                    () -> assertThat(ended).isTrue(),
                    () -> assertThat(SagaEnvelopes.of(store, null).find("order-3").orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(store.findSkippedInputs("order-3", QUARANTINE_ID, 100)).isEmpty()
            );
        }
    }

    @Nested
    class NoRecordIsLeftBehind {

        @Test
        void by_a_replay_of_the_last_record() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");

            boolean applied = replayed(store, "order-1", new SkippedInput(1, "a@1"));

            assertAll(
                    () -> assertThat(applied).isTrue(),
                    () -> assertThat(store.findSkippedInputs("order-1", QUARANTINE_ID, 100)).isEmpty()
            );
        }

        @Test
        void by_the_write_that_ends_the_quarantine() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");
            assertThat(skip(store, "order-2", new SkippedInput(2, "b@2"))).isTrue();

            boolean applied = endedByCompletion(store, "order-2");

            assertAll(
                    () -> assertThat(applied).isTrue(),
                    () -> assertThat(store.findSkippedInputs("order-2", QUARANTINE_ID, 100)).isEmpty()
            );
        }

        @Test
        void by_a_delete() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-3");

            store.delete("order-3");

            assertAll(
                    () -> assertThat(SagaEnvelopes.of(store, null).find("order-3")).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("order-3", QUARANTINE_ID, 100)).isEmpty()
            );
        }
    }

    @Nested
    class TheWriteConcernOnTheWire {

        private void assertDurable(Sent write) {
            assertAll(
                    () -> assertThat(write.writeConcern().get("w")).as("w of %s on %s", write.command(), write.collection()).isEqualTo(new org.bson.BsonString("majority")),
                    () -> assertThat(write.writeConcern().get("j")).as("j of %s on %s", write.command(), write.collection()).isEqualTo(org.bson.BsonBoolean.TRUE)
            );
        }

        @Test
        void is_majority_and_journaled_on_the_commit_of_every_transition_and_of_a_delete_whatever_the_write_concern_of_the_template() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");
            List<Runnable> writes = List.of(
                    () -> skip(store, "order-1", new SkippedInput(2, "b@2")),
                    () -> startRelease(store, "order-1", new SkippedInput(0, "a@1")),
                    () -> replayed(store, "order-1", new SkippedInput(0, "a@1")),
                    () -> endedByCompletion(store, "order-1"));

            for (int i = 0; i < writes.size(); i++) {
                sent.clear();
                writes.get(i).run();

                List<Sent> commits = sent.stream().filter(s -> s.command().equals("commitTransaction")).toList();
                assertThat(commits).as("commitTransaction commands of write %s", i).hasSize(1);
                assertDurable(commits.get(0));
            }
            aQuarantinedInstanceWithTheRecordOfA(store, "order-9");
            sent.clear();
            store.delete("order-9");
            List<Sent> commitsOfTheDelete = sent.stream().filter(s -> s.command().equals("commitTransaction")).toList();
            assertThat(commitsOfTheDelete).as("commitTransaction commands of the delete of a quarantined instance").hasSize(1);
            assertDurable(commitsOfTheDelete.get(0));
        }

        @Test
        void is_majority_and_journaled_on_the_single_document_compare_and_sets_whatever_the_write_concern_of_the_template() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            sent.clear();

            SagaEnvelopes.of(store, null).compareAndSave("order-2", releasing("order-2"), 0);
            SagaEnvelopes.of(store, null).compareAndSave("order-2", next(store, "order-2"), 1);
            SagaEnvelopes.of(store, null).compareAndSaveWithoutState("order-2", next(store, "order-2"), 2);

            List<Sent> writes = sent.stream().filter(s -> s.collection().equals(COLLECTION) && List.of("insert", "update").contains(s.command())).toList();
            assertThat(writes).as("writes to the instance collection").extracting(Sent::command).containsExactly("insert", "update", "update");
            writes.forEach(this::assertDurable);
        }
    }

    @Nested
    class ACallersTransaction {

        private TransactionTemplate callersTransaction() {
            return new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory(), com.mongodb.TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build()));
        }

        private State seenFromOutside(String sagaId) {
            org.bson.Document stored = client.getDatabase(database).getCollection(COLLECTION).find(new org.bson.Document("_id", sagaId)).first();
            List<SkippedInput> records = new java.util.ArrayList<>();
            client.getDatabase(database).getCollection(COLLECTION + "_skipped_inputs").find(new org.bson.Document("sagaId", sagaId)).forEach(d -> records.add(new SkippedInput(((Number) d.get("seq")).longValue(), d.getString("key"))));
            return new State(stored == null ? -1L : ((Number) stored.get("version")).longValue(), records);
        }

        @Test
        void is_joined_and_rolls_the_instance_and_its_record_back_together() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");
            State before = stateOf(store, "order-1");

            callersTransaction().executeWithoutResult(status -> {
                assertThat(skip(store, "order-1", new SkippedInput(2, "b@2"))).isTrue();
                status.setRollbackOnly();
            });

            assertThat(stateOf(store, "order-1")).as("the caller's rollback must undo the instance write and the record together").isEqualTo(before);
        }

        @Test
        void is_joined_and_commits_the_instance_and_its_record_together() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");
            State before = seenFromOutside("order-2");
            State[] outsideBeforeCommit = new State[1];

            callersTransaction().executeWithoutResult(status -> {
                assertThat(skip(store, "order-2", new SkippedInput(2, "b@2"))).isTrue();
                outsideBeforeCommit[0] = seenFromOutside("order-2");
            });

            assertAll(
                    () -> assertThat(outsideBeforeCommit[0]).as("nothing is visible outside before the caller commits").isEqualTo(before),
                    () -> assertThat(stateOf(store, "order-2")).isEqualTo(new State(before.version() + 1, List.of(new SkippedInput(1, "a@1"), new SkippedInput(2, "b@2"))))
            );
        }

        @Test
        void that_a_transition_in_it_loses_its_compare_and_set_leaves_it_committable_with_the_callers_other_write() {
            SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
            aQuarantinedInstanceWithTheRecordOfA(store, "order-4");
            State before = stateOf(store, "order-4");
            SagaEnvelope<Payment> lost = next(store, "order-4");
            boolean[] applied = {true};

            // Version 1 is stale, the instance is at version 2
            Throwable thrown = catchThrowable(() -> callersTransaction().executeWithoutResult(status -> {
                template.insert(new org.bson.Document("_id", "callers-write"), "callers-writes");
                applied[0] = SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, lost, 1, QUARANTINE_ID, new SkippedInput(2, "b@2"));
            }));

            assertAll(
                    () -> assertThat(thrown).as("the caller's commit, which an UnexpectedRollbackException says the lost compare-and-set marked for rollback").isNull(),
                    () -> assertThat(applied[0]).as("the compare-and-set at the stale version").isFalse(),
                    () -> assertThat(client.getDatabase(database).getCollection("callers-writes").find(new org.bson.Document("_id", "callers-write")).first()).as("the caller's other write after its commit").isNotNull(),
                    () -> assertThat(stateOf(store, "order-4")).as("the lost transition wrote nothing").isEqualTo(before)
            );
        }

        @Test
        void is_not_retried_by_the_store_when_the_server_calls_a_failure_transient() {
            SpringMongoSagaStateStore<Payment> store = store(SpringMongoSagaStateStore.defaultRetryStrategy().backoff(org.occurrent.retry.Backoff.fixed(1)));
            aQuarantinedInstanceWithTheRecordOfA(store, "order-3");
            State before = stateOf(store, "order-3");
            sent.clear();

            Throwable thrown;
            try (FailPoint ignored = FailPoint.failing(client, "update", 1, WRITE_CONFLICT, TRANSIENT)) {
                thrown = catchThrowable(() -> callersTransaction().executeWithoutResult(status -> skip(store, "order-3", new SkippedInput(2, "b@2"))));
            }

            assertAll(
                    () -> assertThat(thrown).as("the failure reaches the caller, who owns the transaction").isNotNull(),
                    () -> assertThat(countOf("update", COLLECTION)).as("update commands sent to the instance collection, one per attempt").isEqualTo(1),
                    () -> assertThat(stateOf(store, "order-3")).isEqualTo(before)
            );
        }
    }

    @Nested
    class AStoreOwnedTransaction {

        @Test
        void is_retried_when_the_server_calls_a_failure_transient_and_the_transition_is_applied_once() {
            SpringMongoSagaStateStore<Payment> store = store(SpringMongoSagaStateStore.defaultRetryStrategy().backoff(org.occurrent.retry.Backoff.fixed(1)));
            aQuarantinedInstanceWithTheRecordOfA(store, "order-1");
            State before = stateOf(store, "order-1");
            sent.clear();

            boolean[] applied = new boolean[1];
            Throwable thrown;
            try (FailPoint ignored = FailPoint.failing(client, "update", 1, WRITE_CONFLICT, TRANSIENT)) {
                thrown = catchThrowable(() -> applied[0] = skip(store, "order-1", new SkippedInput(2, "b@2")));
            }

            assertAll(
                    () -> assertThat(thrown).as("a transient failure of a transaction the store owns must not reach the caller").isNull(),
                    () -> assertThat(countOf("update", COLLECTION)).as("update commands sent to the instance collection, one per attempt").isEqualTo(2),
                    () -> assertThat(applied[0]).isTrue(),
                    () -> assertThat(stateOf(store, "order-1")).isEqualTo(new State(before.version() + 1, List.of(new SkippedInput(1, "a@1"), new SkippedInput(2, "b@2"))))
            );
        }

        @Test
        void commits_again_when_the_result_of_the_commit_is_unknown_and_the_transition_is_applied_once() {
            SpringMongoSagaStateStore<Payment> store = store(SpringMongoSagaStateStore.defaultRetryStrategy().backoff(org.occurrent.retry.Backoff.fixed(1)));
            aQuarantinedInstanceWithTheRecordOfA(store, "order-2");
            State before = stateOf(store, "order-2");
            sent.clear();

            boolean[] applied = new boolean[1];
            Throwable thrown;
            try (FailPoint ignored = FailPoint.answeringWithAWriteConcernError(client, "commitTransaction", 2, SHUTDOWN_IN_PROGRESS)) {
                thrown = catchThrowable(() -> applied[0] = skip(store, "order-2", new SkippedInput(2, "b@2")));
            }

            assertAll(
                    () -> assertThat(sent.stream().filter(s -> s.command().equals("commitTransaction")).count()).as("commitTransaction commands, the first of which took effect and said it might not have, and the driver sends one again itself").isGreaterThanOrEqualTo(3),
                    () -> assertThat(thrown).as("a commit whose result is unknown must not reach the caller").isNull(),
                    () -> assertThat(applied[0]).as("the transition that took effect reports that it did").isTrue(),
                    () -> assertThat(stateOf(store, "order-2")).isEqualTo(new State(before.version() + 1, List.of(new SkippedInput(1, "a@1"), new SkippedInput(2, "b@2"))))
            );
        }
    }

    @Test
    void a_read_at_majority_sees_a_durable_write_as_soon_as_the_write_returns() {
        SpringMongoSagaStateStore<Payment> store = store(RetryStrategy.none());
        assertThat(SagaEnvelopes.of(store, null).compareAndSave("order-1", releasing("order-1"), 0)).isTrue();

        for (int i = 0; i < 200; i++) {
            long version = versionOf(store, "order-1");
            assertThat(SagaEnvelopes.of(store, null).compareAndSave("order-1", next(store, "order-1"), version)).isTrue();
            assertThat(versionOf(store, "order-1")).as("version read at majority right after the write of version %s returned", version + 1).isEqualTo(version + 1);
        }
    }
}
