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
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonValue;
import org.jspecify.annotations.Nullable;
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
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set that accepts MongoDB's {@code failCommand} fail point. The store works against the
 * MongoDB transaction a caller has bound to its {@code MongoDatabaseFactory}, or against none. A transaction that is
 * bound while synchronization is off is joined by every operation, so a failure marks it rollback-only. A transaction on a session the template was bound to with
 * {@code withSession} is joined by a transition, so aborting it undoes the transition.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreTransactionBindingTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-binding";
    private static final String QUARANTINE_ID = "q-1";
    private static final int UNKNOWN_ERROR = 8;

    record Payment(long amount) {
    }

    record Sent(String command, String collection, @Nullable BsonValue lsid, @Nullable Boolean autocommit) {
    }

    record State(long version, List<SkippedInput> records) {
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<Payment> store;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-binding-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        var target = event.getCommand().get(event.getCommandName());
                        sent.add(new Sent(event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : "", event.getCommand().get("lsid"),
                                event.getCommand().containsKey("autocommit") ? event.getCommand().getBoolean("autocommit").getValue() : null));
                    }
                }).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        template.setWriteConcern(WriteConcern.MAJORITY);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<Payment> quarantined(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.QUARANTINED, 1, List.of(), Map.of("order-1", 6L), null, NOW.minusSeconds(600), NOW, null, null, true,
                new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID), Map.of(), 4L);
    }

    private static SagaEnvelope<Payment> next(SagaEnvelope<Payment> e) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), e.version() + 1, e.timers(), e.streamWatermarks(), null, e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaEnvelope<Payment> completing(SagaEnvelope<Payment> e) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), SagaStatus.COMPLETED, e.version(), List.of(), e.streamWatermarks(), null, e.createdAt(), NOW, NOW, null, true, null, Map.of(), null);
    }

    private SagaEnvelope<Payment> found(String sagaId) {
        return SagaEnvelopes.of(store, null).find(sagaId).orElseThrow();
    }

    private State stateOf(String sagaId) {
        return new State(found(sagaId).version(), store.findSkippedInputs(sagaId, QUARANTINE_ID, 100));
    }

    private void aQuarantinedInstance(String sagaId) {
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(sagaId, quarantined(sagaId), 0)).isTrue();
    }

    private void aQuarantinedInstanceWithTheRecordOfA(String sagaId) {
        aQuarantinedInstance(sagaId);
        assertThat(SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, next(found(sagaId)), 1, QUARANTINE_ID, new SkippedInput(1, "a@1"))).isTrue();
    }

    @Nested
    class AMongoDbTransactionBoundWithoutSynchronization {

        private TransactionTemplate withoutSynchronization() {
            MongoTransactionManager manager = new MongoTransactionManager(template.getMongoDatabaseFactory());
            manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
            return new TransactionTemplate(manager);
        }

        // The caller catches what the store throws and commits, so only the rollback-only mark keeps the first write out
        private void rollsTheCallersTransactionBackWithTheTransition(String sagaId, FailPoint failureOfTheSecondWrite, Callable<?> write) {
            State before = stateOf(sagaId);
            StoredSaga instanceBefore = store.find(sagaId).orElseThrow();

            Throwable[] thrownByTheStore = new Throwable[1];
            Throwable thrownByTheCommit;
            try (failureOfTheSecondWrite) {
                thrownByTheCommit = catchThrowable(() -> withoutSynchronization().execute(status -> {
                    thrownByTheStore[0] = catchThrowable(write::call);
                    return null;
                }));
            }

            assertAll(
                    () -> assertThat(thrownByTheStore[0]).as("what the store threw inside the transaction").isNotNull(),
                    () -> assertThat(thrownByTheCommit).as("what the commit of the transaction threw").isInstanceOf(UnexpectedRollbackException.class),
                    () -> assertThat(stateOf(sagaId)).as("version and skipped inputs").isEqualTo(before),
                    () -> assertThat(store.find(sagaId)).as("the instance document").contains(instanceBefore)
            );
        }

        @Test
        void compareAndSaveSkipped_that_fails_on_its_second_write_leaves_nothing_of_the_transition_and_the_commit_throws() {
            aQuarantinedInstanceWithTheRecordOfA("order-1");
            SagaEnvelope<Payment> e = next(found("order-1"));

            rollsTheCallersTransactionBackWithTheTransition("order-1", FailPoint.failingAfter(client, "update", 1, UNKNOWN_ERROR),
                    () -> SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, e, 2, QUARANTINE_ID, new SkippedInput(2, "b@2")));
        }

        @Test
        void compareAndSaveReleaseStarted_that_fails_on_its_second_write_leaves_nothing_of_the_transition_and_the_commit_throws() {
            aQuarantinedInstanceWithTheRecordOfA("order-2");
            SagaEnvelope<Payment> e = next(found("order-2"));

            rollsTheCallersTransactionBackWithTheTransition("order-2", FailPoint.failingAfter(client, "update", 1, UNKNOWN_ERROR),
                    () -> SagaEnvelopes.of(store, null).compareAndSaveReleaseStarted(store, e, 2, QUARANTINE_ID, new SkippedInput(0, "a@1")));
        }

        @Test
        void compareAndSaveReplayed_that_fails_on_its_second_write_leaves_nothing_of_the_transition_and_the_commit_throws() {
            aQuarantinedInstanceWithTheRecordOfA("order-3");
            SagaEnvelope<Payment> e = next(found("order-3"));

            rollsTheCallersTransactionBackWithTheTransition("order-3", FailPoint.failing(client, "delete", 1, UNKNOWN_ERROR),
                    () -> SagaEnvelopes.of(store, null).compareAndSaveReplayed(store, e, 2, QUARANTINE_ID, new SkippedInput(1, "a@1")));
        }

        @Test
        void compareAndSaveQuarantineEnded_that_fails_on_its_second_write_leaves_nothing_of_the_transition_and_the_commit_throws() {
            aQuarantinedInstanceWithTheRecordOfA("order-4");
            SagaEnvelope<Payment> e = completing(next(found("order-4")));

            rollsTheCallersTransactionBackWithTheTransition("order-4", FailPoint.failing(client, "delete", 1, UNKNOWN_ERROR),
                    () -> SagaEnvelopes.of(store, null).compareAndSaveQuarantineEnded(store, e, 2, QUARANTINE_ID));
        }

        @Test
        void delete_of_a_quarantined_instance_that_fails_on_its_second_write_leaves_nothing_of_the_delete_and_the_commit_throws() {
            aQuarantinedInstanceWithTheRecordOfA("order-5");

            rollsTheCallersTransactionBackWithTheTransition("order-5", FailPoint.failingAfter(client, "delete", 1, UNKNOWN_ERROR), () -> {
                store.delete("order-5");
                return null;
            });
        }

        private void readsAnAbsentInstanceInTheCallersSession(Function<String, Optional<StoredSaga>> read) {
            sent.clear();

            Optional<StoredSaga> found = withoutSynchronization().execute(status -> read.apply("nowhere"));

            List<Sent> placeholderInserts = sent.stream().filter(s -> s.command().equals("insert") && s.collection().equals(COLLECTION)).toList();
            BsonValue callersSession = sent.stream().filter(s -> s.command().equals("commitTransaction")).map(Sent::lsid).findFirst().orElse(null);
            assertAll(
                    () -> assertThat(found).isEmpty(),
                    () -> assertThat(callersSession).as("session of the caller's transaction").isNotNull(),
                () -> assertThat(placeholderInserts).as("placeholder inserts").isNotEmpty(),
                    () -> assertThat(placeholderInserts).as("placeholder inserts in the caller's transaction").allSatisfy(s -> {
                        assertThat(s.autocommit()).isFalse();
                        assertThat(s.lsid()).isEqualTo(callersSession);
                    }),
                    () -> assertThat(template.getCollection(COLLECTION).countDocuments(com.mongodb.client.model.Filters.eq("_id", "nowhere"))).as("documents left by the read").isZero()
            );
        }

        @Test
        void find_of_an_absent_instance_returns_empty_and_its_placeholder_insert_runs_in_the_callers_session() {
            aQuarantinedInstance("existing-1");

            readsAnAbsentInstanceInTheCallersSession(store::find);
        }

        @Test
        void findWithoutState_of_an_absent_instance_returns_empty_and_its_placeholder_insert_runs_in_the_callers_session() {
            aQuarantinedInstance("existing-2");

            readsAnAbsentInstanceInTheCallersSession(store::findWithoutState);
        }

        @Test
        @SuppressWarnings("unchecked")
        void find_returns_an_instance_the_same_transaction_stored() {
            Optional<StoredSaga>[] outsideFound = new Optional[1];
            Optional<StoredSaga> found = withoutSynchronization().execute(status -> {
                assertThat(SagaEnvelopes.of(store, null).compareAndSave("new-1", quarantined("new-1"), 0)).isTrue();
                outsideFound[0] = CompletableFuture.supplyAsync(() -> store.find("new-1")).join();
                return store.find("new-1");
            });

            assertAll(
                    () -> assertThat(found).as("read inside the transaction").isPresent(),
                    () -> assertThat(outsideFound[0]).as("read by another thread before the commit").isEmpty()
            );
        }

        @Test
        void find_returns_empty_for_an_instance_the_same_transaction_deleted() {
            aQuarantinedInstance("old-1");

            Optional<StoredSaga> found = withoutSynchronization().execute(status -> {
                store.delete("old-1");
                return store.find("old-1");
            });

            assertThat(found).isEmpty();
        }

        @Test
        void a_transaction_that_rolls_back_undoes_both_the_store_and_the_delete() {
            aQuarantinedInstance("old-2");

            withoutSynchronization().executeWithoutResult(status -> {
                assertThat(SagaEnvelopes.of(store, null).compareAndSave("new-2", quarantined("new-2"), 0)).isTrue();
                store.delete("old-2");
                status.setRollbackOnly();
            });

            assertAll(
                    () -> assertThat(store.find("new-2")).as("the instance the transaction stored").isEmpty(),
                    () -> assertThat(store.find("old-2")).as("the instance the transaction deleted").isPresent()
            );
        }

        @Test
        void find_throws_a_transient_transaction_error_when_the_instance_was_stored_by_another_thread_after_the_snapshot_began() {
            aQuarantinedInstance("existing-3");

            Throwable thrown = catchThrowable(() -> withoutSynchronization().executeWithoutResult(status -> {
                assertThat(store.find("existing-3")).as("the read that starts the snapshot").isPresent();
                CompletableFuture.runAsync(() -> aQuarantinedInstance("late-3")).join();
                store.find("late-3");
            }));

            Throwable found = thrown;
            while (found != null && !(found instanceof MongoException)) {
                found = found.getCause();
            }
            Throwable cause = found;
            assertAll(
                    () -> assertThat(thrown).as("a read of an instance the snapshot misses").isNotNull(),
                    () -> assertThat(cause).isInstanceOf(MongoException.class),
                    () -> assertThat(((MongoException) cause).hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)).isTrue()
            );
        }
    }

    @Nested
    class ATransactionOnTheSessionOfAWithSessionTemplate {

        @Test
        void is_joined_by_a_transition_so_aborting_it_leaves_the_instance_and_its_records_as_they_were() {
            aQuarantinedInstance("order-1");
            SagaEnvelope<Payment> e = next(found("order-1"));
            State before = stateOf("order-1");

            boolean applied;
            try (ClientSession session = client.startSession()) {
                SpringMongoSagaStateStore<Payment> sessionBound = new SpringMongoSagaStateStore<>(template.withSession(session), COLLECTION, Payment.class, null, RetryStrategy.none());
                session.startTransaction();
                applied = SagaEnvelopes.of(sessionBound, null).compareAndSaveSkipped(sessionBound, e, 1, QUARANTINE_ID, new SkippedInput(1, "a@1"));
                session.abortTransaction();
            }

            assertAll(
                    () -> assertThat(applied).as("the transition inside the transaction").isTrue(),
                    () -> assertThat(stateOf("order-1")).as("version and skipped inputs after the abort").isEqualTo(before),
                    () -> assertThat(before.records()).isEmpty()
            );
        }
    }
}
