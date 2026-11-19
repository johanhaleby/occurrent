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
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

/**
 * Docker-based. A transaction the caller started reads from a snapshot taken when its first operation ran, so an
 * instance inserted after that is absent to it. {@code find} and {@code findWithoutState} must not report such an
 * instance as absent, because the executor skips an input addressed to an absent instance without a write. They throw
 * with the label that tells the caller to run the transaction again, and a transaction started afterwards finds it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreSnapshotReadTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-snapshot-read";
    private static final String EXISTING = "existing-1";

    record Payment(long amount) {
    }

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<Payment> store;
    private TransactionTemplate callersTransaction;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-snapshot-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        template.setWriteConcern(WriteConcern.MAJORITY);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, RetryStrategy.none());
        callersTransaction = new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory()));
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<Payment> active(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private void stored(String sagaId) {
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(sagaId, active(sagaId), 0)).isTrue();
    }

    // The caller's transaction is bound to its thread, so another thread writes outside it
    private void storedOutsideTheTransaction(String sagaId) {
        CompletableFuture.runAsync(() -> stored(sagaId)).join();
    }

    // The first read of the transaction is what takes its snapshot
    private <T> T afterTheSnapshotBegan(String sagaIdWrittenAfterwards, Function<String, T> read) {
        return callersTransaction.execute(status -> {
            assertThat(store.find(EXISTING)).as("the read that starts the snapshot").isPresent();
            storedOutsideTheTransaction(sagaIdWrittenAfterwards);
            return read.apply(sagaIdWrittenAfterwards);
        });
    }

    private static void assertTransientAndNaming(Exception thrown, String sagaId) {
        assertThat(thrown).as("a read of an instance the snapshot misses").isNotNull();
        assertThat(new MongoExceptionTranslator().isTransientFailure(thrown)).as("transient for a caller that retries on the label").isTrue();
        Throwable cause = thrown;
        while (cause != null && !(cause instanceof MongoException)) {
            cause = cause.getCause();
        }
        assertThat(cause).isInstanceOf(MongoException.class);
        assertThat(((MongoException) cause).hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)).isTrue();
        assertThat(thrown.getMessage()).contains(sagaId);
    }

    @Test
    void find_throws_a_transient_transaction_error_when_the_instance_was_stored_after_the_snapshot_began() {
        stored(EXISTING);

        Exception thrown = catchException(() -> afterTheSnapshotBegan("late-1", store::find));

        assertTransientAndNaming(thrown, "late-1");
    }

    @Test
    void findWithoutState_throws_a_transient_transaction_error_when_the_instance_was_stored_after_the_snapshot_began() {
        stored(EXISTING);

        Exception thrown = catchException(() -> afterTheSnapshotBegan("late-2", store::findWithoutState));

        assertTransientAndNaming(thrown, "late-2");
    }

    @Test
    void find_and_findWithoutState_return_empty_inside_a_transaction_for_an_instance_that_exists_nowhere() {
        stored(EXISTING);

        List<Optional<StoredSaga>> found = callersTransaction.execute(status -> List.of(store.find("nowhere"), store.findWithoutState("nowhere")));

        assertThat(found).containsExactly(Optional.empty(), Optional.empty());
    }

    @Test
    void find_and_findWithoutState_find_an_instance_stored_before_the_transaction_began() {
        stored(EXISTING);

        List<Optional<StoredSaga>> found = callersTransaction.execute(status -> List.of(store.find(EXISTING), store.findWithoutState(EXISTING)));

        assertThat(found).allSatisfy(s -> assertThat(s).isPresent());
    }

    @Test
    void a_transaction_started_after_the_throw_finds_the_instance() {
        stored(EXISTING);
        catchException(() -> afterTheSnapshotBegan("late-3", store::find));

        Optional<StoredSaga> retried = callersTransaction.execute(status -> store.find("late-3"));

        assertThat(retried).isPresent();
    }
}
