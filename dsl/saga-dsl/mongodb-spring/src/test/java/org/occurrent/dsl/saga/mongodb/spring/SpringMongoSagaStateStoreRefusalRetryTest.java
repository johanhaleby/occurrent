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

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A MongoDB server that refuses a transaction or journaling refuses it again on every attempt, so the
 * store must throw at once, while a transaction is run again only when MongoDB labels its failure transient. The
 * failures are injected by a {@link MongoTemplate} whose calls throw a {@link MongoCommandException} with the code and
 * message a server answers with, since a {@code failCommand} fail point cannot set the message the store matches. Each
 * test counts the attempts that reached the failing call, with a strategy that allows several.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreRefusalRetryTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-refusal";
    private static final int ATTEMPTS = 5;
    private static final int ILLEGAL_OPERATION = 20;
    private static final int BAD_VALUE = 2;
    private static final int UNKNOWN_ERROR = 8;
    private static final String NO_JOURNALING = "cannot use 'j' option when a host does not have journaling enabled";

    record Payment(long amount) {
    }

    private MongoClient client;
    private String database;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-refusal-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        database = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static RetryStrategy severalFastAttempts() {
        return RetryStrategy.fixed(1).maxAttempts(ATTEMPTS);
    }

    private static MongoCommandException serverAnswer(int code, String message, String... labels) {
        BsonArray errorLabels = new BsonArray();
        for (String label : labels) {
            errorLabels.add(new BsonString(label));
        }
        return new MongoCommandException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(code)).append("errmsg", new BsonString(message)).append("errorLabels", errorLabels), new ServerAddress());
    }

    private StoredSaga anInstance(String sagaId) {
        SpringMongoSagaStateStore<Payment> healthy = new SpringMongoSagaStateStore<>(new MongoTemplate(client, database), COLLECTION, Payment.class);
        SagaEnvelope<Payment> envelope = new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
        assertThat(SagaEnvelopes.of(healthy, null).compareAndSave(sagaId, envelope, 0)).isTrue();
        return healthy.find(sagaId).orElseThrow();
    }

    @Test
    void a_transition_whose_transaction_the_server_refuses_throws_the_replica_set_message_after_one_attempt() {
        StoredSaga instance = anInstance("order-1");
        FailingMongoTemplate template = FailingMongoTemplate.inTransactions(client, database, serverAnswer(ILLEGAL_OPERATION, "Transaction numbers are only allowed on a replica set member or mongos"));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSaveQuarantineEnded(instance, 1, "q-1"));

        assertAll(
                () -> assertThat(thrown).as("a refusal of the transaction").isInstanceOf(IllegalStateException.class).hasMessage(SpringMongoSagaStateStore.REPLICA_SET_REQUIRED_MESSAGE),
                () -> assertThat(template.attempts()).as("attempts of the transaction, which the server refuses again every time").isEqualTo(1)
        );
    }

    @Test
    void a_live_compare_and_save_that_the_server_refuses_for_journaling_throws_the_journaling_message_after_one_attempt() {
        StoredSaga instance = anInstance("order-2");
        FailingMongoTemplate template = FailingMongoTemplate.inDurableWrites(client, database, serverAnswer(BAD_VALUE, NO_JOURNALING));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSave(instance, 1));

        assertAll(
                () -> assertThat(thrown).as("a refusal of j: true").isInstanceOf(IllegalStateException.class).hasMessage(SpringMongoSagaStateStore.JOURNALING_REQUIRED_MESSAGE),
                () -> assertThat(template.attempts()).as("attempts of the write, which the server refuses again every time").isEqualTo(1)
        );
    }

    @Test
    void a_skip_whose_transaction_the_server_refuses_for_journaling_throws_the_journaling_message_after_one_attempt() {
        StoredSaga instance = anInstance("order-5");
        FailingMongoTemplate template = FailingMongoTemplate.inTransactions(client, database, serverAnswer(BAD_VALUE, NO_JOURNALING));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSaveSkipped(instance, 1, "q-1", new SkippedInput(0, "order-5@7")));

        assertAll(
                () -> assertThat(thrown).as("a refusal of j: true").isInstanceOf(IllegalStateException.class).hasMessage(SpringMongoSagaStateStore.JOURNALING_REQUIRED_MESSAGE),
                () -> assertThat(template.attempts()).as("attempts of the transaction, which the server refuses again every time").isEqualTo(1)
        );
    }

    @Test
    void a_live_compare_and_save_that_fails_with_another_bad_value_is_retried_until_the_attempts_are_used() {
        StoredSaga instance = anInstance("order-6");
        FailingMongoTemplate template = FailingMongoTemplate.inDurableWrites(client, database, serverAnswer(BAD_VALUE, "a bad value"));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSave(instance, 1));

        assertAll(
                () -> assertThat(thrown).as("the failure that used the attempts").isInstanceOf(UncategorizedMongoDbException.class),
                () -> assertThat(template.attempts()).as("attempts of a write that failed with the journaling code and another message").isEqualTo(ATTEMPTS)
        );
    }

    @Test
    void a_live_compare_and_save_that_fails_with_the_journaling_message_under_another_code_is_retried_until_the_attempts_are_used() {
        StoredSaga instance = anInstance("order-7");
        FailingMongoTemplate template = FailingMongoTemplate.inDurableWrites(client, database, serverAnswer(UNKNOWN_ERROR, NO_JOURNALING));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSave(instance, 1));

        assertAll(
                () -> assertThat(thrown).as("the failure that used the attempts").isInstanceOf(UncategorizedMongoDbException.class),
                () -> assertThat(template.attempts()).as("attempts of a write that failed with the journaling message and another code").isEqualTo(ATTEMPTS)
        );
    }

    @Test
    void a_transition_that_fails_in_its_transaction_without_the_transient_label_throws_after_one_attempt() {
        StoredSaga instance = anInstance("order-3");
        FailingMongoTemplate template = FailingMongoTemplate.inTransactions(client, database, serverAnswer(UNKNOWN_ERROR, "an error that MongoDB does not call transient"));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSaveQuarantineEnded(instance, 1, "q-1"));

        assertAll(
                () -> assertThat(thrown).as("the failure reaches the caller").isNotNull(),
                () -> assertThat(template.attempts()).as("attempts of a transaction that failed without the TransientTransactionError label").isEqualTo(1)
        );
    }

    @Test
    void a_transition_that_fails_in_its_transaction_with_the_transient_label_is_run_again_until_the_attempts_are_used() {
        StoredSaga instance = anInstance("order-4");
        FailingMongoTemplate template = FailingMongoTemplate.inTransactions(client, database, serverAnswer(UNKNOWN_ERROR, "an error that MongoDB calls transient", "TransientTransactionError"));
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, severalFastAttempts());

        Throwable thrown = catchThrowable(() -> store.compareAndSaveQuarantineEnded(instance, 1, "q-1"));

        assertAll(
                () -> assertThat(thrown).as("the failure that used the attempts reaches the caller").isNotNull(),
                () -> assertThat(template.attempts()).as("attempts of a transaction that failed with the TransientTransactionError label").isEqualTo(ATTEMPTS)
        );
    }

    /**
     * A real {@link MongoTemplate} that throws {@code failure} from the one place a test names, and counts the calls that
     * reached it: the database a store-owned transaction runs in, or the collection a durable write goes through.
     */
    private static final class FailingMongoTemplate extends MongoTemplate {
        private final AtomicInteger attempts = new AtomicInteger();
        private final RuntimeException failure;
        private final boolean failsDurableWrites;
        private final MongoDatabaseFactory databaseFactory;

        private FailingMongoTemplate(MongoClient client, String database, RuntimeException failure, boolean failsDurableWrites) {
            super(client, database);
            this.failure = failure;
            this.failsDurableWrites = failsDurableWrites;
            this.databaseFactory = failsDurableWrites ? super.getMongoDatabaseFactory() : new FailingDatabaseFactory(super.getMongoDatabaseFactory());
        }

        static FailingMongoTemplate inTransactions(MongoClient client, String database, RuntimeException failure) {
            return new FailingMongoTemplate(client, database, failure, false);
        }

        static FailingMongoTemplate inDurableWrites(MongoClient client, String database, RuntimeException failure) {
            return new FailingMongoTemplate(client, database, failure, true);
        }

        int attempts() {
            return attempts.get();
        }

        @Override
        public MongoDatabaseFactory getMongoDatabaseFactory() {
            return databaseFactory;
        }

        @Override
        public <T> T execute(String collectionName, CollectionCallback<T> callback) {
            if (failsDurableWrites) {
                attempts.incrementAndGet();
                throw failure;
            }
            return super.execute(collectionName, callback);
        }

        private final class FailingDatabaseFactory implements MongoDatabaseFactory {
            private final MongoDatabaseFactory delegate;

            private FailingDatabaseFactory(MongoDatabaseFactory delegate) {
                this.delegate = delegate;
            }

            @Override
            public MongoDatabase getMongoDatabase() {
                attempts.incrementAndGet();
                throw failure;
            }

            @Override
            public MongoDatabase getMongoDatabase(String dbName) {
                attempts.incrementAndGet();
                throw failure;
            }

            @Override
            public PersistenceExceptionTranslator getExceptionTranslator() {
                return delegate.getExceptionTranslator();
            }

            @Override
            public ClientSession getSession(ClientSessionOptions options) {
                return delegate.getSession(options);
            }

            @Override
            public MongoDatabaseFactory withSession(ClientSession session) {
                return new FailingDatabaseFactory(delegate.withSession(session));
            }
        }
    }
}
