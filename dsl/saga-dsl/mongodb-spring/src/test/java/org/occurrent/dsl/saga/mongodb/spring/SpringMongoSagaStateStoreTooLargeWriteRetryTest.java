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
import com.mongodb.MongoServerException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.Backoff;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A write MongoDB refuses as larger than 16 MiB is as large on every attempt, so the store hands it to the
 * caller after one attempt, whatever its retry strategy retries. The attempts are counted as the calls of the
 * {@link FlakyMongoTemplate} that reach {@code execute}, which every write goes through, and a control shows that a
 * transient failure of the same write is counted twice, so the count tells a retry from none. The driver refuses to
 * insert a document over the limit before it sends it, and the server refuses an update whose statement is small but
 * whose resulting document would go over it, which is how it refuses a replacement of a stored instance as well.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreTooLargeWriteRetryTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-too-large-write-retry";
    private static final int MAX_DOCUMENT_SIZE = 16 * 1024 * 1024;
    private static final int RETRIES_THE_STRATEGY_WOULD_MAKE = 5;
    // The server's refusal of an update whose resulting document is over 16 MiB
    private static final int UPDATE_RESULT_TOO_LARGE = 17419;

    private MongoClient client;
    private String database;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-too-large-retry-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        database = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close() {
        client.close();
    }

    private FlakyMongoTemplate counting(int transientFailures) {
        return new FlakyMongoTemplate(client, database, "execute", transientFailures);
    }

    // A store whose strategy would retry any failure up to the number of attempts above, quickly
    private static SpringMongoSagaStateStore<String> store(FlakyMongoTemplate template) {
        return new SpringMongoSagaStateStore<>(template, COLLECTION, String.class, null,
                org.occurrent.retry.RetryStrategy.fixed(1).maxAttempts(RETRIES_THE_STRATEGY_WOULD_MAKE).backoff(Backoff.fixed(1)));
    }

    private static SagaEnvelope<String> instance(String sagaId, String state, long version, SagaFailure failure) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), null);
    }

    private static SagaEnvelope<String> instance(String sagaId, String state, long version) {
        return instance(sagaId, state, version, null);
    }

    private int storedSize(String sagaId) {
        return requireNonNull(client.getDatabase(database).getCollection(COLLECTION)
                .aggregate(List.of(Aggregates.match(new Document("_id", sagaId)), Aggregates.project(Projections.computed("size", new Document("$bsonSize", "$$ROOT")))))
                .first()).getInteger("size");
    }

    // Saves an instance of the given stored size, which takes one call to measure the size of an instance with no state
    private void saveInstanceOfSize(SpringMongoSagaStateStore<String> store, String sagaId, int size) {
        SagaEnvelopes<String> envelopes = SagaEnvelopes.of(store, null);
        assertThat(envelopes.compareAndSave(sagaId, instance(sagaId, "", 1), 0)).isTrue();
        int empty = storedSize(sagaId);
        assertThat(envelopes.compareAndSave(sagaId, instance(sagaId, "x".repeat(size - empty), 2), 1)).isTrue();
        assertThat(storedSize(sagaId)).as("control: the instance is stored at the size asked for").isEqualTo(size);
    }

    private static MongoServerException serverFailure(Throwable thrown) {
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof MongoServerException serverException) {
                return serverException;
            }
        }
        return null;
    }

    @Test
    void control_a_transient_failure_of_a_write_is_retried_and_counted_as_a_second_attempt() {
        FlakyMongoTemplate template = counting(1);
        SpringMongoSagaStateStore<String> store = store(template);
        int before = template.calls();

        boolean saved = SagaEnvelopes.of(store, null).compareAndSave("small", instance("small", "x", 1), 0);

        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(template.calls() - before).as("the failed attempt and its retry").isEqualTo(2)
        );
    }

    @Test
    void an_insert_over_16_MiB_is_refused_by_the_driver_after_one_attempt_though_the_strategy_would_retry() {
        FlakyMongoTemplate template = counting(0);
        SpringMongoSagaStateStore<String> store = store(template);
        int before = template.calls();

        Throwable thrown = catchThrowable(() -> SagaEnvelopes.of(store, null).compareAndSave("big", instance("big", "x".repeat(MAX_DOCUMENT_SIZE + 1024), 1), 0));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(BsonMaximumSizeExceededException.class),
                () -> assertThat(template.calls() - before).as("one attempt, never retried").isEqualTo(1)
        );
    }

    @Test
    void a_replacement_that_leaves_the_stored_document_over_16_MiB_is_refused_by_the_server_after_one_attempt_though_the_strategy_would_retry() {
        FlakyMongoTemplate template = counting(0);
        SpringMongoSagaStateStore<String> store = store(template);
        SagaEnvelopes.of(store, null).compareAndSave("big-replaced", instance("big-replaced", "x", 1), 0);
        int before = template.calls();

        Throwable thrown = catchThrowable(() -> SagaEnvelopes.of(store, null).compareAndSave("big-replaced", instance("big-replaced", "x".repeat(MAX_DOCUMENT_SIZE + 1024), 2), 1));

        assertAll(
                () -> assertThat(serverFailure(thrown)).as("the server refused it").isNotNull(),
                () -> assertThat(serverFailure(thrown).getCode()).isEqualTo(UPDATE_RESULT_TOO_LARGE),
                () -> assertThat(template.calls() - before).as("one attempt, never retried").isEqualTo(1)
        );
    }

    @Test
    void an_update_that_leaves_the_stored_document_over_16_MiB_is_refused_by_the_server_after_one_attempt_though_the_strategy_would_retry() {
        FlakyMongoTemplate template = counting(0);
        SpringMongoSagaStateStore<String> store = store(template);
        saveInstanceOfSize(store, "nearly-full", MAX_DOCUMENT_SIZE - 40);
        SagaFailure failure = new SagaFailure("orders@2", 2L, NOW, IllegalStateException.class.getName(), "m".repeat(500), null, null);
        SagaEnvelope<String> withFailure = instance("nearly-full", "", 3, failure);
        int before = template.calls();

        Throwable thrown = catchThrowable(() -> SagaEnvelopes.of(store, null).compareAndSaveWithoutState("nearly-full", withFailure, 2));

        MongoServerException server = serverFailure(thrown);
        assertAll(
                () -> assertThat(thrown).isNotNull(),
                () -> assertThat(server).as("the server refused it").isNotNull(),
                () -> assertThat(server.getCode()).isEqualTo(UPDATE_RESULT_TOO_LARGE),
                () -> assertThat(template.calls() - before).as("one attempt, never retried").isEqualTo(1),
                () -> assertThat(storedSize("nearly-full")).as("the stored document is as it was").isEqualTo(MAX_DOCUMENT_SIZE - 40)
        );
    }
}
