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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
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
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a standalone MongoDB, which refuses the transactions a write of a record takes. The records are
 * written straight into their collection, and {@code findWithFirstSkippedInput} still returns the instance and its
 * first record, ordered by seq and then key, with plain reads.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore findWithFirstSkippedInput on a standalone server")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreStandaloneFirstSkippedInputTest {

    @Container
    private static final GenericContainer<?> standaloneMongo = new GenericContainer<>("mongo:8.0")
            .withExposedPorts(27017)
            // As in test-support. Without it mongod 8.0 refuses to start on the CI runners' kernel (SERVER-121912)
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
            .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1))
            .withStartupTimeout(Duration.ofMinutes(5));

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-standalone-first-skipped";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";
    private static final EventFormat JSON = requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE));

    record Payment(long amount) {
    }

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<Payment> store;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString("mongodb://" + standaloneMongo.getHost() + ":" + standaloneMongo.getMappedPort(27017) + "/saga-first-" + UUID.randomUUID().toString().substring(0, 8));
        client = MongoClients.create(connectionString);
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static CloudEvent heldEvent(String key) {
        return CloudEventBuilder.v1().withId(key).withSource(URI.create("urn:test")).withType("test").build();
    }

    private void stored(String sagaId, @Nullable String quarantineId) {
        SagaFailure failure = quarantineId == null ? null : new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, quarantineId);
        SagaEnvelope<Payment> envelope = new SagaEnvelope<>(sagaId, new Payment(12L), quarantineId == null ? SagaStatus.ACTIVE : SagaStatus.QUARANTINED, 1, List.of(), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), quarantineId == null ? null : 0L);
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(sagaId, envelope, 0)).isTrue();
    }

    private void record(String sagaId, String quarantineId, long seq, String key) {
        template.getCollection(SKIPPED_COLLECTION).insertOne(new Document("sagaId", sagaId).append("quarantineId", quarantineId).append("seq", seq).append("key", key)
                .append("event", new String(JSON.serialize(heldEvent(key)), StandardCharsets.UTF_8)));
    }

    @Test
    void returns_the_instance_and_its_first_record_by_seq_and_then_key() {
        stored("order-1", QUARANTINE_ID);
        record("order-1", QUARANTINE_ID, 5, "c@3");
        record("order-1", QUARANTINE_ID, 3, "b@2");
        record("order-1", QUARANTINE_ID, 3, "a@1");
        record("order-1", "q-other", 1, "z@0");

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-1");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(SagaEnvelopes.of(store, null).findWithFirstSkippedInput(store, "order-1").orElseThrow().envelope().quarantineId()).isEqualTo(QUARANTINE_ID),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isEqualTo(new SkippedInput(3, "a@1", heldEvent("a@1")))
        );
    }

    @Test
    void returns_no_record_for_an_instance_without_a_quarantine_id_and_empty_for_an_absent_one() {
        stored("order-2", null);
        record("order-2", QUARANTINE_ID, 1, "left@1");

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-2");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isNull(),
                () -> assertThat(store.findWithFirstSkippedInput("nowhere")).isEmpty()
        );
    }
}
