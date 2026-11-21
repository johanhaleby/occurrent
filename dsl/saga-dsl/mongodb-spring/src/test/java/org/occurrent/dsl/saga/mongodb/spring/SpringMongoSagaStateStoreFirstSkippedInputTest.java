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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;
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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set. {@code findWithFirstSkippedInput} returns the instance and the first record under its
 * quarantine id, ordered by seq and then key, as one write left them. Both reads run in one transaction of the store's
 * at read concern snapshot, or in the caller's transaction when there is one.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore findWithFirstSkippedInput on a replica set")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreFirstSkippedInputTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-first-skipped";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";

    record Payment(long amount) {
    }

    // What a command carried when it started
    private record Seen(String name, String collection, @Nullable BsonDocument lsid, @Nullable BsonValue txnNumber, @Nullable String readConcernLevel) {
    }

    private final List<Seen> seen = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private SpringMongoSagaStateStore<Payment> store;
    private MongoTemplate template;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-first-skipped-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonDocument command = event.getCommand();
                BsonValue target = command.get(event.getCommandName());
                BsonDocument readConcern = command.isDocument("readConcern") ? command.getDocument("readConcern") : null;
                seen.add(new Seen(event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : "",
                        command.isDocument("lsid") ? command.getDocument("lsid").clone() : null, command.get("txnNumber"),
                        readConcern != null && readConcern.isString("level") ? readConcern.getString("level").getValue() : null));
            }
        }).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        template.setWriteConcern(WriteConcern.MAJORITY);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, RetryStrategy.none());
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<Payment> instance(String sagaId, @Nullable String quarantineId) {
        SagaFailure failure = quarantineId == null ? null : new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, quarantineId);
        return new SagaEnvelope<>(sagaId, new Payment(12L), quarantineId == null ? SagaStatus.ACTIVE : SagaStatus.QUARANTINED, 1, List.of(), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), quarantineId == null ? null : 0L);
    }

    private static CloudEvent heldEvent(String key) {
        return CloudEventBuilder.v1().withId(key).withSource(URI.create("urn:test")).withType("test").build();
    }

    private void stored(String sagaId, @Nullable String quarantineId) {
        assertThat(SagaEnvelopes.of(store, null).compareAndSave(sagaId, instance(sagaId, quarantineId), 0)).isTrue();
    }

    private void skipped(String sagaId, String quarantineId, long seq, String key) {
        SagaEnvelope<Payment> current = SagaEnvelopes.of(store, null).find(sagaId).orElseThrow();
        SagaEnvelope<Payment> next = new SagaEnvelope<>(sagaId, current.state(), current.status(), current.version() + 1, current.timers(), current.streamWatermarks(), null,
                current.createdAt(), current.updatedAt(), current.completedAt(), current.currentStep(), current.started(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
        assertThat(SagaEnvelopes.of(store, null).compareAndSaveSkipped(store, next, current.version(), quarantineId, new SkippedInput(seq, key, heldEvent(key)))).isTrue();
    }

    private List<Seen> inATransaction(String collection, String commandName) {
        return seen.stream().filter(s -> s.name().equals(commandName) && s.collection().equals(collection) && s.txnNumber() != null).toList();
    }

    @Test
    void returns_the_instance_and_its_first_record_by_seq_and_then_key() {
        stored("order-1", QUARANTINE_ID);
        skipped("order-1", QUARANTINE_ID, 5, "c@3");
        skipped("order-1", QUARANTINE_ID, 3, "b@2");
        skipped("order-1", QUARANTINE_ID, 3, "a@1");

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-1");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isEqualTo(new SkippedInput(3, "a@1", heldEvent("a@1"))),
                () -> assertThat(SagaEnvelopes.of(store, null).findWithFirstSkippedInput(store, "order-1").orElseThrow().envelope().version())
                        .as("the instance is the one the first record was read with").isEqualTo(4L)
        );
    }

    @Test
    void returns_no_record_for_an_instance_without_a_quarantine_id() {
        stored("order-2", null);

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-2");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isNull()
        );
    }

    @Test
    void returns_no_record_when_the_only_records_are_under_another_quarantine_id() {
        stored("order-3", QUARANTINE_ID);
        skipped("order-3", "q-other", 1, "a@1");

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-3");

        assertAll(
                () -> assertThat(read).isPresent(),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isNull()
        );
    }

    @Test
    void returns_empty_for_an_instance_that_exists_nowhere() {
        stored("order-4", QUARANTINE_ID);

        assertThat(store.findWithFirstSkippedInput("nowhere")).isEmpty();
    }

    @Test
    void reads_the_instance_and_its_first_record_in_one_snapshot_transaction() {
        stored("order-5", QUARANTINE_ID);
        skipped("order-5", QUARANTINE_ID, 1, "a@1");
        seen.clear();

        store.findWithFirstSkippedInput("order-5");

        List<Seen> instanceFinds = inATransaction(COLLECTION, "find");
        List<Seen> recordFinds = inATransaction(SKIPPED_COLLECTION, "find");
        List<Seen> ends = seen.stream().filter(s -> s.name().equals("commitTransaction") || s.name().equals("abortTransaction")).toList();
        assertAll(
                () -> assertThat(instanceFinds).as("the read of the instance runs in a transaction").hasSize(1),
                () -> assertThat(recordFinds).as("the read of the first record runs in a transaction").hasSize(1),
                () -> assertThat(instanceFinds.getFirst().readConcernLevel()).as("the transaction begins at read concern snapshot").isEqualTo("snapshot"),
                () -> assertThat(recordFinds.getFirst().lsid()).as("the same session").isEqualTo(instanceFinds.getFirst().lsid()),
                () -> assertThat(recordFinds.getFirst().txnNumber()).as("the same transaction").isEqualTo(instanceFinds.getFirst().txnNumber()),
                () -> assertThat(ends).as("the transaction ends, and it is the one both reads ran in").hasSize(1)
                        .allSatisfy(end -> {
                            assertThat(end.lsid()).isEqualTo(instanceFinds.getFirst().lsid());
                            assertThat(end.txnNumber()).isEqualTo(instanceFinds.getFirst().txnNumber());
                        })
        );
    }

    @Test
    void reads_in_the_transaction_of_the_caller_when_there_is_one() {
        stored("order-6", QUARANTINE_ID);
        skipped("order-6", QUARANTINE_ID, 1, "a@1");
        TransactionTemplate callersTransaction = new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory()));
        seen.clear();

        List<Object> endsBeforeTheCallerEnds = new CopyOnWriteArrayList<>();
        Optional<StoredSagaWithFirstSkippedInput> read = callersTransaction.execute(status -> {
            Optional<StoredSagaWithFirstSkippedInput> found = store.findWithFirstSkippedInput("order-6");
            seen.stream().filter(s -> s.name().equals("commitTransaction") || s.name().equals("abortTransaction")).forEach(endsBeforeTheCallerEnds::add);
            return found;
        });

        List<Seen> instanceFinds = inATransaction(COLLECTION, "find");
        List<Seen> recordFinds = inATransaction(SKIPPED_COLLECTION, "find");
        assertAll(
                () -> assertThat(requireNonNull(read).orElseThrow().firstSkippedInput()).isEqualTo(new SkippedInput(1, "a@1", heldEvent("a@1"))),
                () -> assertThat(instanceFinds).hasSize(1),
                () -> assertThat(recordFinds).hasSize(1),
                () -> assertThat(recordFinds.getFirst().txnNumber()).as("the same transaction").isEqualTo(instanceFinds.getFirst().txnNumber()),
                () -> assertThat(endsBeforeTheCallerEnds).as("the store ends no transaction that is not its own").isEmpty(),
                () -> assertThat(seen.stream().filter(s -> s.name().equals("commitTransaction")).toList()).as("the caller's transaction commits once").hasSize(1)
        );
    }
}
