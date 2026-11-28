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
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set. {@code findWithFirstSkippedInput} returns the instance and the first record under its
 * quarantine id, ordered by seq and then key, as one write left them. Both reads run in one transaction of the store's
 * at read concern snapshot. A transaction of the caller's is refused.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore findWithFirstSkippedInput on a replica set")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreFirstSkippedInputTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-first-skipped";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String QUARANTINE_ID = "q-1";
    private static final int HOST_UNREACHABLE = 6;

    record Payment(long amount) {
    }

    // What a command carried when it started
    private record Seen(long requestId, String name, String collection, @Nullable BsonDocument lsid, @Nullable BsonValue txnNumber, @Nullable String readConcernLevel,
                        @Nullable BsonTimestamp afterClusterTime) {
    }

    // Runs once, on the thread of the command, before the command is sent
    private record Trigger(Predicate<Seen> when, Runnable action) {
    }

    private final List<Seen> seen = new CopyOnWriteArrayList<>();
    private final Map<Long, BsonTimestamp> operationTimeOfFindReply = new ConcurrentHashMap<>();
    private final Map<Long, BsonTimestamp> operationTimeOfFailedFindReply = new ConcurrentHashMap<>();
    private final List<Trigger> triggers = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private MongoClient otherClient;
    private SpringMongoSagaStateStore<Payment> store;
    private SpringMongoSagaStateStore<Payment> otherStore;
    private MongoTemplate template;
    private MongoTemplate otherTemplate;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-first-skipped-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(recordingListener()).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        template.setWriteConcern(WriteConcern.MAJORITY);
        store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, RetryStrategy.none());
        otherClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).build());
        otherTemplate = new MongoTemplate(otherClient, requireNonNull(connectionString.getDatabase()));
        otherTemplate.setWriteConcern(WriteConcern.MAJORITY);
        otherStore = new SpringMongoSagaStateStore<>(otherTemplate, COLLECTION, Payment.class, null, RetryStrategy.none());
    }

    private CommandListener recordingListener() {
        return new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonDocument command = event.getCommand();
                BsonValue target = command.get(event.getCommandName());
                BsonDocument readConcern = command.isDocument("readConcern") ? command.getDocument("readConcern") : null;
                Seen started = new Seen(event.getRequestId(), event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : "",
                        command.isDocument("lsid") ? command.getDocument("lsid").clone() : null, command.get("txnNumber"),
                        readConcern != null && readConcern.isString("level") ? readConcern.getString("level").getValue() : null,
                        readConcern != null && readConcern.isTimestamp("afterClusterTime") ? readConcern.getTimestamp("afterClusterTime") : null);
                seen.add(started);
                for (Trigger trigger : triggers) {
                    if (trigger.when().test(started) && triggers.remove(trigger)) {
                        trigger.action().run();
                    }
                }
            }

            @Override
            public void commandFailed(CommandFailedEvent event) {
                if (event.getCommandName().equals("find") && event.getThrowable() instanceof MongoCommandException failure && failure.getResponse().isTimestamp("operationTime")) {
                    operationTimeOfFailedFindReply.put((long) event.getRequestId(), failure.getResponse().getTimestamp("operationTime"));
                }
            }

            @Override
            public void commandSucceeded(CommandSucceededEvent event) {
                BsonDocument reply = event.getResponse();
                if (event.getCommandName().equals("find") && reply.isTimestamp("operationTime")) {
                    operationTimeOfFindReply.put((long) event.getRequestId(), reply.getTimestamp("operationTime"));
                }
            }
        };
    }

    @AfterEach
    void close() {
        client.close();
        otherClient.close();
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
        stored(store, sagaId, quarantineId);
    }

    private static void stored(SpringMongoSagaStateStore<Payment> into, String sagaId, @Nullable String quarantineId) {
        assertThat(SagaEnvelopes.of(into, null).compareAndSave(sagaId, instance(sagaId, quarantineId), 0)).isTrue();
    }

    private void skipped(String sagaId, String quarantineId, long seq, String key) {
        skipped(store, sagaId, quarantineId, seq, key);
    }

    private static void skipped(SpringMongoSagaStateStore<Payment> into, String sagaId, String quarantineId, long seq, String key) {
        SagaEnvelope<Payment> current = SagaEnvelopes.of(into, null).find(sagaId).orElseThrow();
        SagaEnvelope<Payment> next = new SagaEnvelope<>(sagaId, current.state(), current.status(), current.version() + 1, current.timers(), current.streamWatermarks(), null,
                current.createdAt(), current.updatedAt(), current.completedAt(), current.currentStep(), current.started(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
        assertThat(SagaEnvelopes.of(into, null).compareAndSaveSkipped(into, next, current.version(), quarantineId, new SkippedInput(seq, key, heldEvent(key)))).isTrue();
    }

    private List<Seen> inATransaction(String collection, String commandName) {
        return seen.stream().filter(s -> s.name().equals(commandName) && s.collection().equals(collection) && s.txnNumber() != null).toList();
    }

    private static boolean linearizableFindOfTheInstance(Seen s) {
        return s.name().equals("find") && s.collection().equals(COLLECTION) && "linearizable".equals(s.readConcernLevel());
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
                        .as("the instance is at the version the last write left").isEqualTo(4L)
        );
    }

    @Test
    void returns_no_record_for_an_instance_without_a_quarantine_id() {
        stored("order-2", null);
        skipped("order-2", QUARANTINE_ID, 1, "left@1");

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

    private void assertRefusedBeforeAnyRead(Throwable thrown) {
        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage(SpringMongoSagaStateStore.CALLERS_TRANSACTION_MESSAGE),
                () -> assertThat(seen.stream().filter(s -> s.name().equals("find") && (s.collection().equals(COLLECTION) || s.collection().equals(SKIPPED_COLLECTION))).toList())
                        .as("finds on the instance or the records after the call started").isEmpty()
        );
    }

    // Snapshot is refused too, since the store cannot see the read concern of a transaction it joins. The module has no
    // junit-jupiter-params, so the cases are dynamic tests, which share the database and so use an instance each
    @TestFactory
    Stream<DynamicTest> refuses_a_transaction_of_the_caller_whatever_its_read_concern_before_it_sends_a_read() {
        Map<String, @Nullable TransactionOptions> managers = new LinkedHashMap<>();
        managers.put("read concern local", TransactionOptions.builder().readConcern(ReadConcern.LOCAL).build());
        managers.put("read concern majority", TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).build());
        managers.put("read concern snapshot", TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).build());
        managers.put("no transaction options", null);
        return managers.entrySet().stream().map(entry -> DynamicTest.dynamicTest(entry.getKey(), () -> {
            String sagaId = "order-6 " + entry.getKey();
            stored(sagaId, QUARANTINE_ID);
            skipped(sagaId, QUARANTINE_ID, 1, "a@1");
            TransactionOptions options = entry.getValue();
            MongoTransactionManager manager = options == null ? new MongoTransactionManager(template.getMongoDatabaseFactory()) : new MongoTransactionManager(template.getMongoDatabaseFactory(), options);
            seen.clear();

            Throwable thrown = catchThrowable(() -> new TransactionTemplate(manager).execute(status -> store.findWithFirstSkippedInput(sagaId)));

            assertRefusedBeforeAnyRead(thrown);
        }));
    }

    @Test
    void refuses_a_transaction_bound_while_synchronization_is_off_before_it_sends_a_read() {
        stored("order-7", QUARANTINE_ID);
        MongoTransactionManager manager = new MongoTransactionManager(template.getMongoDatabaseFactory());
        manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
        seen.clear();

        Throwable thrown = catchThrowable(() -> new TransactionTemplate(manager).execute(status -> store.findWithFirstSkippedInput("order-7")));

        assertRefusedBeforeAnyRead(thrown);
    }

    @Test
    void refuses_a_transaction_on_a_session_the_template_was_bound_to_before_it_sends_a_read() {
        stored("order-8", QUARANTINE_ID);
        seen.clear();

        Throwable thrown;
        try (ClientSession session = client.startSession()) {
            SpringMongoSagaStateStore<Payment> sessionBound = new SpringMongoSagaStateStore<>(template.withSession(session), COLLECTION, Payment.class, null, RetryStrategy.none());
            session.startTransaction();
            thrown = catchThrowable(() -> sessionBound.findWithFirstSkippedInput("order-8"));
            session.abortTransaction();
        }

        assertRefusedBeforeAnyRead(thrown);
    }

    // The instance and one record are written through the other client as the linearizable find is about to be sent
    private void theInstanceAppearsAtTheLinearizableRead(String sagaId) {
        triggers.add(new Trigger(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance, () -> {
            stored(otherStore, sagaId, QUARANTINE_ID);
            skipped(otherStore, sagaId, QUARANTINE_ID, 1, "a@1");
        }));
    }

    // The two snapshots cannot be made to disagree on a one-node replica set, so this checks the causal boundary on the
    // wire, the second snapshot asks for a time at or after the one the linearizable read replied with
    @Test
    void reads_an_instance_the_linearizable_read_found_in_a_snapshot_that_starts_after_that_read() {
        theInstanceAppearsAtTheLinearizableRead("order-9");
        seen.clear();

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-9");

        List<Seen> linearizableFinds = seen.stream().filter(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance).toList();
        List<Seen> snapshotFinds = inATransaction(COLLECTION, "find");
        BsonTimestamp linearizableReplyTime = linearizableFinds.isEmpty() ? null : operationTimeOfFindReply.get(linearizableFinds.getFirst().requestId());
        assertAll(
                () -> assertThat(read).as("the instance the linearizable read found").isPresent(),
                () -> assertThat(read.orElseThrow().saga().version()).as("the version written").isEqualTo(2L),
                () -> assertThat(read.orElseThrow().firstSkippedInput()).isEqualTo(new SkippedInput(1, "a@1", heldEvent("a@1"))),
                () -> assertThat(linearizableFinds).as("one linearizable read").hasSize(1),
                () -> assertThat(snapshotFinds).as("one snapshot read before the linearizable read and one after").hasSize(2),
                () -> assertThat(snapshotFinds.getFirst().afterClusterTime()).as("the first snapshot asks for no time").isNull(),
                () -> assertThat(linearizableReplyTime).as("operationTime in the reply of the linearizable read").isNotNull(),
                () -> assertThat(snapshotFinds.getLast().afterClusterTime()).as("the second snapshot asks for a time").isNotNull(),
                () -> assertThat(snapshotFinds.getLast().afterClusterTime()).as("at or after the reply of the linearizable read").isGreaterThanOrEqualTo(linearizableReplyTime)
        );
    }

    @Test
    void returns_empty_when_the_instance_is_gone_again_when_the_snapshot_after_the_linearizable_read_reads_it() {
        theInstanceAppearsAtTheLinearizableRead("order-10");
        triggers.add(new Trigger(s -> s.name().equals("find") && s.collection().equals(COLLECTION) && s.txnNumber() != null && seen.stream().anyMatch(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance),
                () -> otherTemplate.remove(Query.query(Criteria.where("_id").is("order-10")), COLLECTION)));
        seen.clear();

        Optional<StoredSagaWithFirstSkippedInput> read = store.findWithFirstSkippedInput("order-10");

        List<Seen> snapshotFinds = inATransaction(COLLECTION, "find");
        assertAll(
                () -> assertThat(read).isEmpty(),
                () -> assertThat(seen.stream().filter(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance).toList()).as("linearizable reads of the instance").hasSize(1),
                () -> assertThat(snapshotFinds).as("snapshot reads of the instance").hasSize(2),
                () -> assertThat(triggers).as("triggers that did not fire").isEmpty(),
                () -> assertThat(snapshotFinds.getLast().afterClusterTime()).as("the second snapshot asks for a time").isNotNull()
        );
    }

    // The driver advances the operation time of a session from every reply, an error reply included, and sends it as
    // afterClusterTime on the next read of a causally consistent session whatever the read concern. MongoDB refuses
    // afterClusterTime with linearizable, so a linearizable read retried in such a session could never succeed. The
    // driver's own retry of a read is off, so the retry is the store's
    @Test
    void retries_a_linearizable_read_that_failed_with_an_operation_time_without_asking_for_a_time_and_reads_the_snapshot_after_the_retry() {
        theInstanceAppearsAtTheLinearizableRead("order-10");
        AtomicReference<FailPoint> failedFind = new AtomicReference<>();
        triggers.add(new Trigger(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance, () -> failedFind.set(FailPoint.failing(client, "find", 1, HOST_UNREACHABLE))));
        try (MongoClient retryingClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(mongoDBContainer.getReplicaSetUrl(template.getDb().getName())))
                .retryReads(false).addCommandListener(recordingListener()).build())) {
            MongoTemplate retryingTemplate = new MongoTemplate(retryingClient, template.getDb().getName());
            retryingTemplate.setWriteConcern(WriteConcern.MAJORITY);
            SpringMongoSagaStateStore<Payment> retryingStore = new SpringMongoSagaStateStore<>(retryingTemplate, COLLECTION, Payment.class, null, RetryStrategy.fixed(10).maxAttempts(3));
            seen.clear();

            AtomicReference<Optional<StoredSagaWithFirstSkippedInput>> read = new AtomicReference<>(Optional.empty());
            Throwable thrown;
            try {
                thrown = catchThrowable(() -> read.set(retryingStore.findWithFirstSkippedInput("order-10")));
            } finally {
                FailPoint armed = failedFind.get();
                if (armed != null) {
                    armed.close();
                }
            }

            Optional<StoredSagaWithFirstSkippedInput> result = read.get();
            List<Seen> linearizableFinds = seen.stream().filter(SpringMongoSagaStateStoreFirstSkippedInputTest::linearizableFindOfTheInstance).toList();
            List<Seen> snapshotFinds = inATransaction(COLLECTION, "find");
            BsonTimestamp failedReplyTime = linearizableFinds.isEmpty() ? null : operationTimeOfFailedFindReply.get(linearizableFinds.getFirst().requestId());
            BsonTimestamp retriedReplyTime = linearizableFinds.size() < 2 ? null : operationTimeOfFindReply.get(linearizableFinds.getLast().requestId());
            assertAll(
                    () -> assertThat(thrown).as("thrown by the read").isNull(),
                    () -> assertThat(result).as("the instance the retried linearizable read found").isPresent(),
                    () -> assertThat(result.orElseThrow().saga().version()).as("the version written").isEqualTo(2L),
                    () -> assertThat(result.orElseThrow().firstSkippedInput()).isEqualTo(new SkippedInput(1, "a@1", heldEvent("a@1"))),
                    () -> assertThat(failedReplyTime).as("operationTime in the error reply of the failed linearizable read, so the retry is in the window").isNotNull(),
                    () -> assertThat(linearizableFinds).as("the failed linearizable read and its retry").hasSize(2),
                    () -> assertThat(linearizableFinds.getLast().afterClusterTime()).as("the retried linearizable read asks for no time").isNull(),
                    () -> assertThat(retriedReplyTime).as("operationTime in the reply of the retried linearizable read").isNotNull(),
                    () -> assertThat(snapshotFinds).as("one snapshot read before the linearizable reads and one after").hasSize(2),
                    () -> assertThat(snapshotFinds.getLast().afterClusterTime()).as("the second snapshot asks for a time").isNotNull(),
                    () -> assertThat(snapshotFinds.getLast().afterClusterTime()).as("at or after the reply of the retried linearizable read").isGreaterThanOrEqualTo(retriedReplyTime)
            );
        }
    }
}
