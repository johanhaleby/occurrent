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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.PojoCloudEventData;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A call of the store inside a MongoDB transaction of the caller's sends each database operation once,
 * and the failure reaches the caller as MongoDB reported it. MongoDB has aborted the transaction on the server, so the
 * same operation sent again in it fails with 251 {@code NoSuchTransaction}, which would replace the failure the caller
 * can act on. A call outside a transaction keeps its {@link RetryStrategy}. The driver's own retries are off, so every
 * command the listener counts was sent by the store.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreTransactionRetryTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-transaction-retry";
    private static final int HOST_UNREACHABLE = 6;
    private static final int NO_SUCH_TRANSACTION = 251;
    private static final int ATTEMPTS = 3;

    record Payment(long amount) {
    }

    private final List<String> sent = new CopyOnWriteArrayList<>();
    private final AtomicInteger retries = new AtomicInteger();
    private final AtomicInteger commits = new AtomicInteger();
    private MongoClient client;
    private MongoTemplate template;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-transaction-retry-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY)
                .retryReads(false).retryWrites(false)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("commitTransaction")) {
                            commits.incrementAndGet();
                        }
                        var target = event.getCommand().get(event.getCommandName());
                        if (target != null && target.isString() && target.asString().getValue().equals(COLLECTION)) {
                            sent.add(event.getCommandName());
                        }
                    }
                }).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        template.setWriteConcern(WriteConcern.MAJORITY);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private static SagaEnvelope<Payment> payment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of(), null, NOW.minusSeconds(600),
                NOW, null, null, true, null, Map.of(), null);
    }

    private RetryStrategy retryingWithAHook() {
        return RetryStrategy.retry().maxAttempts(ATTEMPTS).backoff(Backoff.fixed(5)).onBeforeRetry(failure -> retries.incrementAndGet());
    }

    // Not built from RetryStrategy.retry(), so the store cannot give it a predicate and runs its loop as it is
    private static final class CountingRetry implements RetryStrategy {
        private final AtomicInteger attempts = new AtomicInteger();
        private final List<RuntimeException> failures = new CopyOnWriteArrayList<>();

        @Override
        public <T> T execute(Supplier<T> supplier) {
            return loop(() -> {
                attempts.incrementAndGet();
                try {
                    return supplier.get();
                } catch (RuntimeException e) {
                    failures.add(e);
                    throw e;
                }
            });
        }

        // The store's index creation at construction, which isn't an attempt the tests count
        @Override
        public void execute(Runnable runnable) {
            loop(() -> {
                runnable.run();
                return null;
            });
        }

        private static <T> T loop(Supplier<T> supplier) {
            RuntimeException last;
            int made = 0;
            do {
                made++;
                try {
                    return supplier.get();
                } catch (RuntimeException e) {
                    last = e;
                }
            } while (made < ATTEMPTS);
            throw last;
        }
    }

    private SpringMongoSagaStateStore<Payment> storeOn(MongoTemplate on, RetryStrategy strategy) {
        return new SpringMongoSagaStateStore<>(on, COLLECTION, Payment.class, null, strategy);
    }

    private void anInstance(String sagaId) {
        assertThat(SagaEnvelopes.of(storeOn(template, RetryStrategy.none()), null).compareAndSave(sagaId, payment(sagaId), 0)).isTrue();
    }

    private long sentOf(String command) {
        return sent.stream().filter(command::equals).count();
    }

    private static List<Integer> mongoCodesIn(Throwable failure) {
        List<Integer> codes = new ArrayList<>();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof MongoException mongo) {
                codes.add(mongo.getCode());
            }
        }
        return codes;
    }

    // The caller catches what the store throws and rolls back, so what the transaction does at its end can't replace it
    private Throwable failureInside(TransactionTemplate transaction, String command, Runnable call) {
        try (FailPoint failure = FailPoint.failing(client, command, ATTEMPTS, HOST_UNREACHABLE)) {
            return transaction.execute(status -> {
                Throwable thrown = catchThrowable(call::run);
                status.setRollbackOnly();
                return thrown;
            });
        }
    }

    private void assertSentOnceAndFailedAsMongoDbReported(String command, Throwable thrown, AtomicInteger attempts) {
        assertAll(
                () -> assertThat(thrown).as("what the store threw inside the transaction").isNotNull(),
                () -> assertThat(sentOf(command)).as("%s commands sent to the saga collection", command).isEqualTo(1),
                () -> assertThat(attempts.get()).as("attempts the retry strategy made or retries it took").isLessThanOrEqualTo(1),
                () -> assertThat(thrown == null ? List.<Integer>of() : mongoCodesIn(thrown)).as("MongoDB error codes in the cause chain").contains(HOST_UNREACHABLE).doesNotContain(NO_SUCH_TRANSACTION)
        );
    }

    @Nested
    class AMongoTransactionManagerWithoutSynchronization {

        private TransactionTemplate transaction() {
            MongoTransactionManager manager = new MongoTransactionManager(template.getMongoDatabaseFactory());
            manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
            return new TransactionTemplate(manager);
        }

        @Test
        void compareAndSave_that_fails_is_sent_once_and_the_caller_sees_the_failure_MongoDB_reported() {
            var store = storeOn(template, retryingWithAHook());

            Throwable thrown = failureInside(transaction(), "insert", () -> SagaEnvelopes.of(store, null).compareAndSave("order-1", payment("order-1"), 0));

            assertSentOnceAndFailedAsMongoDbReported("insert", thrown, retries);
        }

        @Test
        void findWithDueTimers_that_fails_is_sent_once_and_the_caller_sees_the_failure_MongoDB_reported() {
            anInstance("order-2");
            sent.clear();
            var store = storeOn(template, retryingWithAHook());

            Throwable thrown = failureInside(transaction(), "find", () -> SagaEnvelopes.of(store, null).findWithDueTimers(NOW.plusSeconds(60), 10));

            assertSentOnceAndFailedAsMongoDbReported("find", thrown, retries);
        }
    }

    @Nested
    class ATemplateOnAlwaysInATransactionOfAnotherResource {

        // A Spring transaction of another resource, so no MongoDB transaction starts until the template operates
        final class AnotherResourcesTransactionManager extends AbstractPlatformTransactionManager {
            @Override
            protected Object doGetTransaction() {
                return new Object();
            }

            @Override
            protected void doBegin(Object transaction, TransactionDefinition definition) {
            }

            @Override
            protected void doCommit(DefaultTransactionStatus status) {
            }

            @Override
            protected void doRollback(DefaultTransactionStatus status) {
            }
        }

        private MongoTemplate always() {
            MongoTemplate always = new MongoTemplate(template.getMongoDatabaseFactory());
            always.setWriteConcern(WriteConcern.MAJORITY);
            always.setSessionSynchronization(SessionSynchronization.ALWAYS);
            return always;
        }

        @Test
        void compareAndSave_that_fails_as_the_first_operation_is_sent_once_and_the_caller_sees_the_failure_MongoDB_reported() {
            var store = storeOn(always(), retryingWithAHook());

            Throwable thrown = failureInside(new TransactionTemplate(new AnotherResourcesTransactionManager()), "insert", () -> SagaEnvelopes.of(store, null).compareAndSave("order-3", payment("order-3"), 0));

            assertSentOnceAndFailedAsMongoDbReported("insert", thrown, retries);
        }

        @Test
        void compareAndSave_with_a_retry_strategy_implemented_by_the_caller_that_retries_any_failure_sends_once_and_the_strategy_makes_one_attempt() {
            CountingRetry strategy = new CountingRetry();
            var store = storeOn(always(), strategy);

            Throwable thrown = failureInside(new TransactionTemplate(new AnotherResourcesTransactionManager()), "insert", () -> SagaEnvelopes.of(store, null).compareAndSave("order-9", payment("order-9"), 0));

            assertAll(
                    () -> assertThat(sentOf("insert")).as("insert commands sent to the saga collection").isEqualTo(1),
                    () -> assertThat(strategy.attempts.get()).as("attempts the retry strategy made, which it sees as a success").isEqualTo(1),
                    () -> assertThat(strategy.failures).as("failures the attempts threw, which the strategy could retry").isEmpty(),
                    () -> assertThat(thrown == null ? List.<Integer>of() : mongoCodesIn(thrown)).as("MongoDB error codes in the cause chain").contains(HOST_UNREACHABLE).doesNotContain(NO_SUCH_TRANSACTION)
            );
        }

        @Test
        void findWithDueTimers_with_a_retry_strategy_implemented_by_the_caller_that_retries_any_failure_sends_once_and_the_strategy_makes_one_attempt() {
            anInstance("order-10");
            sent.clear();
            CountingRetry strategy = new CountingRetry();
            var store = storeOn(always(), strategy);

            Throwable thrown = failureInside(new TransactionTemplate(new AnotherResourcesTransactionManager()), "find", () -> SagaEnvelopes.of(store, null).findWithDueTimers(NOW.plusSeconds(60), 10));

            assertAll(
                    () -> assertThat(sentOf("find")).as("find commands sent to the saga collection").isEqualTo(1),
                    () -> assertThat(strategy.attempts.get()).as("attempts the retry strategy made, which it sees as a success").isEqualTo(1),
                    () -> assertThat(thrown == null ? List.<Integer>of() : mongoCodesIn(thrown)).as("MongoDB error codes in the cause chain").contains(HOST_UNREACHABLE).doesNotContain(NO_SUCH_TRANSACTION)
            );
        }

        @Test
        void findWithDueTimers_that_fails_as_the_first_operation_is_sent_once_and_the_caller_sees_the_failure_MongoDB_reported() {
            anInstance("order-4");
            sent.clear();
            var store = storeOn(always(), retryingWithAHook());

            Throwable thrown = failureInside(new TransactionTemplate(new AnotherResourcesTransactionManager()), "find", () -> SagaEnvelopes.of(store, null).findWithDueTimers(NOW.plusSeconds(60), 10));

            assertSentOnceAndFailedAsMongoDbReported("find", thrown, retries);
        }
    }

    @Nested
    class AMongoTransactionManagerWithDefaultSynchronization {

        private TransactionTemplate transaction() {
            return new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory()));
        }

        @Test
        void compareAndSave_that_fails_is_sent_once_and_the_caller_sees_the_failure_MongoDB_reported() {
            var store = storeOn(template, retryingWithAHook());

            Throwable thrown = failureInside(transaction(), "insert", () -> SagaEnvelopes.of(store, null).compareAndSave("order-5", payment("order-5"), 0));

            assertSentOnceAndFailedAsMongoDbReported("insert", thrown, retries);
        }

        @Test
        void compareAndSave_with_a_retry_strategy_implemented_by_the_caller_makes_one_attempt() {
            CountingRetry strategy = new CountingRetry();
            var store = storeOn(template, strategy);

            Throwable thrown = failureInside(transaction(), "insert", () -> SagaEnvelopes.of(store, null).compareAndSave("order-6", payment("order-6"), 0));

            assertSentOnceAndFailedAsMongoDbReported("insert", thrown, strategy.attempts);
        }
    }

    @Nested
    class TheCommitOfTheStoresOwnTransaction {
        private static final int BAD_VALUE = 2;
        private static final String UNKNOWN_COMMIT_RESULT = "UnknownTransactionCommitResult";

        private StoredSaga anInstanceToSkipAnInputOf(String sagaId) {
            anInstance(sagaId);
            return storeOn(template, RetryStrategy.none()).find(sagaId).orElseThrow();
        }

        @Test
        void a_commit_that_fails_without_the_unknown_result_label_is_sent_once_under_a_retry_strategy_implemented_by_the_caller_that_retries_any_failure() {
            StoredSaga instance = anInstanceToSkipAnInputOf("order-11");
            var store = storeOn(template, new CountingRetry());

            Throwable thrown;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", ATTEMPTS, BAD_VALUE)) {
                thrown = catchThrowable(() -> store.compareAndSaveSkipped(instance, 1, "q-1", new SkippedInput(0, "order-11@7", heldEvent("order-11@7"))));
            }

            assertAll(
                    () -> assertThat(thrown).as("the failed commit reaches the caller").isNotNull(),
                    () -> assertThat(commits.get()).as("commitTransaction commands sent").isEqualTo(1)
            );
        }

        @Test
        void a_commit_that_fails_with_the_unknown_result_label_is_sent_again_under_a_retry_strategy_implemented_by_the_caller_and_the_transition_is_saved() {
            StoredSaga instance = anInstanceToSkipAnInputOf("order-12");
            var store = storeOn(template, new CountingRetry());

            boolean saved;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", 1, BAD_VALUE, UNKNOWN_COMMIT_RESULT)) {
                saved = store.compareAndSaveSkipped(instance, 1, "q-1", new SkippedInput(0, "order-12@7", heldEvent("order-12@7")));
            }

            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(commits.get()).as("commitTransaction commands sent, the failed one and its retry").isEqualTo(2)
            );
        }

        private static final String QUARANTINE_ID = "q-1";
        private static final String SKIPPED_INPUTS = COLLECTION + "_skipped_inputs";

        // What a call threw and the warnings the store logged during exactly that call
        private record Logged(Throwable thrown, List<String> warnings) {
        }

        private static Logged logged(Runnable call) {
            Logger storeLog = (Logger) LoggerFactory.getLogger(SpringMongoSagaStateStore.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            storeLog.addAppender(appender);
            Throwable thrown;
            try {
                thrown = catchThrowable(call::run);
            } finally {
                storeLog.detachAppender(appender);
                appender.stop();
            }
            return new Logged(thrown, appender.list.stream().filter(event -> event.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList());
        }

        // A number extension the store holds as another class and data it holds only as its bytes, one warning each
        private static CloudEvent eventWithTwoWarnings(String key) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("price", new BigDecimal("1234567890.123456789"));
            return io.cloudevents.core.builder.CloudEventBuilder.v1().withId(key).withSource(java.net.URI.create("urn:test")).withType("test")
                    .withExtension("number", (Number) new AtomicLong(1_234_567))
                    .withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(StandardCharsets.UTF_8))).build();
        }

        private static SagaEnvelope<Payment> quarantined(String sagaId, long version) {
            return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.QUARANTINED, version, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true,
                    new SagaFailure(sagaId + "@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", null, QUARANTINE_ID), Map.of(), 0L);
        }

        private SpringMongoSagaStateStore<Payment> aQuarantinedInstanceWithTwoRecords(String sagaId, SpringMongoSagaStateStore<Payment> store) {
            var envelopes = SagaEnvelopes.of(store, null);
            assertThat(envelopes.compareAndSave(sagaId, quarantined(sagaId, 1), 0)).isTrue();
            assertThat(envelopes.compareAndSaveSkipped(store, quarantined(sagaId, 2), 1, QUARANTINE_ID, new SkippedInput(0, sagaId + "@8", heldEvent(sagaId + "@8")))).isTrue();
            assertThat(envelopes.compareAndSaveSkipped(store, quarantined(sagaId, 3), 2, QUARANTINE_ID, new SkippedInput(1, sagaId + "@9", heldEvent(sagaId + "@9")))).isTrue();
            return store;
        }

        private long records() {
            return template.getCollection(SKIPPED_INPUTS).countDocuments();
        }

        private void assertSkipAndItsWarningsAreNotKept(String sagaId, long recordsBefore, Logged skipped) {
            assertAll(
                    () -> assertThat(skipped.thrown()).as("what the store threw").isNotNull(),
                    () -> assertThat(skipped.warnings()).as("the warnings logged").isEmpty(),
                    () -> assertThat(records()).as("the records kept").isEqualTo(recordsBefore),
                    () -> assertThat(storeOn(template, RetryStrategy.none()).find(sagaId).orElseThrow().version()).as("the version of the instance").isEqualTo(1L)
            );
        }

        private void assertTheStoreWouldHaveWarned(String sagaId) {
            StoredSaga control = anInstanceToSkipAnInputOf(sagaId);
            Logged skipped = logged(() -> storeOn(template, RetryStrategy.none()).compareAndSaveSkipped(control, 1, QUARANTINE_ID, new SkippedInput(0, sagaId + "@7", eventWithTwoWarnings(sagaId + "@7"))));
            assertThat(skipped.warnings()).as("the warnings of the same call when nothing fails").hasSize(2);
        }

        @Test
        void a_commit_that_fails_before_it_applies_makes_compareAndSaveSkipped_throw_and_log_no_warning_for_a_number_or_for_data_held_as_its_bytes() {
            assertTheStoreWouldHaveWarned("order-control-1");
            StoredSaga instance = anInstanceToSkipAnInputOf("order-13");
            var store = storeOn(template, RetryStrategy.none());
            long recordsBefore = records();

            Logged skipped;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", 1, BAD_VALUE)) {
                skipped = logged(() -> store.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(0, "order-13@7", eventWithTwoWarnings("order-13@7"))));
            }

            assertSkipAndItsWarningsAreNotKept("order-13", recordsBefore, skipped);
        }

        @Test
        void a_commit_that_fails_with_the_unknown_result_label_on_every_attempt_the_retry_strategy_allows_makes_compareAndSaveSkipped_throw_and_log_no_warning_and_the_record_is_not_kept() {
            assertTheStoreWouldHaveWarned("order-control-2");
            StoredSaga instance = anInstanceToSkipAnInputOf("order-14");
            var store = storeOn(template, retryingWithAHook());
            long recordsBefore = records();
            int commitsBefore = commits.get();
            int retriesBefore = retries.get();

            Logged skipped;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", ATTEMPTS, BAD_VALUE, UNKNOWN_COMMIT_RESULT)) {
                skipped = logged(() -> store.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(0, "order-14@7", eventWithTwoWarnings("order-14@7"))));
            }

            assertAll(
                    () -> assertSkipAndItsWarningsAreNotKept("order-14", recordsBefore, skipped),
                    () -> assertThat(commits.get() - commitsBefore).as("commitTransaction commands sent, the first and the store's retries").isEqualTo(ATTEMPTS),
                    () -> assertThat(retries.get() - retriesBefore).as("retries the store's strategy made").isEqualTo(ATTEMPTS - 1)
            );
        }

        @Test
        void a_commit_that_fails_before_it_applies_makes_delete_of_a_quarantined_instance_throw_and_log_no_warning_and_the_instance_and_its_records_are_still_stored() {
            var store = aQuarantinedInstanceWithTwoRecords("order-15", storeOn(template, RetryStrategy.none()));

            Logged deleted;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", 1, BAD_VALUE)) {
                deleted = logged(() -> store.delete("order-15"));
            }

            assertAll(
                    () -> assertThat(deleted.thrown()).as("what the store threw").isNotNull(),
                    () -> assertThat(deleted.warnings()).as("the warnings logged").isEmpty(),
                    () -> assertThat(store.find("order-15")).as("the instance").isPresent(),
                    () -> assertThat(records()).as("the records kept").isEqualTo(2)
            );
        }

        @Test
        void a_commit_that_fails_with_the_unknown_result_label_on_every_attempt_the_retry_strategy_allows_makes_delete_of_a_quarantined_instance_throw_and_log_no_warning_and_the_instance_and_its_records_are_still_stored() {
            var store = aQuarantinedInstanceWithTwoRecords("order-16", storeOn(template, retryingWithAHook()));
            int commitsBefore = commits.get();
            int retriesBefore = retries.get();

            Logged deleted;
            try (FailPoint failure = FailPoint.failing(client, "commitTransaction", ATTEMPTS, BAD_VALUE, UNKNOWN_COMMIT_RESULT)) {
                deleted = logged(() -> store.delete("order-16"));
            }

            assertAll(
                    () -> assertThat(commits.get() - commitsBefore).as("commitTransaction commands sent, the first and the store's retries").isEqualTo(ATTEMPTS),
                    () -> assertThat(retries.get() - retriesBefore).as("retries the store's strategy made").isEqualTo(ATTEMPTS - 1),
                    () -> assertThat(deleted.thrown()).as("what the store threw").isNotNull(),
                    () -> assertThat(deleted.warnings()).as("the warnings logged").isEmpty(),
                    () -> assertThat(store.find("order-16")).as("the instance").isPresent(),
                    () -> assertThat(records()).as("the records kept").isEqualTo(2)
            );
        }
    }

    @Nested
    class OutsideATransaction {

        @Test
        void compareAndSave_that_fails_once_is_retried_once_and_the_instance_is_saved() {
            var store = storeOn(template, retryingWithAHook());

            boolean saved;
            try (FailPoint failure = FailPoint.failing(client, "insert", 1, HOST_UNREACHABLE)) {
                saved = SagaEnvelopes.of(store, null).compareAndSave("order-7", payment("order-7"), 0);
            }

            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(sentOf("insert")).as("insert commands, the failed one and its retry").isEqualTo(2),
                    () -> assertThat(retries.get()).as("retries").isEqualTo(1)
            );
        }

        @Test
        void compareAndSave_with_a_retry_strategy_implemented_by_the_caller_is_retried_once_and_the_instance_is_saved() {
            CountingRetry strategy = new CountingRetry();
            var store = storeOn(template, strategy);

            boolean saved;
            try (FailPoint failure = FailPoint.failing(client, "insert", 1, HOST_UNREACHABLE)) {
                saved = SagaEnvelopes.of(store, null).compareAndSave("order-8", payment("order-8"), 0);
            }

            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(sentOf("insert")).as("insert commands, the failed one and its retry").isEqualTo(2),
                    () -> assertThat(strategy.attempts.get()).as("attempts").isEqualTo(2)
            );
        }
    }

    private static io.cloudevents.CloudEvent heldEvent(String key) {
        return io.cloudevents.core.builder.CloudEventBuilder.v1().withId(key).withSource(java.net.URI.create("urn:test")).withType("test").build();
    }
}
