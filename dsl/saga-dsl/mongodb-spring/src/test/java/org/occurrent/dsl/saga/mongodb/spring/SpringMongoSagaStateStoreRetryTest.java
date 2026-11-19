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

import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInstance;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.DataAccessResourceFailureException;
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
 * Docker-based. Every test here injects a transient failure and then lets the call go through a second time, except
 * the one with an outage that never clears. A read by id fails on the wire, where the store reads through the driver
 * and not the template, with a {@code failCommand} fail point on {@code find}. Every other call fails with one
 * {@link DataAccessResourceFailureException} in a named method of a {@link FlakyMongoTemplate}, a real
 * {@code MongoTemplate}.
 * <p>
 * Tests 1 through 6 build the store with the plain, no-{@link RetryStrategy} constructor, so a test passing is itself
 * the evidence that a default retry strategy is applied without the caller asking for one. The last test builds the
 * store with the constructor that takes a {@link RetryStrategy} and passes {@link RetryStrategy#none()}, where the
 * same single failure reaches the caller instead, which is what makes the earlier tests' claim mean something.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreRetryTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = FailPoint.container();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-retry";

    private static final int HOST_UNREACHABLE = 6;

    private final AtomicInteger finds = new AtomicInteger();
    private MongoClient client;
    private String database;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-retry-" + UUID.randomUUID().toString().substring(0, 8)));
        // retryReads is off so a failed read reaches the store and the driver does not retry it first
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).retryReads(false).addCommandListener(new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("find") && event.getCommand().getString("find").getValue().equals(COLLECTION)) {
                    finds.incrementAndGet();
                }
            }
        }).build());
        database = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close() {
        client.close();
    }

    private FlakyMongoTemplate failing(String methodName, int failures) {
        return new FlakyMongoTemplate(client, database, methodName, failures);
    }

    record Payment(long amount) {
    }

    private static SagaEnvelope<Payment> payment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of(), null, NOW.minusSeconds(600),
                NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<Payment> replacementPayment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(99L), SagaStatus.ACTIVE, 2,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of(), null, NOW.minusSeconds(600),
                NOW, null, null, true, null, Map.of(), null);
    }

    private FlakyMongoTemplate healthy() {
        return failing("none", 0);
    }

    @Test
    void a_transient_failure_on_a_read_is_retried_rather_than_reaching_the_caller() {
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class);
        SagaEnvelopes.of(store, null).compareAndSave("order-1", payment("order-1"), 0);
        finds.set(0);

        SagaEnvelope<Payment> read;
        try (FailPoint failure = FailPoint.failing(client, "find", 1, HOST_UNREACHABLE)) {
            read = SagaEnvelopes.of(store, null).find("order-1").orElseThrow();
        }

        assertAll(
                () -> assertThat(read.state()).isEqualTo(new Payment(12L)),
                () -> assertThat(finds.get()).as("find commands, the failed one and its retry").isEqualTo(2)
        );
    }

    @Test
    void a_transient_failure_inserting_a_new_instance_is_retried_and_the_instance_is_still_saved() {
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing("execute", 1), COLLECTION, Payment.class);

        boolean saved = SagaEnvelopes.of(store, null).compareAndSave("order-2", payment("order-2"), 0);

        assertThat(saved).isTrue();
    }

    @Test
    void a_transient_failure_replacing_an_existing_instance_is_retried_and_the_replacement_is_still_saved() {
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class);
        SagaEnvelopes.of(setup, null).compareAndSave("order-3", payment("order-3"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing("execute", 1), COLLECTION, Payment.class);

        boolean saved = SagaEnvelopes.of(store, null).compareAndSave("order-3", replacementPayment("order-3"), 1);

        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(SagaEnvelopes.of(store, null).find("order-3").orElseThrow().state()).isEqualTo(new Payment(99L))
        );
    }

    @Test
    void a_transient_failure_finding_due_timers_is_retried_and_the_instance_is_still_found() {
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class);
        SagaEnvelopes.of(setup, null).compareAndSave("order-4", payment("order-4"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing("find", 1), COLLECTION, Payment.class);

        List<SagaEnvelope<Payment>> due = SagaEnvelopes.of(store, null).findWithDueTimers(NOW.plusSeconds(60), 10);

        assertThat(due).extracting(SagaInstance::sagaId).containsExactly("order-4");
    }

    @Test
    void a_transient_failure_deleting_an_instance_is_retried_and_the_instance_is_still_gone() {
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class);
        SagaEnvelopes.of(setup, null).compareAndSave("order-5", payment("order-5"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing("execute", 1), COLLECTION, Payment.class);

        store.delete("order-5");

        assertThat(SagaEnvelopes.of(store, null).find("order-5")).isEmpty();
    }

    @Test
    void a_transient_failure_creating_the_indexes_at_construction_is_retried_and_the_store_is_still_usable() {
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing("getCollection", 1), COLLECTION, Payment.class);

        assertThat(SagaEnvelopes.of(store, null).compareAndSave("order-6", payment("order-6"), 0)).isTrue();
    }

    @Test
    void an_outage_that_never_clears_stops_after_the_shipped_number_of_attempts_instead_of_calling_mongodb_forever() {
        // The shipped policy with its backoff swapped for a fast one, so this exercises the attempt limit the store
        // actually ships rather than a limit the test invented. The limit is what keeps a sustained outage from
        // retrying without end, since this store has no shutdown flag to stop one.
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class, null,
                SpringMongoSagaStateStore.defaultRetryStrategy().backoff(Backoff.fixed(1)));
        finds.set(0);

        Throwable thrown;
        try (FailPoint outage = FailPoint.failing(client, "find", Integer.MAX_VALUE, HOST_UNREACHABLE)) {
            thrown = catchThrowable(() -> SagaEnvelopes.of(store, null).find("order-8"));
        }

        assertAll(
                () -> assertThat(finds.get())
                        .as("the shipped default must give up after exactly %s attempts, so an outage that never clears cannot retry without end", SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS)
                        .isEqualTo(SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS),
                () -> assertThat(thrown)
                        .as("the failure that exhausted the attempts must reach the caller rather than being swallowed")
                        .isInstanceOf(DataAccessResourceFailureException.class)
        );
    }

    @Test
    void the_constructor_that_accepts_a_retry_strategy_is_honoured_and_a_transient_failure_reaches_the_caller() {
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(healthy(), COLLECTION, Payment.class, null, RetryStrategy.none());
        finds.set(0);

        Throwable thrown;
        try (FailPoint failure = FailPoint.failing(client, "find", 1, HOST_UNREACHABLE)) {
            thrown = catchThrowable(() -> SagaEnvelopes.of(store, null).find("order-7"));
        }

        assertAll(
                () -> assertThat(thrown).isInstanceOf(DataAccessResourceFailureException.class),
                () -> assertThat(finds.get()).as("find commands, with no retry").isEqualTo(1)
        );
    }
}
