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
import com.mongodb.client.MongoClients;
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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
 * Docker-based. Every test here injects exactly one transient {@link DataAccessResourceFailureException} into a named
 * {@link MongoOperations} method through a {@link Proxy}, then lets the call go through a second time.
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
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-retry";

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-retry-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    record Payment(long amount) {
    }

    private static SagaEnvelope<Payment> payment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, 1,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of(), null, NOW.minusSeconds(600),
                NOW, null, null, true, null);
    }

    private static SagaEnvelope<Payment> replacementPayment(String sagaId) {
        return new SagaEnvelope<>(sagaId, new Payment(99L), SagaStatus.ACTIVE, 2,
                List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of(), null, NOW.minusSeconds(600),
                NOW, null, null, true, null);
    }

    /** Wraps {@code real}, throwing a {@link DataAccessResourceFailureException} on the first {@code failures} calls to the method named {@code methodName} and delegating every other call and every other method unchanged. */
    private static MongoOperations failing(MongoOperations real, String methodName, int failures) {
        AtomicInteger remaining = new AtomicInteger(failures);
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals(methodName) && remaining.getAndDecrement() > 0) {
                throw new DataAccessResourceFailureException("simulated transient MongoDB failure in " + methodName);
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(),
                new Class<?>[]{MongoOperations.class}, handler);
    }

    /** Wraps {@code real}, throwing on every call to {@code methodName} and counting how many of them reached it. */
    private static MongoOperations alwaysFailing(MongoOperations real, String methodName, AtomicInteger calls) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals(methodName)) {
                calls.incrementAndGet();
                throw new DataAccessResourceFailureException("simulated sustained MongoDB outage in " + methodName);
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(),
                new Class<?>[]{MongoOperations.class}, handler);
    }

    @Test
    void a_transient_failure_on_a_read_is_retried_rather_than_reaching_the_caller() {
        MongoOperations mongoOperations = failing(mongoOperations(), "findById", 1);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
        store.compareAndSave("order-1", payment("order-1"), 0);

        SagaEnvelope<Payment> read = store.find("order-1").orElseThrow();

        assertThat(read.state()).isEqualTo(new Payment(12L));
    }

    @Test
    void a_transient_failure_inserting_a_new_instance_is_retried_and_the_instance_is_still_saved() {
        MongoOperations mongoOperations = failing(mongoOperations(), "insert", 1);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);

        boolean saved = store.compareAndSave("order-2", payment("order-2"), 0);

        assertThat(saved).isTrue();
    }

    @Test
    void a_transient_failure_replacing_an_existing_instance_is_retried_and_the_replacement_is_still_saved() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
        setup.compareAndSave("order-3", payment("order-3"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing(mongoOperations, "findAndReplace", 1), COLLECTION, Payment.class);

        boolean saved = store.compareAndSave("order-3", replacementPayment("order-3"), 1);

        assertThat(saved).isTrue();
    }

    @Test
    void a_transient_failure_finding_due_timers_is_retried_and_the_instance_is_still_found() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
        setup.compareAndSave("order-4", payment("order-4"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing(mongoOperations, "find", 1), COLLECTION, Payment.class);

        List<SagaEnvelope<Payment>> due = store.findWithDueTimers(NOW.plusSeconds(60), 10);

        assertThat(due).extracting(SagaInstance::sagaId).containsExactly("order-4");
    }

    @Test
    void a_transient_failure_deleting_an_instance_is_retried_and_the_instance_is_still_gone() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<Payment> setup = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);
        setup.compareAndSave("order-5", payment("order-5"), 0);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(failing(mongoOperations, "remove", 1), COLLECTION, Payment.class);

        store.delete("order-5");

        assertThat(store.find("order-5")).isEmpty();
    }

    @Test
    void a_transient_failure_creating_the_indexes_at_construction_is_retried_and_the_store_is_still_usable() {
        MongoOperations mongoOperations = failing(mongoOperations(), "getCollection", 1);

        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class);

        assertThat(store.compareAndSave("order-6", payment("order-6"), 0)).isTrue();
    }

    @Test
    void an_outage_that_never_clears_stops_after_the_shipped_number_of_attempts_instead_of_calling_mongodb_forever() {
        // The shipped policy with its backoff swapped for a fast one, so this exercises the attempt limit the store
        // actually ships rather than a limit the test invented. The limit is what keeps a sustained outage from
        // retrying without end, since this store has no shutdown flag to stop one.
        AtomicInteger calls = new AtomicInteger();
        MongoOperations mongoOperations = alwaysFailing(mongoOperations(), "findById", calls);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class, null,
                SpringMongoSagaStateStore.defaultRetryStrategy().backoff(Backoff.fixed(1)));

        Throwable thrown = catchThrowable(() -> store.find("order-8"));

        assertAll(
                () -> assertThat(calls)
                        .as("the shipped default must give up after exactly %s attempts, so an outage that never clears cannot retry without end", SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS)
                        .hasValue(SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS),
                () -> assertThat(thrown)
                        .as("the failure that exhausted the attempts must reach the caller rather than being swallowed")
                        .isInstanceOf(DataAccessResourceFailureException.class)
        );
    }

    @Test
    void the_constructor_that_accepts_a_retry_strategy_is_honoured_and_a_transient_failure_reaches_the_caller() {
        MongoOperations mongoOperations = failing(mongoOperations(), "findById", 1);
        SagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, Payment.class, null, RetryStrategy.none());

        Throwable thrown = catchThrowable(() -> store.find("order-7"));

        assertThat(thrown).isInstanceOf(DataAccessResourceFailureException.class);
    }
}
