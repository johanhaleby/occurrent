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
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, for the store's construction only. A MongoDB without journaling refuses a skipped-input write with
 * {@code j: true} on every attempt, so a store with the default retry strategy must not retry it, and it tells the
 * caller what the server needs. The server here has journaling, so a {@link MongoOperations} that refuses the
 * skipped-input write the way such a server does stands in for one without.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreJournalingRefusalTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-journaling-refusal";
    private static final String SKIPPED_INPUTS_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String NO_JOURNALING = "cannot use 'j' option when a host does not have journaling enabled";

    record Payment(long amount) {
    }

    private static MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-journaling-" + UUID.randomUUID().toString().substring(0, 8)));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    // The default strategy of the store with a fast backoff, so it keeps the attempt limit it ships with
    private static RetryStrategy defaultStrategyWithAFastBackoff() {
        return SpringMongoSagaStateStore.defaultRetryStrategy().backoff(Backoff.fixed(1));
    }

    private static MongoCommandException refusal(int code, String message) {
        return new MongoCommandException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(code)).append("errmsg", new BsonString(message)), new ServerAddress());
    }

    /** Wraps {@code real}, refusing every {@code execute} on the skipped-inputs collection with {@code refusal}, translated as Spring does, and counting the attempts. */
    private static MongoOperations refusingSkippedInputWrites(MongoOperations real, MongoCommandException refusal, AtomicInteger attempts) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals("execute") && SKIPPED_INPUTS_COLLECTION.equals(args[0])) {
                attempts.incrementAndGet();
                throw new UncategorizedMongoDbException(refusal.getMessage(), refusal);
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(), new Class<?>[]{MongoOperations.class}, handler);
    }

    private static Throwable recordASkippedInputOnAStoreRefusingWith(MongoCommandException refusal, AtomicInteger attempts) {
        SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(refusingSkippedInputWrites(mongoOperations(), refusal, attempts), COLLECTION, Payment.class, null, defaultStrategyWithAFastBackoff());
        return catchThrowable(() -> store.recordSkippedInput("order-1", "q-1", new SkippedInput(0, "order-1@7")));
    }

    @Test
    void a_server_without_journaling_is_refused_once_with_the_message_that_names_what_it_needs() {
        AtomicInteger attempts = new AtomicInteger();

        Throwable thrown = recordASkippedInputOnAStoreRefusingWith(refusal(2, NO_JOURNALING), attempts);

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage(SpringMongoSagaStateStore.JOURNALING_REQUIRED_MESSAGE),
                () -> assertThat(attempts).as("attempts").hasValue(1)
        );
    }

    @Test
    void another_refusal_with_the_same_code_is_retried_up_to_the_attempt_limit() {
        AtomicInteger attempts = new AtomicInteger();

        Throwable thrown = recordASkippedInputOnAStoreRefusingWith(refusal(2, "a bad value"), attempts);

        assertAll(
                () -> assertThat(thrown).isInstanceOf(UncategorizedMongoDbException.class),
                () -> assertThat(attempts).as("attempts").hasValue(SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS)
        );
    }

    @Test
    void the_journaling_message_under_another_code_is_retried_up_to_the_attempt_limit() {
        AtomicInteger attempts = new AtomicInteger();

        Throwable thrown = recordASkippedInputOnAStoreRefusingWith(refusal(3, NO_JOURNALING), attempts);

        assertAll(
                () -> assertThat(thrown).isInstanceOf(UncategorizedMongoDbException.class),
                () -> assertThat(attempts).as("attempts").hasValue(SpringMongoSagaStateStore.DEFAULT_MAX_ATTEMPTS)
        );
    }
}
