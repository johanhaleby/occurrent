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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Proves that {@link ReactorCheckpointStorage} survives a transient MongoDB error while reading, saving or deleting
 * a checkpoint, the same class of failure {@code SpringMongoCheckpointStorage} already retries on the blocking
 * stack (see #656).
 * <p>
 * The fault is injected as a first-subscription failure on a deferred {@link Mono}, not as a second invocation of
 * the mocked method. {@link ReactorCheckpointStorage} calls {@code mongo.upsert(...)}, {@code findOne(...)} and
 * {@code remove(...)} exactly once per operation, and retries by resubscribing to the {@link Mono} that call
 * returns. A fault that only fails a later method invocation would never be reached by the retry.
 */
@Testcontainers
@Timeout(20)
class ReactorCheckpointStorageResilienceTest {

    private static final String CHECKPOINT_COLLECTION = "checkpoints";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoOperations realMongoOperations;

    @BeforeEach
    void createRealMongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorcheckpointresilience");
        mongoClient = MongoClients.create(connectionString);
        realMongoOperations = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
    }

    @AfterEach
    void shutdown() {
        mongoClient.close();
    }

    /**
     * Wraps {@code realMongoOperations} so that the first subscription to the {@code Mono} returned by
     * {@code findOne}, {@code upsert} or {@code remove} fails with {@code exception}, while every later
     * subscription (a retry) behaves exactly like the real operations. {@code attempts} exposes how many times each
     * operation was actually subscribed to.
     */
    @SuppressWarnings("unchecked")
    private ReactiveMongoOperations operationsThatFailFirstSubscription(RuntimeException exception, Attempts attempts) {
        ReactiveMongoOperations throwingOperations = mock(ReactiveMongoOperations.class);
        when(throwingOperations.findOne(any(Query.class), eq(Document.class), eq(CHECKPOINT_COLLECTION)))
                .thenAnswer(invocation -> Mono.defer(() -> attempts.findOne.getAndIncrement() == 0
                        ? Mono.error(exception)
                        : realMongoOperations.findOne(invocation.getArgument(0), Document.class, CHECKPOINT_COLLECTION)));
        when(throwingOperations.upsert(any(Query.class), any(UpdateDefinition.class), eq(CHECKPOINT_COLLECTION)))
                .thenAnswer(invocation -> Mono.defer(() -> attempts.upsert.getAndIncrement() == 0
                        ? Mono.error(exception)
                        : realMongoOperations.upsert(invocation.getArgument(0), invocation.getArgument(1), CHECKPOINT_COLLECTION)));
        when(throwingOperations.remove(any(Query.class), eq(CHECKPOINT_COLLECTION)))
                .thenAnswer(invocation -> Mono.defer(() -> attempts.remove.getAndIncrement() == 0
                        ? Mono.error(exception)
                        : realMongoOperations.remove((Query) invocation.getArgument(0), CHECKPOINT_COLLECTION)));
        return throwingOperations;
    }

    private static final class Attempts {
        private final AtomicInteger findOne = new AtomicInteger();
        private final AtomicInteger upsert = new AtomicInteger();
        private final AtomicInteger remove = new AtomicInteger();
    }

    /**
     * Simulates the class of error a driver surfaces during a replica-set primary election or failover. A
     * socket-level read fails until a new primary is elected. Mirrors
     * {@code ReactorMongoSubscriptionModelResilienceTest}'s equivalent injection.
     */
    private static MongoSocketReadException failoverLikeException() {
        return new MongoSocketReadException("expected: simulated primary election/failover", new ServerAddress(), new java.io.IOException("Connection reset by peer"));
    }

    private static void insertExistingCheckpoint(ReactiveMongoOperations mongo, String subscriptionId, String value) {
        mongo.upsert(new Query(Criteria.where("_id").is(subscriptionId)), Update.update("checkpoint", value), CHECKPOINT_COLLECTION).block();
    }

    @Nested
    @DisplayName("default retry")
    class DefaultRetryTest {

        @Test
        void save_retries_a_transient_error_and_succeeds() {
            // Given
            Attempts attempts = new Attempts();
            ReactiveMongoOperations throwingOperations = operationsThatFailFirstSubscription(failoverLikeException(), attempts);
            ReactorCheckpointStorage storage = new ReactorCheckpointStorage(throwingOperations, CHECKPOINT_COLLECTION);
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Checkpoint saved = storage.save(subscriptionId, new StringBasedCheckpoint("first-value")).block();

            // Then
            assertThat(saved).isEqualTo(new StringBasedCheckpoint("first-value"));
            assertThat(attempts.upsert.get()).isEqualTo(2);
        }

        @Test
        void read_retries_a_transient_error_and_succeeds() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            insertExistingCheckpoint(realMongoOperations, subscriptionId, "existing-value");
            Attempts attempts = new Attempts();
            ReactiveMongoOperations throwingOperations = operationsThatFailFirstSubscription(failoverLikeException(), attempts);
            ReactorCheckpointStorage storage = new ReactorCheckpointStorage(throwingOperations, CHECKPOINT_COLLECTION);

            // When
            Checkpoint checkpoint = storage.read(subscriptionId).block();

            // Then
            assertThat(checkpoint).isEqualTo(new StringBasedCheckpoint("existing-value"));
            assertThat(attempts.findOne.get()).isEqualTo(2);
        }

        @Test
        void delete_retries_a_transient_error_and_succeeds() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            insertExistingCheckpoint(realMongoOperations, subscriptionId, "existing-value");
            Attempts attempts = new Attempts();
            ReactiveMongoOperations throwingOperations = operationsThatFailFirstSubscription(failoverLikeException(), attempts);
            ReactorCheckpointStorage storage = new ReactorCheckpointStorage(throwingOperations, CHECKPOINT_COLLECTION);

            // When
            storage.delete(subscriptionId).block();

            // Then
            assertThat(realMongoOperations.exists(new Query(Criteria.where("_id").is(subscriptionId)), CHECKPOINT_COLLECTION).block()).isFalse();
            assertThat(attempts.remove.get()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("custom retry")
    class CustomRetryTest {

        @Test
        void a_retry_that_permits_no_attempts_lets_the_transient_error_propagate() {
            // Given: proves the operation only succeeds because of the retry, not on its own, and that a caller
            // can dial retrying down.
            Attempts attempts = new Attempts();
            ReactiveMongoOperations throwingOperations = operationsThatFailFirstSubscription(failoverLikeException(), attempts);
            ReactorCheckpointStorage storage = new ReactorCheckpointStorage(throwingOperations, CHECKPOINT_COLLECTION, Retry.max(0));
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Throwable thrown = catchThrowable(() -> storage.save(subscriptionId, new StringBasedCheckpoint("value")).block());

            // Then
            assertThat(thrown).hasCauseInstanceOf(MongoSocketReadException.class);
            assertThat(attempts.upsert.get()).isEqualTo(1);
        }
    }
}
