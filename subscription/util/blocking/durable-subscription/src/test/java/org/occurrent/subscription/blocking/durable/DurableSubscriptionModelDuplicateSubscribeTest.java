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

package org.occurrent.subscription.blocking.durable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A subscribe with the model default for an id that is already running or paused is refused with the
 * {@link DuplicateSubscriptionIdException} the wrapped model throws, and the refused call stores no start position for
 * the id. The wrapped model is {@link NativeMongoSubscriptionModel} rather than a test double, so the exception is the
 * one a caller gets from it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelDuplicateSubscribeTest {

    private static final String DATABASE = "durablesubscriptionduplicatesubscribe";
    private static final Duration STARTED_TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final Consumer<CloudEvent> action = cloudEvent -> {
    };

    private ExecutorService executor;
    private CheckpointStorage storage;
    private DurableSubscriptionModel durable;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void createModel() {
        executor = Executors.newCachedThreadPool();
        RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f);
        NativeMongoSubscriptionModel wrapped = new NativeMongoSubscriptionModel(database, database.getCollection("events-" + UUID.randomUUID()),
                TimeRepresentation.RFC_3339_STRING, executor, retryStrategy);
        storage = new InMemoryCheckpointStorage();
        durable = new DurableSubscriptionModel(wrapped, storage);
    }

    @AfterEach
    void shutDown() {
        durable.shutdown();
        executor.shutdownNow();
    }

    @Test
    void a_refused_duplicate_of_a_running_subscription_stores_no_start_position() {
        String id = UUID.randomUUID().toString();
        boolean started = durable.subscribe(id, StartAt.now(), action).waitUntilStarted(STARTED_TIMEOUT);
        assertThat(started).isTrue();
        assertThat(storage.exists(id)).as("precondition: nothing stored for the running subscription").isFalse();

        DuplicateSubscriptionIdException exception = catchThrowableOfType(DuplicateSubscriptionIdException.class, () -> durable.subscribe(id, action));

        assertThat(exception).hasMessage("Subscription " + id + " is already defined.");
        assertThat(storage.read(id)).as("the position a refused subscribe would have stored").isNull();
        assertThat(durable.isRunning(id)).isTrue();
    }

    @Test
    void a_refused_duplicate_of_a_paused_subscription_stores_no_start_position() {
        String id = UUID.randomUUID().toString();
        durable.subscribePaused(id, null, StartAt.now(), action);
        assertThat(durable.isPaused(id)).isTrue();
        assertThat(storage.exists(id)).as("precondition: nothing stored for the paused subscription").isFalse();

        DuplicateSubscriptionIdException exception = catchThrowableOfType(DuplicateSubscriptionIdException.class, () -> durable.subscribe(id, action));

        assertThat(exception).hasMessage("Subscription " + id + " is already defined.");
        assertThat(storage.read(id)).as("the position a refused subscribe would have stored").isNull();
        assertThat(durable.isPaused(id)).isTrue();
    }
}
