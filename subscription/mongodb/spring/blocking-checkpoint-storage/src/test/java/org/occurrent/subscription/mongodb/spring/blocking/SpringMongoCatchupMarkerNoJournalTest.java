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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.internal.CatchupMarkerForgetWrites;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoCatchupMarkerNoJournalTest {

    private static final String COLLECTION = "checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    // A mongod started with --nojournal refuses every write that asks for j: true. The option is gone from 6.1, so the
    // image is pinned to a version that still has it rather than taken from the default version.
    @Container
    private static final GenericContainer<?> withoutJournaling = new GenericContainer<>("mongo:4.4.5")
            .withCommand("--nojournal")
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
            .withExposedPorts(27017)
            .waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 1));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private SpringMongoCheckpointStorage storage;

    @BeforeEach
    void open_a_store_that_retries_a_few_times_without_waiting() {
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://" + withoutJournaling.getHost() + ":" + withoutJournaling.getMappedPort(27017)))
                .addCommandListener(commands)
                .build());
        storage = open(RetryStrategy.fixed(1L).maxAttempts(5));
    }

    @AfterEach
    void close_mongo_client() {
        storage.shutdown();
        mongoClient.close();
    }

    @Test
    void forget_fails_with_the_journaling_message_and_the_refusal_as_cause_after_one_attempt_and_keeps_the_marker() {
        // Given
        long before = storage.readCatchupMarker(subscriptionId).generation();
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        commands.clear();

        // When
        Throwable thrown = catchThrowable(() -> storage.forgetCatchupMarker(subscriptionId));
        CatchupMarker afterTheFailedForget = storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> forgetWrites = commands.commands().stream()
                .filter(command -> COLLECTION.equals(command.collection()) && (command.name().equals("update") || command.name().equals("delete")))
                .toList();
        assertAll(
                () -> assertThat(thrown).as("thrown").isExactlyInstanceOf(IllegalStateException.class).hasMessage(CatchupMarkerForgetWrites.JOURNALING_REQUIRED_MESSAGE),
                () -> assertThat(thrown.getCause()).as("cause").matches(MongoCommons::isJournalNotSupported, "is the refusal of j: true"),
                () -> assertThat(forgetWrites).extracting(Command::name).as("one attempt, a refused update and no delete, %s", forgetWrites).containsExactly("update"),
                () -> assertThat(forgetWrites.get(0).document().getDocument("writeConcern").get("j")).as("j in the refused write").isEqualTo(BsonBoolean.TRUE),
                () -> assertThat(forgetWrites.get(0).reply()).as("the server refused the write, so it has no success reply, %s", forgetWrites.get(0)).isNull(),
                () -> assertThat(afterTheFailedForget.isPresent()).as("present after the failed forget").isTrue(),
                () -> assertThat(afterTheFailedForget.generation()).as("generation after the failed forget").isEqualTo(before)
        );
    }

    @Test
    void forget_under_the_default_retry_strategy_fails_at_once_instead_of_retrying_without_end() {
        // Given, the default strategy has no limit on the number of attempts, so only a refusal that is not retried ends the call
        SpringMongoCheckpointStorage withDefaultStrategy = open(null);

        try {
            // When
            Throwable thrown = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> catchThrowable(() -> withDefaultStrategy.forgetCatchupMarker(subscriptionId)));

            // Then
            assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class).hasMessage(CatchupMarkerForgetWrites.JOURNALING_REQUIRED_MESSAGE);
        } finally {
            withDefaultStrategy.shutdown();
        }
    }

    private SpringMongoCheckpointStorage open(RetryStrategy retryStrategy) {
        String database = "nojournal_" + UUID.randomUUID().toString().replace("-", "");
        MongoTemplate template = new MongoTemplate(mongoClient, database);
        return retryStrategy == null ? new SpringMongoCheckpointStorage(template, COLLECTION) : new SpringMongoCheckpointStorage(template, COLLECTION, retryStrategy);
    }
}
