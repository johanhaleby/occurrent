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
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code resolveFirstCheckpointRace} reached through {@link MongoTemplate}, the one path
 * {@link SpringMongoCheckpointStorage} takes that {@code NativeMongoCheckpointStorageFirstCheckpointRaceTest}
 * cannot exercise, since it goes through the underlying driver collection rather than {@code MongoOperations}
 * directly (see {@link SpringMongoCheckpointStorage#persistFirstCheckpointRaceResolution}). The comparison itself
 * is the shared {@code MongoCommons} pipeline, already checked there. See ADR 130.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class SpringMongoCheckpointStorageFirstCheckpointRaceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
    private static final String COLLECTION = "subscriptions";

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoTemplate mongoTemplate;
    private SpringMongoCheckpointStorage storage;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".checkpoints");
        mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(connectionString));
        storage = new SpringMongoCheckpointStorage(mongoTemplate, COLLECTION);
        subscriptionId = UUID.randomUUID().toString();
    }

    @AfterEach
    void disconnect() {
        mongoTemplate.getMongoDatabaseFactory().getMongoDatabase().drop();
    }

    @Test
    void writes_the_candidate_when_nothing_is_stored() {
        Checkpoint candidate = operationTimeCheckpoint(200);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
    }

    @Test
    void the_candidate_replaces_a_stored_operation_time_that_is_later() {
        storage.save(subscriptionId, operationTimeCheckpoint(900));
        Checkpoint candidate = operationTimeCheckpoint(100);

        storage.resolveFirstCheckpointRace(subscriptionId, candidate);

        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
    }

    @Test
    void a_stored_operation_time_that_is_earlier_than_the_candidate_is_left_untouched() {
        Checkpoint earlierStored = operationTimeCheckpoint(100);
        storage.save(subscriptionId, earlierStored);

        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(900));

        assertThat(storage.read(subscriptionId).asString()).isEqualTo(earlierStored.asString());
    }

    @Test
    void a_stored_generic_checkpoint_cannot_be_compared_and_is_left_untouched() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500));

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo("a-caller-supplied-position");
    }

    @Test
    void a_candidate_that_is_not_an_operation_time_checkpoint_answers_empty_and_touches_nothing() {
        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("not-an-operation-time"));

        assertThat(resolution).isEmpty();
        assertThat(storage.exists(subscriptionId)).isFalse();
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
