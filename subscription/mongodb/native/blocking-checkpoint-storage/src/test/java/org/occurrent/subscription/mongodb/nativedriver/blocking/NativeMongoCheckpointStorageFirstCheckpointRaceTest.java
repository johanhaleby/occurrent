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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Optional;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code resolveFirstCheckpointRace} against a real replica set, the way ADR 114 and ADR 116's own pipelines were
 * checked before anything was built on them: the aggregation pipeline's {@code $cmp}-style comparison between the
 * stored {@code operationTime} and the one the candidate carries is exactly the part a fake storage cannot exercise.
 * See ADR 130.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class NativeMongoCheckpointStorageFirstCheckpointRaceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
    private static final String COLLECTION = "subscriptions";
    private static final String ID_FIELD = "_id";

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoDatabase database;
    private NativeMongoCheckpointStorage storage;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".checkpoints");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(connectionString.getDatabase());
        storage = new NativeMongoCheckpointStorage(database.getCollection(COLLECTION));
        subscriptionId = UUID.randomUUID().toString();
    }

    @AfterEach
    void disconnect() {
        mongoClient.close();
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

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString())
                .as("the earlier position governs, so nothing between it and the later stored one is skipped")
                .isEqualTo(candidate.asString());
    }

    @Test
    void a_stored_operation_time_that_is_earlier_than_the_candidate_is_left_untouched() {
        Checkpoint earlierStored = operationTimeCheckpoint(100);
        storage.save(subscriptionId, earlierStored);
        Checkpoint candidate = operationTimeCheckpoint(900);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(earlierStored.asString()));
        assertThat(storage.read(subscriptionId).asString())
                .as("already earlier than the candidate, so it stays exactly as it was")
                .isEqualTo(earlierStored.asString());
    }

    @Test
    void a_stored_operation_time_equal_to_the_candidate_is_left_untouched() {
        Checkpoint stored = operationTimeCheckpoint(500);
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500));

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
    }

    @Test
    void a_stored_resume_token_checkpoint_cannot_be_compared_and_is_left_untouched() {
        // Only real delivery produces a resume-token checkpoint, so this is the ordinary case of a subscription that
        // has been running elsewhere, which this method must never overwrite.
        MongoResumeTokenCheckpoint stored = new MongoResumeTokenCheckpoint(new org.bson.BsonDocument("_data", new org.bson.BsonString("some-resume-token")));
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500));

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
    }

    @Test
    void a_stored_generic_checkpoint_cannot_be_compared_and_is_left_untouched() {
        Checkpoint stored = new StringBasedCheckpoint("a-caller-supplied-position");
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500));

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo("a-caller-supplied-position");
    }

    @Test
    void a_candidate_that_is_not_an_operation_time_checkpoint_answers_empty_and_touches_nothing() {
        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("not-an-operation-time"));

        assertThat(resolution).isEmpty();
        assertThat(storage.exists(subscriptionId))
                .as("nothing to compare with means nothing is written either, upsert included")
                .isFalse();
    }

    @Test
    void does_not_disturb_the_write_version_a_fence_stamped() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), org.occurrent.subscription.CheckpointWriteCondition.notOlderThan(7));
        Checkpoint candidate = operationTimeCheckpoint(100);

        storage.resolveFirstCheckpointRace(subscriptionId, candidate);

        Document afterDocument = database.getCollection(COLLECTION).find(eq(ID_FIELD, subscriptionId)).first();
        assertThat(afterDocument).isNotNull();
        assertThat(afterDocument.get("version", Number.class).longValue()).isEqualTo(7L);
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
