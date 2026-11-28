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
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code storeWriteVersion} against a MongoDB replica set in a container. The {@code findOneAndUpdate} without upsert and the
 * conditional {@code $replaceWith} it runs are the part a fake storage cannot exercise.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class NativeMongoCheckpointStorageStoreWriteVersionTest {

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
    void answers_empty_and_writes_nothing_when_nothing_is_stored() {
        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertThat(answer).as("answer").isEmpty();
        assertThat(storedDocument()).as("stored document, no upsert").isNull();
        assertThat(storage.read(subscriptionId)).as("stored position").isNull();
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEmpty();
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_an_operation_time_checkpoint_with_no_version_is_stored() {
        Checkpoint stored = operationTimeCheckpoint(500);
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertThat(answer).as("answer").hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L);
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_a_checkpoint_with_an_older_version_is_stored() {
        Checkpoint stored = operationTimeCheckpoint(500);
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertThat(answer).as("answer").hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L);
    }

    @Test
    void answers_the_checkpoint_and_keeps_the_version_when_a_checkpoint_with_the_same_version_is_stored() {
        Checkpoint stored = operationTimeCheckpoint(500);
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 2);

        assertThat(answer).as("answer").hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L);
    }

    @Test
    void keeps_a_stored_resume_token_checkpoint_and_stamps_it() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 3);

        assertThat(answer).as("answer").hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L);
    }

    @Test
    void keeps_a_stored_generic_checkpoint_and_stamps_it() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position"));

        Optional<Checkpoint> answer = storage.storeWriteVersion(subscriptionId, 3);

        assertThat(answer).as("answer").hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo("a-caller-supplied-position"));
        assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("a-caller-supplied-position");
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L);
    }

    @Test
    void refuses_a_version_older_than_the_stored_one_with_that_version_and_leaves_the_document_as_it_was() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(5));
        Document before = storedDocument();

        assertThatThrownBy(() -> storage.storeWriteVersion(subscriptionId, 4))
                .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> {
                    assertThat(e.storedVersion).as("stored version in the exception").hasValue(5L);
                    assertThat(e.condition).as("condition in the exception").isEqualTo(CheckpointWriteCondition.notOlderThan(4));
                });

        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_negative_version_is_refused_and_leaves_the_document_as_it_was() {
        storage.save(subscriptionId, operationTimeCheckpoint(900));
        Document before = storedDocument();

        assertThatThrownBy(() -> storage.storeWriteVersion(subscriptionId, -1)).isInstanceOf(IllegalArgumentException.class);

        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_negative_version_is_refused_by_the_three_argument_resolveFirstCheckpointRace_and_stores_nothing() {
        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(200), -1))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(storedDocument()).as("stored document").isNull();
    }

    private Document storedDocument() {
        return database.getCollection(COLLECTION).find(eq(ID_FIELD, subscriptionId)).first();
    }

    private static MongoResumeTokenCheckpoint resumeTokenCheckpoint() {
        return new MongoResumeTokenCheckpoint(new org.bson.BsonDocument("_data", new org.bson.BsonString("some-resume-token")));
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
