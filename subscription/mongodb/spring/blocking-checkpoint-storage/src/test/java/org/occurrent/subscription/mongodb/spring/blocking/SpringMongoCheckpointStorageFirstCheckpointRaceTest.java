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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private SpringMongoCheckpointStorage storage;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".checkpoints");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        storage = new SpringMongoCheckpointStorage(mongoTemplate, COLLECTION);
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

    @Test
    void writes_the_candidate_with_the_version_when_nothing_is_stored() {
        Checkpoint candidate = operationTimeCheckpoint(200);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate, 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void the_candidate_replaces_a_stored_operation_time_that_is_later_and_the_version_is_stored() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(2));
        Checkpoint candidate = operationTimeCheckpoint(100);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate, 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_operation_time_that_is_earlier_than_the_candidate_stays_and_is_the_answer_and_the_version_is_stored() {
        Checkpoint earlierStored = operationTimeCheckpoint(100);
        storage.save(subscriptionId, earlierStored, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(900), 2);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(earlierStored.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(earlierStored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(2L);
    }

    @Test
    void a_newer_stored_version_refuses_the_resolution_and_leaves_position_and_version_as_they_were() {
        Checkpoint stored = operationTimeCheckpoint(900);
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(5));

        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100), 4))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(5L);
    }

    @Test
    void a_stored_resume_token_checkpoint_is_kept_and_is_the_answer_and_the_version_is_stored() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_resume_token_checkpoint_with_an_older_version_is_kept_and_the_version_is_raised() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_generic_checkpoint_is_kept_and_is_the_answer_and_the_version_is_stored() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo("a-caller-supplied-position"));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo("a-caller-supplied-position");
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_newer_stored_version_refuses_the_resolution_for_a_stored_resume_token_checkpoint_and_leaves_it_as_it_was() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(5));

        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 4))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(5L);
    }

    @Test
    void a_newer_stored_version_refuses_the_resolution_for_a_stored_generic_checkpoint_and_leaves_it_as_it_was() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position"), CheckpointWriteCondition.notOlderThan(5));

        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 4))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(subscriptionId).asString()).isEqualTo("a-caller-supplied-position");
        assertThat(storage.writeVersion(subscriptionId)).hasValue(5L);
    }

    @Test
    void retries_a_failed_resolution_with_a_version() {
        AtomicInteger calls = new AtomicInteger();
        SpringMongoCheckpointStorage retrying = new SpringMongoCheckpointStorage(mongoTemplate, COLLECTION) {
            @Override
            Document persistFirstCheckpointRaceResolution(String subscriptionId, Document candidateDocument, long writeVersion) {
                if (calls.getAndIncrement() == 0) {
                    throw new IllegalStateException("expected");
                }
                return super.persistFirstCheckpointRaceResolution(subscriptionId, candidateDocument, writeVersion);
            }
        };
        Checkpoint candidate = operationTimeCheckpoint(200);

        Optional<Checkpoint> resolution = retrying.resolveFirstCheckpointRace(subscriptionId, candidate, 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(retrying.writeVersion(subscriptionId)).hasValue(3L);
        assertThat(calls).as("one failed attempt and one that went through").hasValue(2);
    }

    private static MongoResumeTokenCheckpoint resumeTokenCheckpoint() {
        // Only real delivery produces a resume-token checkpoint, so this is a subscription that has been running elsewhere
        return new MongoResumeTokenCheckpoint(new org.bson.BsonDocument("_data", new org.bson.BsonString("some-resume-token")));
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
