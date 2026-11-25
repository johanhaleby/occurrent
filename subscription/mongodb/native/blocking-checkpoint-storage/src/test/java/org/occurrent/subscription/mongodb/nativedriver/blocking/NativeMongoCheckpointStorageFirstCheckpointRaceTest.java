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
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void a_candidate_that_is_not_an_operation_time_checkpoint_answers_empty_and_touches_nothing_when_another_checkpoint_is_stored_and_a_version_is_given() {
        Checkpoint stored = operationTimeCheckpoint(900);
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("not-an-operation-time"), 3);

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(2L);
    }

    @Test
    void a_newer_stored_version_refuses_a_candidate_that_is_not_an_operation_time_checkpoint_and_leaves_the_document_as_it_was() {
        storage.save(subscriptionId, new StringBasedCheckpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(5));
        Document before = storedDocument();

        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 4))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_newer_stored_version_refuses_the_same_candidate_that_is_not_an_operation_time_checkpoint_and_leaves_the_document_as_it_was() {
        storage.save(subscriptionId, new StringBasedCheckpoint("candidate"), CheckpointWriteCondition.notOlderThan(5));
        Document before = storedDocument();

        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 4))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_candidate_that_is_not_an_operation_time_checkpoint_is_stored_with_the_version_and_answered_when_nothing_is_stored() {
        Checkpoint candidate = new StringBasedCheckpoint("candidate");

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate, 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_resume_token_candidate_is_stored_with_the_version_and_answered_when_nothing_is_stored() {
        MongoResumeTokenCheckpoint candidate = resumeTokenCheckpoint();

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate, 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_checkpoint_equal_to_a_candidate_that_is_not_an_operation_time_checkpoint_gets_the_version_with_an_older_one_stored() {
        Checkpoint candidate = new StringBasedCheckpoint("candidate");
        storage.save(subscriptionId, candidate, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_checkpoint_equal_to_a_candidate_that_is_not_an_operation_time_checkpoint_gets_the_version_with_none_stored() {
        Checkpoint candidate = new StringBasedCheckpoint("candidate");
        storage.save(subscriptionId, candidate);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(candidate.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(candidate.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void a_stored_resume_token_checkpoint_equal_to_a_resume_token_candidate_gets_the_version() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, resumeTokenCheckpoint(), 3);

        assertThat(resolution).hasValueSatisfying(checkpoint -> assertThat(checkpoint.asString()).isEqualTo(stored.asString()));
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).hasValue(3L);
    }

    @Test
    void another_stored_generic_checkpoint_answers_empty_for_a_candidate_that_is_not_an_operation_time_checkpoint_and_is_left_as_it_was() {
        storage.save(subscriptionId, new StringBasedCheckpoint("stored"), CheckpointWriteCondition.notOlderThan(1));
        Document before = storedDocument();

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 3);

        assertThat(resolution).isEmpty();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_stored_resume_token_checkpoint_answers_empty_for_a_generic_candidate_and_is_left_as_it_was() {
        storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(1));
        Document before = storedDocument();

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 3);

        assertThat(resolution).isEmpty();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_stored_operation_time_answers_empty_for_a_generic_candidate_and_is_left_as_it_was_with_no_version() {
        storage.save(subscriptionId, operationTimeCheckpoint(900));
        Document before = storedDocument();

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("candidate"), 3);

        assertThat(resolution).isEmpty();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
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
    void a_stored_resume_token_checkpoint_with_no_version_answers_empty_and_stays_without_one() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored);

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEmpty();
    }

    @Test
    void a_stored_resume_token_checkpoint_with_an_older_version_answers_empty_and_keeps_that_version() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(1L);
    }

    @Test
    void a_stored_resume_token_checkpoint_with_a_version_equal_to_the_offered_one_answers_empty_and_stays_as_it_was() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        storage.save(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(3));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).isEmpty();
        assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());
        assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L);
    }

    @Test
    void a_stored_generic_checkpoint_answers_empty_and_is_left_exactly_as_it_was_with_its_version() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position"), CheckpointWriteCondition.notOlderThan(1));
        Document before = database.getCollection(COLLECTION).find(eq(ID_FIELD, subscriptionId)).first();

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500), 3);

        assertThat(resolution).isEmpty();
        assertThat(database.getCollection(COLLECTION).find(eq(ID_FIELD, subscriptionId)).first()).as("stored document").isEqualTo(before);
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
        NativeMongoCheckpointStorage retrying = new NativeMongoCheckpointStorage(database.getCollection(COLLECTION)) {
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

    private Document storedDocument() {
        return database.getCollection(COLLECTION).find(eq(ID_FIELD, subscriptionId)).first();
    }

    private static MongoResumeTokenCheckpoint resumeTokenCheckpoint() {
        // Only real delivery produces a resume-token checkpoint, so this is a subscription that has been running elsewhere
        return new MongoResumeTokenCheckpoint(new org.bson.BsonDocument("_data", new org.bson.BsonString("some-resume-token")));
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
