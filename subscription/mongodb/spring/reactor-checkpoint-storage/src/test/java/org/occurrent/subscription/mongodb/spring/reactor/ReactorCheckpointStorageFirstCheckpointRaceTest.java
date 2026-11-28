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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonString;
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
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@code resolveFirstCheckpointRace} against a real replica set on the reactor stack. The comparison itself is the
 * shared {@code MongoCommons} pipeline, already checked in
 * {@code NativeMongoCheckpointStorageFirstCheckpointRaceTest}; this confirms the reactive plumbing around it, in
 * particular that the resolution signals empty rather than an element for a stored checkpoint it cannot compare.
 * See ADR 130.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class ReactorCheckpointStorageFirstCheckpointRaceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String CHECKPOINT_COLLECTION = "checkpoints";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoOperations mongoOperations;
    private ReactorCheckpointStorage storage;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorcheckpointrace");
        mongoClient = MongoClients.create(connectionString);
        mongoOperations = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        storage = new ReactorCheckpointStorage(mongoOperations, CHECKPOINT_COLLECTION);
        subscriptionId = UUID.randomUUID().toString();
    }

    @AfterEach
    void disconnect() {
        mongoClient.close();
    }

    @Test
    void writes_the_candidate_when_nothing_is_stored() {
        Checkpoint candidate = operationTimeCheckpoint(200);

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate).block(TIMEOUT);

        assertThat(resolution.asString()).isEqualTo(candidate.asString());
        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(candidate.asString());
    }

    @Test
    void the_candidate_replaces_a_stored_operation_time_that_is_later() {
        storage.save(subscriptionId, operationTimeCheckpoint(900)).block(TIMEOUT);
        Checkpoint candidate = operationTimeCheckpoint(100);

        storage.resolveFirstCheckpointRace(subscriptionId, candidate).block(TIMEOUT);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(candidate.asString());
    }

    @Test
    void a_stored_operation_time_that_is_earlier_than_the_candidate_is_left_untouched() {
        Checkpoint earlierStored = operationTimeCheckpoint(100);
        storage.save(subscriptionId, earlierStored).block(TIMEOUT);

        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(900)).block(TIMEOUT);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(earlierStored.asString());
    }

    @Test
    void a_stored_generic_checkpoint_cannot_be_compared_and_is_left_untouched() {
        storage.save(subscriptionId, new StringBasedCheckpoint("a-caller-supplied-position")).block(TIMEOUT);

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500)).blockOptional(TIMEOUT).orElse(null);

        assertThat(resolution).isNull();
        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo("a-caller-supplied-position");
    }

    @Test
    void a_candidate_that_is_not_an_operation_time_checkpoint_signals_empty_and_touches_nothing() {
        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, new StringBasedCheckpoint("not-an-operation-time"))
                .blockOptional(TIMEOUT).orElse(null);

        assertThat(resolution).isNull();
        assertThat(storage.read(subscriptionId).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void a_resume_token_save_from_a_later_position_is_refused_once_a_start_position_moved_the_stored_position_earlier() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        assertThatThrownBy(() -> storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT))
                .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class,
                        refusal -> assertThat(refusal.storedVersion).as("stored version of the refusal").hasValue(3L));

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).as("stored checkpoint").isEqualTo(operationTimeCheckpoint(100).asString());
        assertThat(storage.writeVersion(subscriptionId).block(TIMEOUT)).as("stored version").isEqualTo(3L);
    }

    @Test
    void an_operation_time_save_from_a_later_position_is_refused_once_a_start_position_moved_the_stored_position_earlier() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        assertThatThrownBy(() -> storage.save(subscriptionId, operationTimeCheckpoint(950), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).as("stored checkpoint").isEqualTo(operationTimeCheckpoint(100).asString());
    }

    @Test
    void a_generic_checkpoint_save_is_not_refused_once_a_start_position_moved_the_stored_position_earlier() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        storage.save(subscriptionId, new StringBasedCheckpoint("generic"), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo("generic");
    }

    @Test
    void a_write_that_goes_through_takes_the_refusal_away() {
        storage.save(subscriptionId, operationTimeCheckpoint(900), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        storage.save(subscriptionId, operationTimeCheckpoint(100), CheckpointWriteCondition.any()).block(TIMEOUT);
        Throwable thrown = catchThrowable(() -> storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT));

        assertThat(thrown).as("thrown by a save from the later position once a write went through").isNull();
        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).as("stored checkpoint").isEqualTo(resumeTokenCheckpoint().asString());
    }

    @Test
    void a_resolution_that_does_not_move_the_stored_position_earlier_leaves_a_save_from_a_later_position_allowed() {
        storage.save(subscriptionId, operationTimeCheckpoint(100), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(900)).block(TIMEOUT);

        storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(resumeTokenCheckpoint().asString());
    }

    @Test
    void a_resolution_that_writes_into_an_empty_subscription_leaves_a_save_from_a_later_position_allowed() {
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(3)).block(TIMEOUT);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(resumeTokenCheckpoint().asString());
    }

    @Test
    void a_write_of_an_operation_time_over_nothing_stored_sets_the_lineage_origin_to_it() {
        storage.save(subscriptionId, operationTimeCheckpoint(300)).block(TIMEOUT);

        assertThat(storedDocument().get(MongoCommons.LINEAGE_ORIGIN)).isEqualTo(new BsonTimestamp(300, 0));
    }

    @Test
    void a_write_of_a_resume_token_over_nothing_stored_sets_no_lineage_origin() {
        storage.save(subscriptionId, resumeTokenCheckpoint()).block(TIMEOUT);

        assertThat(storedDocument()).doesNotContainKey(MongoCommons.LINEAGE_ORIGIN);
    }

    @Test
    void a_not_older_than_write_of_a_resume_token_over_an_operation_time_keeps_the_lineage_origin() {
        storage.save(subscriptionId, operationTimeCheckpoint(300), CheckpointWriteCondition.notOlderThan(1)).block(TIMEOUT);

        storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(2)).block(TIMEOUT);

        assertThat(storedDocument().get(MongoCommons.LINEAGE_ORIGIN)).isEqualTo(new BsonTimestamp(300, 0));
    }

    @Test
    void a_stored_resume_token_whose_lineage_began_later_than_the_candidate_is_replaced_by_it_and_marked() {
        storage.save(subscriptionId, operationTimeCheckpoint(900)).block(TIMEOUT);
        storage.save(subscriptionId, resumeTokenCheckpoint()).block(TIMEOUT);
        Checkpoint candidate = operationTimeCheckpoint(100);

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, candidate).block(TIMEOUT);

        assertThat(resolution.asString()).isEqualTo(candidate.asString());
        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString())
                .as("the earlier position governs, so the events between it and the token's line of delivery are not skipped")
                .isEqualTo(candidate.asString());
        assertThat(storedDocument().get(MongoCommons.LINEAGE_ORIGIN)).isEqualTo(new BsonTimestamp(100, 0));
        assertThat(storedDocument().get(MongoCommons.POSITION_MOVED_EARLIER)).isEqualTo(true);
    }

    @Test
    void a_save_of_a_resume_token_is_refused_after_a_resolution_replaced_a_stored_resume_token() {
        storage.save(subscriptionId, operationTimeCheckpoint(900)).block(TIMEOUT);
        storage.save(subscriptionId, resumeTokenCheckpoint()).block(TIMEOUT);
        storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);

        assertThatThrownBy(() -> storage.save(subscriptionId, resumeTokenCheckpoint(), CheckpointWriteCondition.notOlderThan(1)).block(TIMEOUT))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(operationTimeCheckpoint(100).asString());
    }

    @Test
    void a_stored_resume_token_whose_lineage_began_earlier_than_the_candidate_is_kept_and_signals_empty() {
        storage.save(subscriptionId, operationTimeCheckpoint(100)).block(TIMEOUT);
        storage.save(subscriptionId, resumeTokenCheckpoint()).block(TIMEOUT);
        Document before = storedDocument();

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(900)).blockOptional(TIMEOUT).orElse(null);

        assertThat(resolution).isNull();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_stored_resume_token_whose_lineage_began_at_the_candidate_is_kept_and_signals_empty() {
        storage.save(subscriptionId, operationTimeCheckpoint(500)).block(TIMEOUT);
        storage.save(subscriptionId, resumeTokenCheckpoint()).block(TIMEOUT);
        Document before = storedDocument();

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(500)).blockOptional(TIMEOUT).orElse(null);

        assertThat(resolution).isNull();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
    }

    @Test
    void a_stored_resume_token_without_a_lineage_origin_as_a_version_before_0_35_stored_it_is_kept_and_signals_empty() {
        MongoResumeTokenCheckpoint stored = resumeTokenCheckpoint();
        mongoOperations.insert(MongoCommons.generateCheckpointDocument(subscriptionId, stored), CHECKPOINT_COLLECTION).block(TIMEOUT);
        Document before = storedDocument();

        Checkpoint resolution = storage.resolveFirstCheckpointRace(subscriptionId, operationTimeCheckpoint(100)).blockOptional(TIMEOUT).orElse(null);

        assertThat(before).doesNotContainKey(MongoCommons.LINEAGE_ORIGIN);
        assertThat(resolution).isNull();
        assertThat(storedDocument()).as("stored document").isEqualTo(before);
        assertThat(storage.read(subscriptionId).block(TIMEOUT).asString()).isEqualTo(stored.asString());
    }

    private Document storedDocument() {
        return mongoOperations.findById(subscriptionId, Document.class, CHECKPOINT_COLLECTION).block(TIMEOUT);
    }

    private static MongoResumeTokenCheckpoint resumeTokenCheckpoint() {
        return new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("some-resume-token")));
    }

    private static Checkpoint operationTimeCheckpoint(int seconds) {
        return new MongoOperationTimeCheckpoint(new BsonTimestamp(seconds, 0));
    }
}
