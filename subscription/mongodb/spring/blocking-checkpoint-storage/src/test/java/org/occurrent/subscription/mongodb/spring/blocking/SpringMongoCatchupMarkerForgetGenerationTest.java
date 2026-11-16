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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.Document;
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
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoCatchupMarkerForgetGenerationTest {

    private static final String COLLECTION = "checkpoints";
    private static final String APPLICATION_NAME = "marker-forget-generation";
    private static final int WRITE_CONCERN_FAILED = 64;
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    // enableTestCommands lets the server accept configureFailPoint. withCommand replaces the whole command line, so
    // the replica set name is repeated
    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private SpringMongoCheckpointStorage storage;

    @BeforeEach
    void open_a_store_that_retries_a_few_times_without_waiting() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("springmarkerforgetgeneration"));
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applicationName(APPLICATION_NAME)
                .writeConcern(WriteConcern.MAJORITY)
                .addCommandListener(commands)
                .build());
        String database = requireNonNull(connectionString.getDatabase());
        collection = mongoClient.getDatabase(database).getCollection(COLLECTION);
        storage = new SpringMongoCheckpointStorage(new MongoTemplate(mongoClient, database), COLLECTION, RetryStrategy.fixed(1L).maxAttempts(3));
    }

    @AfterEach
    void turn_off_the_fail_point_and_close_mongo_client() {
        configureFailPoint(new Document("mode", "off"));
        mongoClient.close();
    }

    @Test
    void a_late_save_stamped_with_a_generation_that_a_rollback_took_back_reads_absent_after_the_forget_is_retried() {
        // Given, a forget that an attempt read the generation of and that a rollback then took back
        Document beforeTheFirstForget = generationDocument();
        storage.forgetCatchupMarker(subscriptionId);
        long readByTheAttempt = storage.readCatchupMarker(subscriptionId).generation();
        restore(beforeTheFirstForget);

        // When, the retry and then the late save of the attempt
        storage.forgetCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, readByTheAttempt, CheckpointWriteCondition.any());
        CatchupMarker afterTheLateSave = storage.readCatchupMarker(subscriptionId);

        // Then
        assertAll(
                () -> assertThat(afterTheLateSave.isPresent()).as("present after the late save").isFalse(),
                () -> assertThat(afterTheLateSave.generation()).as("generation after the retry").isNotEqualTo(readByTheAttempt)
        );
    }

    @Test
    void every_attempt_of_one_forget_sets_its_own_generation_other_than_0() {
        // Given, a first attempt that the server applies and then reports as failed, so the call retries
        failNextUpdate(new Document("writeConcernError", new Document("code", WRITE_CONCERN_FAILED)
                .append("errmsg", "waiting for replication timed out")
                .append("errInfo", new Document("wtimeout", true))));
        commands.clear();

        // When
        storage.forgetCatchupMarker(subscriptionId);

        // Then
        List<Command> updates = commands.commands("update", COLLECTION);
        assertThat(updates).as("%s", updates).hasSize(2);
        List<BsonValue> generationsSent = updates.stream().map(SpringMongoCatchupMarkerForgetGenerationTest::generationSet).toList();
        assertAll(
                () -> assertThat(generationsSent).as("generations sent, one per attempt, %s", updates).doesNotHaveDuplicates().doesNotContain(new BsonInt64(0)),
                () -> assertThat(new BsonInt64(storage.readCatchupMarker(subscriptionId).generation())).as("generation stored").isEqualTo(generationsSent.get(1))
        );
    }

    // What the first update sets as the generation, null when it does not set one
    private static BsonValue generationSet(Command update) {
        BsonDocument operators = update.document().getArray("updates").get(0).asDocument().getDocument("u");
        return operators.containsKey("$set") ? operators.getDocument("$set").get("generation", BsonNull.VALUE) : BsonNull.VALUE;
    }

    private Document generationDocument() {
        return collection.find(eq("_id", new Document("catchupMarker", subscriptionId))).first();
    }

    // Puts the generation document back as it was, which is what a rollback of the forget does
    private void restore(Document before) {
        if (before == null) {
            collection.deleteOne(eq("_id", new Document("catchupMarker", subscriptionId)));
        } else {
            collection.replaceOne(eq("_id", before.get("_id")), before, new ReplaceOptions().upsert(true));
        }
    }

    private void failNextUpdate(Document failure) {
        Document data = new Document("failCommands", List.of("update")).append("appName", APPLICATION_NAME);
        data.putAll(failure);
        configureFailPoint(new Document("mode", new Document("times", 1)).append("data", data));
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        mongoClient.getDatabase("admin").runCommand(command);
    }
}
