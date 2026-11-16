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
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerCommandsTest {

    private static final String COLLECTION = "checkpoints";
    private static final long TEN_SECONDS_IN_MILLIS = 10_000;
    // A direct connection to a replica set member sends a primary read as primaryPreferred
    private static final List<String> PRIMARY_MODES = List.of("primary", "primaryPreferred");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private NativeMongoCheckpointStorage storage;

    // w:1 and secondaryPreferred, so every majority write concern and every primary read in a command came from the store
    @BeforeEach
    void open_a_store_on_a_collection_at_w_1_that_prefers_secondaries() {
        openStore(settings -> {
        });
    }

    private void openStore(Consumer<MongoClientSettings.Builder> customizer) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("nativemarkercommands"));
        MongoClientSettings.Builder settings = MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(commands);
        customizer.accept(settings);
        mongoClient = MongoClients.create(settings.build());
        MongoCollection<Document> collection = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection(COLLECTION)
                .withWriteConcern(WriteConcern.W1)
                .withReadPreference(ReadPreference.secondaryPreferred());
        storage = new NativeMongoCheckpointStorage(collection);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void read_finds_the_marker_at_majority_then_the_generation_linearizable_both_with_a_time_limit_on_the_primary() {
        // When
        storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> finds = commands.commands("find", COLLECTION);
        assertThat(finds).as("%s", finds).hasSize(2);
        Command markerRead = finds.get(0);
        Command generationRead = finds.get(1);
        assertAll(
                () -> assertThat(markerRead.document().getDocument("filter").get("_id")).as("%s", markerRead).isEqualTo(new BsonString(subscriptionId)),
                () -> assertThat(markerRead.readConcernLevel()).as("%s", markerRead).isEqualTo("majority"),
                () -> assertThat(markerRead.document().get("maxTimeMS")).as("%s", markerRead).isIn(new BsonInt32((int) TEN_SECONDS_IN_MILLIS), new BsonInt64(TEN_SECONDS_IN_MILLIS)),
                () -> assertThat(readPreferenceMode(markerRead)).as("%s", markerRead).isIn(PRIMARY_MODES),
                () -> assertThat(generationRead.document().getDocument("filter").get("_id")).as("%s", generationRead).isEqualTo(generationId()),
                () -> assertThat(generationRead.readConcernLevel()).as("%s", generationRead).isEqualTo("linearizable"),
                () -> assertThat(generationRead.document().get("maxTimeMS")).as("%s", generationRead).isIn(new BsonInt32((int) TEN_SECONDS_IN_MILLIS), new BsonInt64(TEN_SECONDS_IN_MILLIS)),
                () -> assertThat(readPreferenceMode(generationRead)).as("%s", generationRead).isIn(PRIMARY_MODES)
        );
    }

    @Test
    void forget_sets_a_generation_other_than_0_then_deletes_the_marker_both_at_majority_with_journal_and_a_write_timeout() {
        // When
        storage.forgetCatchupMarker(subscriptionId);

        // Then
        List<Command> writes = commands.commands().stream().filter(command -> COLLECTION.equals(command.collection())).toList();
        assertThat(writes).extracting(Command::name).as("%s", writes).containsExactly("update", "delete");
        Command change = writes.get(0);
        Command delete = writes.get(1);
        BsonDocument majorityWithJournalAndTimeout = new BsonDocument("w", new BsonString("majority")).append("j", BsonBoolean.TRUE).append("wtimeout", new BsonInt64(TEN_SECONDS_IN_MILLIS));
        BsonDocument generationUpdate = change.document().getArray("updates").get(0).asDocument();
        BsonDocument update = generationUpdate.getDocument("u");
        assertAll(
                () -> assertThat(generationUpdate.getDocument("q").get("_id")).as("%s", change).isEqualTo(generationId()),
                () -> assertThat(generationUpdate.getBoolean("upsert").getValue()).as("upsert, %s", change).isTrue(),
                () -> assertThat(update.keySet()).as("update operators, %s", change).containsExactly("$set"),
                () -> assertThat(update.getDocument("$set").keySet()).as("fields set, %s", change).containsExactly("generation"),
                () -> assertThat(update.getDocument("$set").get("generation")).as("generation set, %s", change).isInstanceOf(BsonInt64.class).isNotEqualTo(new BsonInt64(0)),
                () -> assertThat(normalized(change.document().getDocument("writeConcern", new BsonDocument()))).as("%s", change).isEqualTo(majorityWithJournalAndTimeout),
                () -> assertThat(delete.document().getArray("deletes").get(0).asDocument().getDocument("q").get("_id")).as("%s", delete).isEqualTo(new BsonString(subscriptionId)),
                () -> assertThat(normalized(delete.document().getDocument("writeConcern", new BsonDocument()))).as("%s", delete).isEqualTo(majorityWithJournalAndTimeout)
        );
    }

    @Test
    void save_writes_the_marker_at_the_write_concern_of_the_collection() {
        // When
        storage.saveCatchupMarker(subscriptionId, new StringBasedCheckpoint("marker"), 0, CheckpointWriteCondition.any());

        // Then
        List<Command> saves = commands.commands("findAndModify", COLLECTION);
        assertThat(saves).as("%s", saves).hasSize(1);
        Command save = saves.get(0);
        assertThat(save.document().getDocument("writeConcern", new BsonDocument()).get("w")).as("%s", save).isEqualTo(new BsonInt32(1));
    }

    @Test
    void forget_on_a_client_with_an_infinite_operation_timeout_sends_both_writes_with_a_max_time_of_at_most_the_write_timeout() {
        // Given
        mongoClient.close();
        openStore(settings -> settings.timeout(0, TimeUnit.SECONDS));
        commands.clear();

        // When
        storage.forgetCatchupMarker(subscriptionId);

        // Then
        List<Command> writes = collectionCommands();
        assertThat(writes).extracting(Command::name).as("%s", writes).containsExactly("update", "delete");
        assertAll(writes.stream().map(write -> (Executable) () ->
                assertThat(maxTimeMillis(write)).as("%s", write).isBetween(1L, TEN_SECONDS_IN_MILLIS)).toList());
    }

    @Test
    void read_on_a_client_with_an_infinite_operation_timeout_sends_both_finds_with_a_max_time_of_at_most_the_read_limit() {
        // Given
        mongoClient.close();
        openStore(settings -> settings.timeout(0, TimeUnit.SECONDS));
        commands.clear();

        // When
        storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> finds = commands.commands("find", COLLECTION);
        assertThat(finds).as("%s", finds).hasSize(2);
        Command markerRead = finds.get(0);
        Command generationRead = finds.get(1);
        assertAll(
                () -> assertThat(markerRead.readConcernLevel()).as("%s", markerRead).isEqualTo("majority"),
                () -> assertThat(maxTimeMillis(markerRead)).as("%s", markerRead).isBetween(1L, TEN_SECONDS_IN_MILLIS),
                () -> assertThat(generationRead.readConcernLevel()).as("%s", generationRead).isEqualTo("linearizable"),
                () -> assertThat(maxTimeMillis(generationRead)).as("%s", generationRead).isBetween(1L, TEN_SECONDS_IN_MILLIS)
        );
    }

    @Test
    void forget_and_read_on_a_client_with_an_operation_timeout_keep_the_timeout_of_the_client() {
        // Given
        mongoClient.close();
        openStore(settings -> settings.timeout(2, TimeUnit.SECONDS));
        commands.clear();

        // When
        storage.forgetCatchupMarker(subscriptionId);
        storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> all = collectionCommands();
        assertThat(all).extracting(Command::name).as("%s", all).containsExactly("update", "delete", "find", "find");
        assertAll(all.stream().map(command -> (Executable) () ->
                assertThat(maxTimeMillis(command)).as("%s", command).isBetween(1L, 2_000L)).toList());
    }

    private List<Command> collectionCommands() {
        return commands.commands().stream().filter(command -> COLLECTION.equals(command.collection())).toList();
    }

    // Absent is reported as 0, which a bound never is
    private static long maxTimeMillis(Command command) {
        return command.document().containsKey("maxTimeMS") ? command.document().getNumber("maxTimeMS").longValue() : 0L;
    }

    private static String readPreferenceMode(Command command) {
        BsonDocument readPreference = command.document().getDocument("$readPreference", null);
        return readPreference == null ? "primary" : readPreference.getString("mode").getValue();
    }

    private BsonDocument generationId() {
        return new BsonDocument("catchupMarker", new BsonString(subscriptionId));
    }

    // The driver sends wtimeout as an int or a long depending on its size
    private static BsonDocument normalized(BsonDocument writeConcern) {
        BsonDocument copy = writeConcern.clone();
        if (copy.containsKey("wtimeout")) {
            copy.put("wtimeout", new BsonInt64(copy.getNumber("wtimeout").longValue()));
        }
        return copy;
    }
}
