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

package org.occurrent.testing.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This container is neither reused nor pinned to host port 27017, unlike most MongoDB tests here, so it is the one
 * deliberate deviation in this module. Both of those exist to share one container between test classes for local speed,
 * and sharing is what this suite cannot tolerate: it watches a change stream across a flush, so a container another
 * class drops out from under it fails these tests for a reason that has nothing to do with the code. Testcontainers
 * disables reuse in CI anyway, so this changes nothing there.
 * <p>
 * The database is still named rather than left as {@code test}, per {@code AGENTS.md}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentMongoFlushTest {

    private static final String EVENTS = "events";
    private static final String DATABASE = "occurrent-testing-mongodb";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient mongoClient;
    private MongoDatabase database;

    @BeforeEach
    void connectAndStartFromAnEmptyDatabase() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
        database.drop();
    }

    @AfterEach
    void disconnect() {
        mongoClient.close();
    }

    @Test
    void a_change_stream_keeps_delivering_across_a_flush() {
        MongoCollection<Document> events = database.getCollection(EVENTS);
        events.insertOne(new Document("_id", "seed"));
        try (var changeStream = events.watch().cursor()) {
            // A change stream only carries what happens after it opens, so open it before writing what it must see.
            openStream(changeStream);
            events.insertOne(new Document("_id", "before"));
            assertThat(nextOperation(changeStream)).isEqualTo("insert");

            OccurrentMongoFlush.everyCollectionIn(database).run();
            events.insertOne(new Document("_id", "after"));

            // Skip the delete documents the flush itself produces. A subscription model ignores them, since it only
            // converts an insert, so what matters is that the insert after the flush still arrives.
            assertThat(idOfNextInsert(changeStream))
                    .as("deleting documents must leave the change stream usable, or every subscription resumed after a "
                            + "flush silently receives nothing")
                    .isEqualTo("after");
        }
    }

    @Test
    void dropping_invalidates_the_change_stream_instead() {
        MongoCollection<Document> events = database.getCollection(EVENTS);
        events.insertOne(new Document("_id", "seed"));
        try (var changeStream = events.watch().cursor()) {
            openStream(changeStream);
            events.insertOne(new Document("_id", "before"));
            assertThat(nextOperation(changeStream)).isEqualTo("insert");

            events.drop();

            // Positively awaited rather than asserted as an absence: the stream says drop, then invalidate, and an
            // invalidated cursor is why nothing arrives afterwards.
            List<String> operations = new ArrayList<>();
            operations.add(nextOperation(changeStream));
            operations.add(nextOperation(changeStream));

            assertThat(operations)
                    .as("this is the failure the flush exists to avoid, so it has to be shown rather than assumed")
                    .containsExactly("drop", "invalidate");
        }
    }

    @Test
    void the_event_stores_unique_indexes_survive_a_flush() {
        MongoCollection<Document> events = database.getCollection(EVENTS);
        events.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")),
                new IndexOptions().unique(true));
        events.insertOne(new Document("id", "1").append("source", "urn:test"));

        OccurrentMongoFlush.everyCollectionIn(database).run();

        assertThat(indexNames(events))
                .as("an Occurrent event store creates its unique indexes in its constructor, so a drop removes them "
                        + "for the rest of a run and duplicate detection then passes without an index behind it")
                .contains("id_1_source_1");
        assertThat(events.countDocuments()).isZero();
    }

    @Test
    void dropping_removes_the_collection_and_its_indexes_which_is_what_emptying_cannot_do() {
        MongoCollection<Document> events = database.getCollection(EVENTS);
        events.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")),
                new IndexOptions().unique(true));
        events.insertOne(new Document("id", "1").append("source", "urn:test"));

        OccurrentMongoFlush.droppingTheDatabaseIn(database).run();

        assertThat(collectionNames())
                .as("a test asserting a collection is absent needs the collection gone, which is the only reason this "
                        + "strategy is published")
                .doesNotContain(EVENTS);
        assertThat(indexNames(database.getCollection(EVENTS)))
                .as("and the indexes with it, which is exactly why it is not the default")
                .doesNotContain("id_1_source_1");
    }

    @Test
    void every_collection_is_emptied_including_the_ones_a_hand_written_list_forgets() {
        List<String> collections = List.of(EVENTS, "events_position", "events_dcb_checkpoints", "subscriptions",
                "competing-consumer-locks", "an-application-collection");
        collections.forEach(name -> database.getCollection(name).insertOne(new Document("_id", name)));

        OccurrentMongoFlush.everyCollectionIn(database).run();

        assertThat(collections)
                .allSatisfy(name -> assertThat(database.getCollection(name).countDocuments())
                        .as("%s must be emptied without being named", name)
                        .isZero());
    }

    @Test
    void only_the_named_collections_are_emptied() {
        database.getCollection(EVENTS).insertOne(new Document("_id", "1"));
        database.getCollection("keep-me").insertOne(new Document("_id", "1"));

        OccurrentMongoFlush.collectionsIn(database, EVENTS).run();

        assertThat(database.getCollection(EVENTS).countDocuments()).isZero();
        assertThat(database.getCollection("keep-me").countDocuments()).isOne();
    }

    @Test
    void an_excepted_collection_keeps_its_documents() {
        database.getCollection(EVENTS).insertOne(new Document("_id", "1"));
        database.getCollection("reference-data").insertOne(new Document("_id", "1"));

        OccurrentMongoFlush.everyCollectionIn(database).except("reference-data").run();

        assertThat(database.getCollection(EVENTS).countDocuments()).isZero();
        assertThat(database.getCollection("reference-data").countDocuments()).isOne();
    }

    @Test
    void a_view_is_left_alone_rather_than_failing_the_flush() {
        database.getCollection(EVENTS).insertOne(new Document("_id", "1").append("type", "NameDefined"));
        database.createView("events-view", EVENTS, List.of(new Document("$match", new Document("type", "NameDefined"))),
                new CreateViewOptions());

        // A view cannot be written to, so a flush that treated it as a collection would throw here.
        OccurrentMongoFlush.everyCollectionIn(database).run();

        assertThat(database.getCollection(EVENTS).countDocuments()).isZero();
    }

    @Test
    void except_on_a_database_drop_rejects_the_call_instead_of_silently_keeping_nothing() {
        OccurrentMongoFlush flush = OccurrentMongoFlush.droppingTheDatabaseIn(database);

        assertThatThrownBy(() -> flush.except("keepMe"))
                .as("droppingTheDatabaseIn(db).except(\"keepMe\") used to compile and drop keepMe anyway")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("except");
    }

    @Test
    void excepting_every_named_collection_fails_loudly_instead_of_flushing_nothing() {
        database.getCollection(EVENTS).insertOne(new Document("_id", "1"));
        OccurrentMongoFlush flush = OccurrentMongoFlush.collectionsIn(database, EVENTS).except(EVENTS);

        assertThatThrownBy(flush::run)
                .as("collectionsIn(db, \"events\").except(\"events\") used to flush nothing, silently")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(EVENTS);

        assertThat(database.getCollection(EVENTS).countDocuments())
                .as("a failed flush must not leave stale data behind while pretending to have succeeded")
                .isOne();
    }

    @Test
    void a_database_that_cannot_be_reached_fails_loudly_and_names_itself() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost:1/unreachable"))
                .applyToClusterSettings(cluster -> cluster.serverSelectionTimeout(200, TimeUnit.MILLISECONDS))
                .build();

        try (MongoClient unreachable = MongoClients.create(settings)) {
            OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(unreachable.getDatabase("unreachable"));

            assertThatThrownBy(flush::run)
                    .as("a flush that failed quietly would leave the previous test's data in place, which is worse "
                            + "than failing")
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("unreachable");
        }
    }

    // The driver issues the aggregate only on the first read, so this establishes the stream before the test writes
    // anything it expects to see. It returns null, which is the point.
    private static void openStream(MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStream) {
        changeStream.tryNext();
    }

    // Every read below is bounded. tryNext() polls rather than blocking, so a stream that goes quiet, which is exactly
    // what an invalidated one does, fails the test instead of hanging a shard that has no rerun backstop.
    private static String idOfNextInsert(MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStream) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            ChangeStreamDocument<Document> change = changeStream.tryNext();
            if (change != null && "insert".equals(change.getOperationType().getValue())) {
                return change.getDocumentKey().getString("_id").getValue();
            }
        }
        throw new AssertionError("No insert arrived on the change stream within " + TIMEOUT);
    }

    private static String nextOperation(MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStream) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            ChangeStreamDocument<Document> change = changeStream.tryNext();
            if (change != null) {
                return change.getOperationType().getValue();
            }
        }
        throw new AssertionError("Nothing arrived on the change stream within " + TIMEOUT);
    }

    private List<String> collectionNames() {
        List<String> names = new ArrayList<>();
        database.listCollectionNames().forEach(names::add);
        return names;
    }

    private static List<String> indexNames(MongoCollection<Document> collection) {
        return StreamSupport.stream(collection.listIndexes().spliterator(), false)
                .map(index -> index.getString("name"))
                .toList();
    }
}
