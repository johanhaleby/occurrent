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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;

@Testcontainers
class MongoEventStoreCursorLifecycleTest {

    private static final String STREAM_ID = "cursor-lifecycle";
    private static final String TYPE = "SomethingHappened";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoEventStore eventStore;

    @BeforeEach
    void setUp() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        MongoCollection<Document> eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        // batchSize(1) forces the driver to keep a live server-side cursor open across getMore calls instead of
        // returning every document in the first batch, so a leaked cursor is actually observable below.
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .queryOptions(findIterable -> findIterable.batchSize(1))
                .build();
        eventStore = new MongoEventStore(mongoClient, database, eventCollection, config);
    }

    @AfterEach
    void tearDown() {
        mongoClient.close();
    }

    @Test
    void closing_the_returned_stream_releases_the_underlying_mongo_cursor() {
        eventStore.write(STREAM_ID, List.of(event("event-1", TYPE), event("event-2", TYPE), event("event-3", TYPE)));

        long openCursorsBeforeRead = openServerCursors();

        // findFirst() consumes only the first (of three) batch-of-one documents, leaving a real server-side cursor
        // open until the try-with-resources block below closes it.
        try (Stream<CloudEvent> events = eventStore.read(STREAM_ID).events()) {
            events.findFirst();
        }

        assertThat(openServerCursors()).isEqualTo(openCursorsBeforeRead);
    }

    private long openServerCursors() {
        Document status = database.runCommand(new Document("serverStatus", 1));
        Document open = status.get("metrics", Document.class).get("cursor", Document.class).get("open", Document.class);
        return ((Number) open.get("total")).longValue();
    }
}
