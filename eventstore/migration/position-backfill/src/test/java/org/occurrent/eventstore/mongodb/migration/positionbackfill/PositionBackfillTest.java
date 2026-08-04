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

package org.occurrent.eventstore.mongodb.migration.positionbackfill;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionBackfillTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String COLLECTION_NAME = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoDatabase database;
    private SpringMongoEventStore eventStoreWithoutPosition;

    @BeforeEach
    void create_store_and_raw_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".position-backfill");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));

        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION_NAME)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                // Turn off stream position so this store writes the old unpositioned events the backfill retrofits.
                .withoutStreamPosition()
                .build();
        eventStoreWithoutPosition = new SpringMongoEventStore(mongoTemplate, config);
    }

    @Test
    void backfills_position_onto_existing_events_in_id_order() {
        writeUnpositionedEvents("stream-a", 3);
        writeUnpositionedEvents("stream-b", 2);

        List<Document> beforeById = eventsSortedById();
        assertThat(beforeById).allSatisfy(doc -> assertThat(doc.containsKey(OccurrentCloudEventExtension.POSITION)).isFalse());

        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, PositionBackfillOptions.defaults().withBatchSize(2));
        PositionBackfillResult result = backfill.run();

        assertThat(result.eventsPositioned()).isEqualTo(5);
        assertThat(result.completed()).isTrue();

        List<Document> afterById = eventsSortedById();
        List<Long> positions = afterById.stream()
                .map(doc -> ((Number) doc.get(OccurrentCloudEventExtension.POSITION)).longValue())
                .collect(Collectors.toList());

        // Every event has a position, strictly increasing in _id order, with no duplicates.
        assertThat(positions).doesNotContainNull();
        assertThat(positions).isSorted();
        assertThat(positions).doesNotHaveDuplicates();
        assertThat(positions).hasSize(5);
    }

    @Test
    void removes_the_checkpoint_document_when_the_run_completes() {
        writeUnpositionedEvents("stream-a", 3);

        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, PositionBackfillOptions.defaults().withBatchSize(2));
        backfill.run();

        MongoCollection<Document> checkpointCollection = database.getCollection(COLLECTION_NAME + "_position_backfill_checkpoint");
        Document checkpoint = checkpointCollection.find(new Document("_id", "positionBackfill")).first();
        assertThat(checkpoint).isNull();
    }

    @Test
    void seeds_counter_above_accurate_historical_count_plus_slack() {
        writeUnpositionedEvents("stream-a", 7);

        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, new PositionBackfillOptions(500, 0, 100));
        long seededTo = backfill.seedCounter();

        assertThat(seededTo).isEqualTo(7 + 100);

        MongoCollection<Document> positionCollection = database.getCollection(DcbMarkerModel.positionCollectionName(COLLECTION_NAME));
        Document counterDocument = positionCollection.find(new Document("_id", DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
        assertThat(counterDocument).isNotNull();
        assertThat(((Number) counterDocument.get(DcbMarkerModel.COUNTER_POSITION)).longValue()).isEqualTo(107L);
    }

    @Test
    void a_live_write_after_the_backfill_completes_lands_above_every_backfilled_position() {
        writeUnpositionedEvents("stream-a", 5);

        // Simulate the deploy sequence: seed the counter, run the backfill to completion, then a live write on the
        // now position-writing store.
        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, PositionBackfillOptions.defaults().withCounterSeedSlack(50));
        backfill.seedCounter();
        backfill.run();

        long livePosition = reservePositionForLiveWrite();

        List<Document> backfilledEvents = eventsSortedById();
        long maxBackfilledPosition = backfilledEvents.stream()
                .mapToLong(doc -> ((Number) doc.get(OccurrentCloudEventExtension.POSITION)).longValue())
                .max()
                .orElseThrow();

        assertThat(livePosition).isGreaterThan(maxBackfilledPosition);
    }

    private long reservePositionForLiveWrite() {
        MongoCollection<Document> positionCollection = database.getCollection(DcbMarkerModel.positionCollectionName(COLLECTION_NAME));
        Document updated = positionCollection.findOneAndUpdate(
                new Document("_id", DcbMarkerModel.POSITION_DOCUMENT_ID),
                new Document("$inc", new Document(DcbMarkerModel.COUNTER_POSITION, 1L)),
                new com.mongodb.client.model.FindOneAndUpdateOptions().upsert(true).returnDocument(com.mongodb.client.model.ReturnDocument.AFTER)
        );
        return ((Number) requireNonNull(updated).get(DcbMarkerModel.COUNTER_POSITION)).longValue();
    }

    @Test
    void rerunning_the_backfill_is_a_noop() {
        writeUnpositionedEvents("stream-a", 4);

        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, PositionBackfillOptions.defaults());
        PositionBackfillResult firstRun = backfill.run();
        assertThat(firstRun.eventsPositioned()).isEqualTo(4);

        List<Document> afterFirstRun = eventsSortedById();

        PositionBackfillResult secondRun = backfill.run();
        assertThat(secondRun.eventsPositioned()).isZero();

        List<Document> afterSecondRun = eventsSortedById();
        assertThat(afterSecondRun).isEqualTo(afterFirstRun);
    }

    @Test
    void resumes_from_checkpoint_after_a_partial_run() {
        writeUnpositionedEvents("stream-a", 3);
        writeUnpositionedEvents("stream-b", 3);

        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, PositionBackfillOptions.defaults().withBatchSize(2));

        // Simulate a crash after one batch: process a single batch instead of run()'s full loop.
        backfill.seedCounter();
        long firstBatch = backfill.backfillBatch();
        assertThat(firstBatch).isEqualTo(2);

        long remainingPositioned = 0;
        long batchPositioned;
        do {
            batchPositioned = backfill.backfillBatch();
            remainingPositioned += batchPositioned;
        } while (batchPositioned > 0);

        assertThat(remainingPositioned).isEqualTo(4);

        List<Document> afterById = eventsSortedById();
        List<Long> positions = afterById.stream()
                .map(doc -> ((Number) doc.get(OccurrentCloudEventExtension.POSITION)).longValue())
                .toList();
        assertThat(positions).doesNotContainNull();
        assertThat(positions).isSorted();
        assertThat(positions).doesNotHaveDuplicates();
        assertThat(positions).hasSize(6);
    }

    @Test
    void throttles_between_batches() {
        writeUnpositionedEvents("stream-a", 4);

        long throttleMillis = 150;
        PositionBackfill backfill = new PositionBackfill(database, COLLECTION_NAME, new PositionBackfillOptions(1, throttleMillis, 10));

        long start = System.currentTimeMillis();
        PositionBackfillResult result = backfill.run();
        long elapsed = System.currentTimeMillis() - start;

        assertThat(result.eventsPositioned()).isEqualTo(4);
        // 4 batches of size 1 => 3 throttle sleeps between them at minimum.
        assertThat(elapsed).isGreaterThanOrEqualTo(3 * throttleMillis);
    }

    private void writeUnpositionedEvents(String streamId, int count) {
        List<CloudEvent> events = Stream.generate(() -> (CloudEvent) CloudEventBuilder.v1()
                        .withId(java.util.UUID.randomUUID().toString())
                        .withSource(SOURCE)
                        .withType("SomethingHappened")
                        .withTime(java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
                        .withSubject(streamId)
                        .build())
                .limit(count)
                .toList();
        eventStoreWithoutPosition.write(streamId, events);
    }

    private List<Document> eventsSortedById() {
        MongoCollection<Document> eventCollection = database.getCollection(COLLECTION_NAME);
        return eventCollection.find()
                .sort(new Document("_id", 1))
                .into(new ArrayList<>());
    }
}
