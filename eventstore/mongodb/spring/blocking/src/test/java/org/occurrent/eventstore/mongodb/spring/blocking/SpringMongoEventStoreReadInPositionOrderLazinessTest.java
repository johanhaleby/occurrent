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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.filter.Filter;
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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Pins that {@code readInPositionOrder} reads through a server cursor rather than decoding the whole result up front.
 * <p>
 * The defect it guards against is a from-the-beginning replay (which is what both push catch-up models issue) holding
 * the entire event history in memory before delivering its first event. Verifying that by heap size means a large
 * store and a small heap, which is neither deterministic nor cheap enough for a build, so this counts the documents
 * the server actually hands over instead. That is the same property stated exactly: an eager read pulls every matched
 * document before the first one is available, a cursor-backed read pulls one batch.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreReadInPositionOrderLazinessTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";
    private static final int STREAMS = 5;
    private static final int EVENTS_PER_STREAM = 100;
    private static final int TOTAL_EVENTS = STREAMS * EVENTS_PER_STREAM;

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final CursorTraffic cursorTraffic = new CursorTraffic(EVENT_COLLECTION);

    private SpringMongoEventStore eventStore;

    @BeforeEach
    void create_event_store_with_a_history_worth_paging() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".position_order_laziness");
        MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .addCommandListener(cursorTraffic)
                .build());
        String databaseName = requireNonNull(connectionString.getDatabase());
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        eventStore = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build());

        IntStream.range(0, STREAMS).forEach(stream -> eventStore.write("stream:" + stream, WriteCondition.anyStreamVersion(),
                IntStream.range(0, EVENTS_PER_STREAM).mapToObj(__ -> event("NameDefined")).toList()));
    }

    @Test
    void a_replay_from_the_beginning_delivers_its_first_event_without_fetching_the_whole_history() {
        cursorTraffic.reset();

        try (Stream<CloudEvent> replay = eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
            assertThat(replay.findFirst()).isPresent();
        }

        // An eager read fetches all TOTAL_EVENTS before findFirst() can answer. A cursor-backed one fetches the
        // first batch, which the server caps well below this. Asserting against the total rather than against a
        // batch size keeps this independent of how the driver and server size their batches.
        assertThat(cursorTraffic.documentsFetched()).isLessThan(TOTAL_EVENTS);
    }

    @Test
    void closing_a_replay_early_releases_the_server_cursor() {
        cursorTraffic.reset();

        try (Stream<CloudEvent> replay = eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
            assertThat(replay.findFirst()).isPresent();
        }

        assertThat(cursorTraffic.cursorsKilled()).isEqualTo(1);
    }

    @Test
    void a_replay_read_to_the_end_still_returns_every_event_in_position_order() {
        final List<Long> positions;
        try (Stream<CloudEvent> replay = eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
            positions = replay.map(OccurrentCloudEventExtension::getPosition).toList();
        }

        assertThat(positions).hasSize(TOTAL_EVENTS).isSorted();
    }

    @Test
    void a_replay_read_to_the_end_releases_the_server_cursor_without_being_closed() {
        cursorTraffic.reset();

        // Not in a try-with-resources on purpose: exhausting the stream has to release the cursor on its own, which
        // is what every caller that reads a whole window relies on.
        long count = eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()).count();

        assertThat(count).isEqualTo(TOTAL_EVENTS);
        assertThat(cursorTraffic.cursorsLeftOpen()).isZero();
    }

    /**
     * Counts the documents the server hands over for one collection, and the cursors opened and killed for it. The
     * command name alone does not say which collection a batch belongs to, so the started event is what carries that
     * and the succeeded event is matched back to it by request id.
     */
    private static final class CursorTraffic implements CommandListener {
        private final String collectionName;
        private final Map<Integer, String> collectionByRequestId = new ConcurrentHashMap<>();
        private final AtomicLong documentsFetched = new AtomicLong();
        private final AtomicLong cursorsOpened = new AtomicLong();
        private final AtomicLong cursorsKilled = new AtomicLong();

        private CursorTraffic(String collectionName) {
            this.collectionName = collectionName;
        }

        @Override
        public void commandStarted(CommandStartedEvent event) {
            String commandName = event.getCommandName();
            BsonDocument command = event.getCommand();
            switch (commandName) {
                case "find" -> collectionByRequestId.put(event.getRequestId(), command.getString("find").getValue());
                case "getMore" -> collectionByRequestId.put(event.getRequestId(), command.getString("collection").getValue());
                case "killCursors" -> {
                    if (collectionName.equals(command.getString("killCursors").getValue())) {
                        cursorsKilled.addAndGet(command.getArray("cursors").size());
                    }
                }
                default -> {
                }
            }
        }

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            String collection = collectionByRequestId.remove(event.getRequestId());
            if (!collectionName.equals(collection)) {
                return;
            }
            BsonDocument cursor = event.getResponse().getDocument("cursor", new BsonDocument());
            String batchField = "find".equals(event.getCommandName()) ? "firstBatch" : "nextBatch";
            if (cursor.containsKey(batchField)) {
                documentsFetched.addAndGet(cursor.getArray(batchField).size());
            }
            if (!cursor.containsKey("id")) {
                return;
            }
            boolean cursorStillOpen = cursor.getInt64("id").getValue() != 0;
            if ("find".equals(event.getCommandName())) {
                if (cursorStillOpen) {
                    cursorsOpened.incrementAndGet();
                }
            } else if (!cursorStillOpen) {
                // getMore draining the last batch closes the cursor server-side without a killCursors call, the same
                // release an explicit close/kill achieves.
                cursorsKilled.incrementAndGet();
            }
        }

        void reset() {
            collectionByRequestId.clear();
            documentsFetched.set(0);
            cursorsOpened.set(0);
            cursorsKilled.set(0);
        }

        long documentsFetched() {
            return documentsFetched.get();
        }

        long cursorsKilled() {
            return cursorsKilled.get();
        }

        /**
         * A cursor drained to the end is closed by the server, which reports it by returning id 0, so only a cursor
         * that was opened and neither drained nor killed counts as left open.
         */
        long cursorsLeftOpen() {
            return cursorsOpened.get() - cursorsKilled.get();
        }
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
