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

package org.occurrent.subscription.blocking.durable.catchup;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A DCB append larger than 16 megabytes commits as more than one oplog entry. When its positions are reserved before a
 * catch-up reads its live start and it commits after, every event of it must reach the catch-up once.
 */
@Timeout(240)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupLargeAppendStraddlingLiveStartMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(60);
    private static final String HELD_WRITER = "large-append-held";
    private static final String DATABASE = "dcblargeappend";
    private static final int LARGE_APPEND_EVENTS = 36;
    private static final int PAYLOAD_CHARACTERS = 512 * 1024;

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, DATABASE));

    private final CountDownLatch largeAppendInCommit = new CountDownLatch(1);
    private final CountDownLatch releaseLargeAppend = new CountDownLatch(1);
    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String databaseName;

    // Holds the commit of the transaction on the HELD_WRITER thread, after its positions are reserved and its
    // documents inserted, so none of them is visible to another reader yet
    class HoldingTransactionManager extends MongoTransactionManager {
        HoldingTransactionManager(MongoDatabaseFactory factory) {
            super(factory);
        }

        @Override
        protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
            if (Thread.currentThread().getName().equals(HELD_WRITER)) {
                largeAppendInCommit.countDown();
                assertThat(releaseLargeAppend.await(60, TimeUnit.SECONDS)).isTrue();
            }
            super.doCommit(transactionObject);
        }
    }

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager tx = new HoldingTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, config);
    }

    @AfterEach
    void shutdown() {
        releaseLargeAppend.countDown();
        mongoClient.close();
    }

    @Test
    void every_event_of_an_append_over_16_megabytes_reserved_before_the_live_start_and_committed_after_it_is_delivered_once() throws Exception {
        // Given
        List<CloudEvent> history = List.of(event("history-1", "small"), event("history-2", "small"));
        eventStore.append(history);
        String payload = "x".repeat(PAYLOAD_CHARACTERS);
        List<CloudEvent> largeAppend = IntStream.rangeClosed(1, LARGE_APPEND_EVENTS).mapToObj(i -> event("large-" + i, payload)).toList();
        Thread writer = new Thread(() -> eventStore.append(largeAppend), HELD_WRITER);
        writer.start();
        assertThat(largeAppendInCommit.await(60, TimeUnit.SECONDS)).as("whether the large append reached its commit").isTrue();

        SpringMongoSubscriptionModel live = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(live, eventStore, DcbCriteria.all());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        try {
            catchup.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> received.add(cloudEvent.getId()));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the replay delivered").containsExactly("history-1", "history-2"));

            // When
            releaseLargeAppend.countDown();
            writer.join(60_000);
            eventStore.append(List.of(event("after", "small")));

            // Then
            List<String> expected = new ArrayList<>(List.of("history-1", "history-2"));
            largeAppend.forEach(cloudEvent -> expected.add(cloudEvent.getId()));
            expected.add("after");
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the catch-up delivered").contains("after"));
            assertThat(received).as("what the catch-up delivered").containsExactlyElementsOf(expected);
            assertThat(oplogEntriesOfTheLargeAppend()).as("the oplog entries the large append committed as").isGreaterThan(1);
        } finally {
            catchup.shutdown();
        }
    }

    private long oplogEntriesOfTheLargeAppend() {
        return mongoClient.getDatabase("local").getCollection("oplog.rs").countDocuments(Filters.and(
                Filters.eq("o.applyOps.ns", databaseName + ".events"),
                Filters.regex("o.applyOps.o.id", "^large-")));
    }

    private static CloudEvent event(String id, String payload) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("Large")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData(("{\"payload\":\"" + payload + "\"}").getBytes(StandardCharsets.UTF_8))
                .build();
        return DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse("event:" + UUID.randomUUID())));
    }
}
