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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.DcbSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;

/**
 * Real MongoDB integration test for {@link CatchupSubscriptionModel} in DCB mode (ADR 20). The in-memory
 * {@code DcbCatchupSubscriptionModelTest} exercises the DCB-specific replay, resume and filtering logic deterministically
 * but stubs position-awareness. This test covers the part that only a real database can: the catch-up to live handover
 * across the change stream, where DCB events written during and after the bulk replay must be delivered exactly once.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupSubscriptionModelMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final Duration AT_MOST = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReplicaSet()
                    .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    private SpringMongoEventStore eventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private SpringMongoSubscriptionPositionStorage storage;
    private CatchupSubscriptionModel subscription;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(connectionString.getCollection()), timeRepresentation);
        storage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, "storage");
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.shutdown();
        }
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void replays_dcb_history_then_delivers_live_events_across_the_handover_without_duplicates() {
        // Given a DCB history written before the subscription starts
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        appendTagged("name:1", historic1);
        appendTagged("other:1", nameDefined("ignoredHistoric"));
        appendTagged("name:1", historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tags("name:1"),
                new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));

        // When the DCB-mode catch-up subscription replays from the beginning of the DCB sequence and hands over to the live change stream
        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();

        // Then the matching history is delivered (non-matching events filtered out)
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(historic1, historic2));

        // And when matching and non-matching events are written after the handover
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        appendTagged("name:1", live1);
        appendTagged("other:1", nameDefined("ignoredLive"));
        appendTagged("name:1", live2);

        // Then the matching live events arrive through the change stream, in order, with no duplicates at the seam
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, live1, live2);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void recovers_a_dcb_event_that_was_in_flight_below_the_replay_head_when_it_commits_during_catchup() {
        // Five matching events occupy a contiguous DCB run; the middle one stands in for an in-flight, below-head hole.
        NameDefined name1 = nameDefined("name1");
        NameDefined name2 = nameDefined("name2");
        NameDefined hole = nameDefined("hole");
        NameDefined name4 = nameDefined("name4");
        NameDefined name5 = nameDefined("name5");
        appendTagged("name:1", name1);
        appendTagged("name:1", name2);
        appendTagged("name:1", hole);
        appendTagged("name:1", name4);
        appendTagged("name:1", name5);

        // Simulate the middle event being in-flight (its dcbposition reserved outside the commit transaction) while the
        // replay scans its window: remove its document so the replay cannot see it, while the later events keep the
        // store head above it. Re-inserting it during the replay models the in-flight append finally committing.
        Document holeDocument = requireNonNull(mongoTemplate.findOne(query(where("id").is(hole.eventId())), Document.class, eventCollectionName));
        mongoTemplate.remove(query(where("id").is(hole.eventId())), eventCollectionName);

        CountDownLatch bulkReadReached = new CountDownLatch(1);
        CountDownLatch holeCommitted = new CountDownLatch(1);
        DcbEventStore blockingDuringBulkReplay = new BlockingOnFirstWindowRead(eventStore, bulkReadReached, holeCommitted);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        // A whole-range window forces a single bulk read that the wrapper can hold open across the hole's commit.
        subscription = new CatchupSubscriptionModel(subscriptionModel, blockingDuringBulkReplay, DcbQuery.tags("name:1"),
                new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1))
                        .dcbCatchupPositionWindowSize(1_000_000_000L));

        // The catch-up runs asynchronously and blocks inside the bulk replay read, after it has read a snapshot that
        // excludes the removed event.
        Subscription handle = subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received));

        // While the replay is blocked (it has scanned past the hole without seeing it and has not captured any
        // post-replay token), commit the in-flight event by re-inserting its document, then let the replay finish.
        awaitLatch(bulkReadReached);
        mongoTemplate.insert(holeDocument, eventCollectionName);
        holeCommitted.countDown();

        handle.waitUntilStarted();

        // The hole, committed during the replay at a below-head position, is recovered through the live change stream
        // because the resume token was captured before the replay. With a post-replay token it would be lost.
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactlyInAnyOrder(name1, name2, hole, name4, name5));
        assertThat(received).doesNotHaveDuplicates();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch reached in time").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Delegates to a real {@link DcbEventStore} but blocks inside the first windowed bulk-replay read until the test
     * signals, after the read has captured its snapshot. Holding the replay open lets the test commit an in-flight,
     * below-head event mid-replay and exercise the catch-up to live handover deterministically.
     */
    private static final class BlockingOnFirstWindowRead implements DcbEventStore {
        private final DcbEventStore delegate;
        private final CountDownLatch bulkReadReached;
        private final CountDownLatch proceed;
        private final AtomicBoolean blockedOnce = new AtomicBoolean(false);

        private BlockingOnFirstWindowRead(DcbEventStore delegate, CountDownLatch bulkReadReached, CountDownLatch proceed) {
            this.delegate = delegate;
            this.bulkReadReached = bulkReadReached;
            this.proceed = proceed;
        }

        @Override
        public DcbEventStream read(DcbQuery query, DcbReadOptions options) {
            DcbEventStream result = delegate.read(query, options);
            boolean windowRead = options.afterSequencePosition().isPresent() && options.upToSequencePosition().isPresent()
                    && options.afterSequencePosition().getAsLong() != options.upToSequencePosition().getAsLong();
            if (windowRead && blockedOnce.compareAndSet(false, true)) {
                bulkReadReached.countDown();
                try {
                    // Bounded so a test failure before the latch is counted down cannot hang this thread indefinitely.
                    proceed.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            return delegate.append(events, condition);
        }
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(tag)))
                .toList();
        eventStore.append(cloudEvents);
    }
}
