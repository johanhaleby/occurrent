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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A time-based catch-up stores the time of the last event it handled and resumes from that time. An event with an
 * earlier time can commit after the replay read past that time, and it must still reach the subscription after a restart.
 */
@Timeout(240)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamTimeCatchupLateCommitMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(20);
    private static final String HELD_WRITER = "writer-A-held";
    private static final String DATABASE = "latecommit";

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, DATABASE));

    private final CountDownLatch aInCommit = new CountDownLatch(1);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;

    // Holds the commit of whichever transaction runs on the HELD_WRITER thread, after the store inserted its documents
    // inside the transaction, so they stay invisible to every other reader
    class HoldingTransactionManager extends MongoTransactionManager {
        HoldingTransactionManager(MongoDatabaseFactory factory) {
            super(factory);
        }

        @Override
        protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
            if (Thread.currentThread().getName().equals(HELD_WRITER)) {
                aInCommit.countDown();
                assertThat(releaseA.await(60, TimeUnit.SECONDS)).isTrue();
            }
            super.doCommit(transactionObject);
        }
    }

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager tx = new HoldingTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        releaseA.countDown();
        mongoClient.close();
    }

    @Test
    void an_event_whose_earlier_time_commits_after_a_catch_up_checkpoint_passed_it_is_delivered_after_a_restart() throws Exception {
        // Writer A takes the earliest time and holds its transaction open
        Thread writerA = new Thread(() -> appendToStream("stream-a", named("A")), HELD_WRITER);
        writerA.start();
        assertThat(aInCommit.await(20, TimeUnit.SECONDS)).isTrue();
        // B and C take later times and commit
        appendToStream("stream-b", named("B"));
        appendToStream("stream-c", named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        // The first process replays B, stores B's time and dies while handling C
        SpringMongoSubscriptionModel firstLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        StreamCatchupSubscriptionModel first = new StreamCatchupSubscriptionModel(firstLive, eventStore, config);
        CountDownLatch handlingC = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", StartAt.checkpoint(TimeBasedCheckpoint.beginningOfTime()), cloudEvent -> {
            String name = nameOf(cloudEvent);
            received.add(name);
            if (name.equals("C")) {
                handlingC.countDown();
                try {
                    crashed.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        assertThat(handlingC.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(requireNonNull(storage.read("sub")))).isFalse();
        first.shutdown();
        crashed.countDown();

        // A commits, with a time earlier than the stored one
        releaseA.countDown();
        writerA.join(20_000);

        // Second process resumes from storage
        SpringMongoSubscriptionModel secondLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        StreamCatchupSubscriptionModel second = new StreamCatchupSubscriptionModel(secondLive, eventStore, config);
        try {
            Subscription subscription = second.subscribe("sub", StartAt.subscriptionModelDefault(), cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            appendToStream("stream-d", named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("D"));
            assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "D");
        } finally {
            second.shutdown();
        }
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) converter.toDomainEvent(cloudEvent)).name();
    }

    private NameDefined named(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "user", name);
    }

    private void appendToStream(String streamId, DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event));
        eventStore.write(streamId, WriteCondition.anyStreamVersion(), cloudEvents);
    }
}
