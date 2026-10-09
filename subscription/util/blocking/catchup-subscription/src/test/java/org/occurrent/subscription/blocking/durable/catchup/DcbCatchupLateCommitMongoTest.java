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
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
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
import org.testcontainers.mongodb.MongoDBContainer;

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
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A Mongo event store reserves a DCB event's position before its transaction commits, so an event at a lower position
 * can become visible after one at a higher position. A DCB catch-up that stored a position past such an event, and
 * then restarted, must still deliver it.
 * <p>
 * An event written while the catch-up was down, after it read its live start, must reach it once after the restart.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupLateCommitMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(20);
    private static final String HELD_WRITER = "writer-A-held";
    // A has a type and a tag of its own, so B and C share no conflict marker with it and commit while A is held
    private static final DcbCriteria CRITERIA = DcbCriteria.all();

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "dcblatecommit"));

    private final CountDownLatch aInCommit = new CountDownLatch(1);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;

    // Holds the commit of whichever transaction runs on the HELD_WRITER thread, after the store reserved its position
    // and inserted its documents inside the transaction, so they stay invisible to every other reader
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
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("dcblatecommit"));
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager tx = new HoldingTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
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
    void an_event_whose_lower_position_commits_after_a_catch_up_checkpoint_passed_it_is_delivered_after_a_restart() throws Exception {
        // Writer A reserves position 1 and holds its transaction open
        Thread writerA = new Thread(() -> append(changedTo("A")), HELD_WRITER);
        writerA.start();
        assertThat(aInCommit.await(20, TimeUnit.SECONDS)).isTrue();
        // B and C take positions 2 and 3 and commit
        append(named("B"));
        append(named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers B, stores position 2, then the process dies while handling C
        SpringMongoSubscriptionModel firstLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel first = new CatchupSubscriptionModel(firstLive, eventStore, CRITERIA, config);
        CountDownLatch handlingC = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
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
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub")))).isEqualTo(2);
        first.shutdown();
        crashed.countDown();

        // A commits at position 1, below the stored checkpoint
        releaseA.countDown();
        writerA.join(20_000);

        // Second process resumes from storage, like @Subscription(startAt = BEGINNING) does on a restart
        SpringMongoSubscriptionModel secondLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel second = new CatchupSubscriptionModel(secondLive, eventStore, CRITERIA, config);
        try {
            Subscription subscription = second.subscribe("sub", StartAt.subscriptionModelDefault(), cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            append(named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("D"));
            assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "D");
        } finally {
            second.shutdown();
        }
    }

    @Test
    void an_event_written_while_a_catch_up_was_down_mid_replay_is_delivered_once_after_a_restart() throws Exception {
        append(named("A"));
        append(named("B"));
        append(named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A, stores position 1, then the process dies while handling B
        SpringMongoSubscriptionModel firstLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel first = new CatchupSubscriptionModel(firstLive, eventStore, CRITERIA, config);
        CountDownLatch handlingB = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            String name = nameOf(cloudEvent);
            received.add(name);
            if (name.equals("B")) {
                handlingB.countDown();
                try {
                    crashed.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        assertThat(handlingB.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub")))).isEqualTo(1);
        first.shutdown();
        crashed.countDown();

        // W commits after the first process read its live start
        append(named("W"));

        SpringMongoSubscriptionModel secondLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel second = new CatchupSubscriptionModel(secondLive, eventStore, CRITERIA, config);
        try {
            Subscription subscription = second.subscribe("sub", StartAt.subscriptionModelDefault(), cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            // D commits after W, so once D is in, every delivery of W is in
            append(named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("D"));
            assertThat(received.stream().filter("W"::equals)).as("deliveries of W, which committed while the catch-up was down").hasSize(1);
        } finally {
            second.shutdown();
        }
    }

    @Test
    void control_without_a_restart_the_live_token_captured_before_the_replay_delivers_the_late_commit() throws Exception {
        Thread writerA = new Thread(() -> append(changedTo("A")), HELD_WRITER);
        writerA.start();
        assertThat(aInCommit.await(20, TimeUnit.SECONDS)).isTrue();
        append(named("B"));
        append(named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        SpringMongoSubscriptionModel live = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, eventStore, CRITERIA, config);
        try {
            Subscription subscription = model.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("B", "C"));
            releaseA.countDown();
            writerA.join(20_000);
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("A", "B", "C"));
        } finally {
            model.shutdown();
        }
    }

    private String nameOf(CloudEvent cloudEvent) {
        return switch (converter.toDomainEvent(cloudEvent)) {
            case NameDefined e -> e.name();
            case NameWasChanged e -> e.name();
            default -> throw new IllegalStateException("Unexpected event " + cloudEvent);
        };
    }

    private NameWasChanged changedTo(String name) {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "user", name);
    }

    private NameDefined named(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "user", name);
    }

    private void append(DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event)).stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse("name:" + nameOf(cloudEvent)))))
                .toList();
        eventStore.append(cloudEvents);
    }
}
