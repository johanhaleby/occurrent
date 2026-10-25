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
import org.awaitility.core.ConditionTimeoutException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
import org.occurrent.tck.internal.scaffolding.ExpectedGuaranteeViolation;
import org.occurrent.tck.latecommit.LateCommitGuarantee;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
 * An event written while the catch-up was down, after it read its live start, must reach it once after the restart,
 * also when that live start leaves the change stream history before the replay is done, and also when the catch-up
 * is started the way the Spring Boot starter starts it.
 */
@Timeout(240)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@ExpectedGuaranteeViolation(
        tests = "an_event_that_committed_below_a_position_the_application_stored_itself_is_delivered_by_a_catch_up_started_at_that_position",
        issue = "https://github.com/johanhaleby/occurrent/issues/1297")
class DcbCatchupLateCommitMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(20);
    private static final String HELD_WRITER = "writer-A-held";
    // A has a type and a tag of its own, so B and C share no conflict marker with it and commit while A is held
    private static final DcbCriteria CRITERIA = DcbCriteria.all();
    private static final String DATABASE = "dcblatecommit";

    // A one megabyte oplog, so a test can make MongoDB drop the change stream history from a live start
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
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
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

        // Second process resumes from the checkpoint in storage, which the model default does
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
    void an_event_written_while_a_catch_up_was_down_is_delivered_when_the_live_start_leaves_the_change_stream_history_during_the_resumed_replay() throws Exception {
        append(named("A"));
        append(named("B"));
        append(named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A, stores position 1, then the process dies while handling B
        CatchupSubscriptionModel first = new CatchupSubscriptionModel(liveModelThatRestartsOnLostHistory(), eventStore, CRITERIA, config);
        CountDownLatch handlingB = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            String name = nameOf(cloudEvent);
            received.add(name);
            if (name.equals("B")) {
                handlingB.countDown();
                block(crashed);
            }
        });
        assertThat(handlingB.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub")))).isEqualTo(1);
        first.shutdown();
        crashed.countDown();

        // W commits after the first process read its live start
        append(named("W"));

        // Second process: the resumed replay checks the stored live start, then MongoDB drops the history from it while
        // the replay handles the first event it delivers, B or C depending on whether the first process stored B on its
        // way down
        CatchupSubscriptionModel second = new CatchupSubscriptionModel(liveModelThatRestartsOnLostHistory(), eventStore, CRITERIA, config);
        CountDownLatch handlingTheFirstResumedEvent = new CountDownLatch(1);
        CountDownLatch historyDropped = new CountDownLatch(1);
        try {
            Subscription subscription = second.subscribe("sub", StartAt.subscriptionModelDefault(), cloudEvent -> {
                String name = nameOf(cloudEvent);
                received.add(name);
                if (handlingTheFirstResumedEvent.getCount() > 0) {
                    handlingTheFirstResumedEvent.countDown();
                    block(historyDropped);
                }
            });
            assertThat(handlingTheFirstResumedEvent.await(20, TimeUnit.SECONDS)).isTrue();
            dropChangeStreamHistoryUpToNow();
            historyDropped.countDown();
            subscription.waitUntilStarted();
            append(named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "W", "D"));
        } finally {
            second.shutdown();
        }
    }

    @Test
    void a_catch_up_started_the_way_the_spring_boot_starter_starts_it_resumes_a_replay_that_stopped_in_the_middle() throws Exception {
        append(named("A"));
        append(named("B"));
        append(named("C"));

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        // The start the starter gives a subscription that starts at the beginning and resumes the default way
        StartAt startAt = StartAt.dynamic(ctx -> storage.exists("sub") ? StartAt.subscriptionModelDefault() : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A, stores position 1, then the process dies while handling B
        SpringMongoSubscriptionModel firstLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel first = new CatchupSubscriptionModel(new DurableSubscriptionModel(firstLive, storage), eventStore, CRITERIA, config);
        CountDownLatch handlingB = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", startAt, cloudEvent -> {
            String name = nameOf(cloudEvent);
            received.add(name);
            if (name.equals("B")) {
                handlingB.countDown();
                block(crashed);
            }
        });
        assertThat(handlingB.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub")))).isEqualTo(1);
        first.shutdown();
        crashed.countDown();

        // W commits while the process is down
        append(named("W"));

        SpringMongoSubscriptionModel secondLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel second = new CatchupSubscriptionModel(new DurableSubscriptionModel(secondLive, storage), eventStore, CRITERIA, config);
        try {
            Subscription subscription = second.subscribe("sub", startAt, cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            append(named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "W", "D"));
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

    @Test
    void a_catch_up_whose_start_resolves_to_nothing_for_the_live_model_delivers_an_event_that_commits_below_the_replay_before_the_handover() throws Exception {
        // Given
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
        StartAt resolvesToNothingForTheLiveModel = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(SpringMongoSubscriptionModel.class) ? null : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        try {
            // When
            model.subscribe("sub", resolvesToNothingForTheLiveModel, cloudEvent -> {
                String name = nameOf(cloudEvent);
                received.add(name);
                if (name.equals("C")) {
                    // A commits below the replay after the replay read past it and before the handover
                    releaseA.countDown();
                    join(writerA);
                }
            }).waitUntilStarted();
            append(named("D"));
            boolean deliveredAfterTheHandover = arrives(received, "D");

            // Then
            assertThat(writerA.isAlive()).as("writer A still committing").isFalse();
            assertThat(deliveredAfterTheHandover).as("an event written after the handover is delivered live").isTrue();
            assertThat(received).as("the event that committed below the replay before the handover").contains("A");
        } finally {
            model.shutdown();
        }
    }

    // The Spring Boot starter's default
    @Test
    void an_event_that_committed_below_a_position_the_application_stored_itself_is_delivered_by_a_catch_up_started_at_that_position() throws Exception {
        // Writer A reserves a position and holds its transaction open, B and C take higher positions and commit
        AtomicReference<DcbAppendResult> appendedA = new AtomicReference<>();
        Thread writerA = new Thread(() -> appendedA.set(append(changedTo("A"))), HELD_WRITER);
        writerA.start();
        assertThat(aInCommit.await(20, TimeUnit.SECONDS)).isTrue();
        DcbAppendResult appendedB = append(named("B"));
        DcbAppendResult appendedC = append(named("C"));

        // A commits below the position the application stored for C
        releaseA.countDown();
        writerA.join(20_000);
        assertThat(appendedA.get().firstSequencePosition()).as("A reserved its position before B did").isLessThan(appendedB.firstSequencePosition());

        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints");
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        SpringMongoSubscriptionModel live = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel catchup = new CatchupSubscriptionModel(live, eventStore, CRITERIA, config);
        try {
            Subscription subscription = catchup.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(appendedC.lastSequencePosition())), cloudEvent -> received.add(nameOf(cloudEvent)));
            subscription.waitUntilStarted();
            append(named("D"));
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("the catch-up is delivering").contains("D"));
            LateCommitGuarantee.require(received.contains("A"),
                    "the event that committed below the position the application stored is delivered",
                    () -> "the subscription was delivered " + received);
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void an_event_that_committed_below_the_checkpoint_a_replayed_event_was_delivered_with_is_delivered_after_a_restart_from_it() throws Exception {
        // Given
        // Writer A reserves position 1 and holds its transaction open while B and C take positions 2 and 3 and commit
        Thread writerA = new Thread(() -> append(changedTo("A")), HELD_WRITER);
        writerA.start();
        assertThat(aInCommit.await(20, TimeUnit.SECONDS)).isTrue();
        append(named("B"));
        append(named("C"));

        // A first process without checkpoint storage handles B, holds on to the checkpoint B was delivered with, then dies while handling C
        ConcurrentHashMap<String, String> checkpointDeliveredWith = new ConcurrentHashMap<>();
        SpringMongoSubscriptionModel firstLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel first = new CatchupSubscriptionModel(firstLive, eventStore, CRITERIA, new CatchupSubscriptionModelConfig(100));
        CountDownLatch handlingC = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            String name = nameOf(cloudEvent);
            if (name.equals("C")) {
                handlingC.countDown();
                block(crashed);
            } else {
                checkpointDeliveredWith.put(name, EventMetadata.from(cloudEvent).getCheckpoint().orElseThrow());
            }
        });
        assertThat(handlingC.await(20, TimeUnit.SECONDS)).isTrue();
        first.shutdown();
        crashed.countDown();
        String stored = requireNonNull(checkpointDeliveredWith.get("B"));
        assertThat(GlobalCheckpoint.positionOf(new StringBasedCheckpoint(stored))).as("the position the checkpoint of B holds").isEqualTo(2);

        // A commits at position 1, below that checkpoint
        releaseA.countDown();
        writerA.join(20_000);

        // When
        // A second process restarts from the checkpoint the application held on to
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        SpringMongoSubscriptionModel secondLive = new SpringMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        CatchupSubscriptionModel second = new CatchupSubscriptionModel(secondLive, eventStore, CRITERIA, new CatchupSubscriptionModelConfig(100));
        try {
            second.subscribe("sub", DcbStartAt.checkpoint(stored).toStartAt(), cloudEvent -> received.add(nameOf(cloudEvent))).waitUntilStarted();
            append(named("D"));

            // Then
            await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("the restarted subscription is delivering").contains("C", "D"));
            LateCommitGuarantee.require(received.contains("A"),
                    "the event that committed below the delivered checkpoint is delivered after the restart",
                    () -> "the subscription was delivered " + received);
        } finally {
            second.shutdown();
        }
    }

    private SpringMongoSubscriptionModel liveModelThatRestartsOnLostHistory() {
        return new SpringMongoSubscriptionModel(mongoTemplate, SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));
    }

    private void dropChangeStreamHistoryUpToNow() {
        BsonTimestamp now = requireNonNull(mongoClient.getDatabase(DATABASE).runCommand(new Document("hostInfo", 1)).get("operationTime", BsonTimestamp.class));
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, now);
    }

    private static boolean arrives(List<String> received, String name) {
        try {
            await().atMost(AT_MOST).until(() -> received.contains(name));
            return true;
        } catch (ConditionTimeoutException e) {
            return false;
        }
    }

    private static void join(Thread thread) {
        try {
            thread.join(20_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void block(CountDownLatch latch) {
        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    private DcbAppendResult append(DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event)).stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse("name:" + nameOf(cloudEvent)))))
                .toList();
        return eventStore.append(cloudEvents);
    }
}
