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
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.HoldingMongoClient;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.testsupport.previousrelease.PreviousRelease;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.tck.ConformanceEvents.eventAt;

/**
 * A catch-up stores a marked checkpoint when its replay runs without the live delegate. Occurrent 0.34 would resume
 * the composite it stored before without the live start, so it must refuse the marked form, before it writes
 * anything, and this release must resume from it.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupMarkedCheckpointPreviousReleaseMongoTest {

    private static final Duration AT_MOST = Duration.ofSeconds(30);
    private static final String DATABASE = "markedcheckpoint";
    private static final String EVENTS = "events";
    private static final String CHECKPOINTS = "checkpoints";
    private static final String SUBSCRIPTION = "sub";
    private static final OffsetDateTime TIME = OffsetDateTime.of(2026, 10, 1, 12, 0, 0, 0, ZoneOffset.UTC);

    // Subscribes the way a 0.34 node resumes a subscription from storage
    private static final String READER = """
            import com.mongodb.client.MongoClient;
            import com.mongodb.client.MongoDatabase;
            import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
            import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
            import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
            import org.occurrent.subscription.StartAt;
            import org.occurrent.subscription.api.blocking.CheckpointStorage;
            import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
            import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
            import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
            import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
            import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
            import org.springframework.data.mongodb.core.MongoTemplate;

            import java.util.concurrent.ExecutorService;
            import java.util.concurrent.Executors;

            import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

            public class Reader {
                public static Object run(Object... args) {
                    MongoClient client = (MongoClient) args[0];
                    String database = (String) args[1];
                    boolean spring = (Boolean) args[2];
                    boolean writesPosition = (Boolean) args[3];
                    MongoDatabase db = client.getDatabase(database);
                    CheckpointStorage storage = spring ? new SpringMongoCheckpointStorage(new MongoTemplate(client, database), "checkpoints")
                            : new NativeMongoCheckpointStorage(db, "checkpoints");
                    EventStoreConfig.Builder config = new EventStoreConfig.Builder().timeRepresentation(TimeRepresentation.RFC_3339_STRING);
                    MongoEventStore eventStore = new MongoEventStore(client, database, "events",
                            (writesPosition ? config.withStreamPosition() : config.withoutStreamPosition()).build());
                    ExecutorService executor = Executors.newCachedThreadPool();
                    StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(
                            new NativeMongoSubscriptionModel(db, "events", TimeRepresentation.RFC_3339_STRING, executor), eventStore,
                            new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
                    try {
                        catchup.subscribe("sub", StartAt.subscriptionModelDefault(), event -> {
                        }).waitUntilStarted();
                        return null;
                    } finally {
                        catchup.shutdown();
                        executor.shutdownNow();
                    }
                }
            }
            """;

    enum Storage {NATIVE, SPRING}

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, DATABASE));

    private static PreviousRelease.Driver reader;

    private final CountDownLatch crashed = new CountDownLatch(1);
    private final CountDownLatch releaseLateCommit = new CountDownLatch(1);
    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<StreamCatchupSubscriptionModel> models = new ArrayList<>();
    private MongoClient mongoClient;
    private String databaseName;
    private MongoDatabase database;

    @BeforeAll
    static void compile_the_0_34_reader() {
        reader = PreviousRelease.fromJarsIn(Path.of("target", "occurrent-0.34.0")).compile("Reader", READER);
    }

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        database = mongoClient.getDatabase(databaseName);
    }

    @AfterEach
    void shutdown() {
        crashed.countDown();
        releaseLateCommit.countDown();
        models.forEach(StreamCatchupSubscriptionModel::shutdown);
        executors.forEach(ExecutorService::shutdownNow);
        mongoClient.close();
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    void a_position_a_replay_without_the_live_delegate_stored_is_refused_by_0_34_and_resumed_by_this_release(Storage storage) {
        // Given
        MongoEventStore eventStore = eventStore(true);
        String stored = storeMarkedCheckpointByStoppingInTheReplay(eventStore, storage, StartAt.checkpoint(GlobalCheckpoint.of(0)));
        assertThat(CatchupCheckpoints.isMarked(new StringBasedCheckpoint(stored)) && !CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(stored)))
                .as("the position the replay stored, %s, is the marked form of a position replay", stored).isTrue();

        // When
        // Then
        verifyRefusedBy034AndResumedByThisRelease(eventStore, storage, stored, true);
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    void a_time_a_replay_without_the_live_delegate_stored_on_a_store_writing_no_positions_is_refused_by_0_34_and_resumed_by_this_release_by_time(Storage storage) {
        // Given
        MongoEventStore eventStore = eventStore(false);
        String stored = storeMarkedCheckpointByStoppingInTheReplay(eventStore, storage, StartAtTime.beginningOfTime());
        assertThat(CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(stored)))
                .as("the time the replay stored, %s, is the marked form of a time replay", stored).isTrue();

        // When
        // Then
        verifyRefusedBy034AndResumedByThisRelease(eventStore, storage, stored, false);
    }

    @Test
    void a_time_marked_form_restored_as_an_explicit_start_on_a_store_writing_no_positions_replays_by_time() {
        // Given
        MongoEventStore eventStore = eventStore(false);
        String stored = storeMarkedCheckpointByStoppingInTheReplay(eventStore, Storage.NATIVE, StartAtTime.beginningOfTime());
        assertThat(CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(stored))).as("the time the replay stored, %s", stored).isTrue();
        List<String> received = new CopyOnWriteArrayList<>();

        // When
        StreamCatchupSubscriptionModel restored = catchup(eventStore, new NativeMongoCheckpointStorage(database, "restored-" + CHECKPOINTS));
        restored.subscribe("restored", StartAt.checkpoint(new StringBasedCheckpoint(stored)), event -> received.add(event.getId())).waitUntilStarted();
        eventStore.write("stream-d", List.of(eventAt("D", "Happened", TIME.plusMinutes(3))));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the restored subscription received").contains("D"));

        // Then
        assertThat(received).as("the events after the restored watermark, replayed by time rather than skipped by a start live from now")
                .contains("B", "C");
    }

    @Test
    void a_no_live_resume_from_a_marked_form_0_34_refused_goes_live_from_the_stored_live_start_and_delivers_a_late_commit_once() {
        // Given
        MongoEventStore eventStore = eventStore(true);
        CheckpointStorage checkpoints = checkpointStorage(Storage.NATIVE);
        StartAt resumesWithoutTheLiveDelegate = resumingFromStorageWithoutTheLiveDelegate(checkpoints);
        LateCommit lateCommit = storeMarkedCheckpointWhileALateCommitIsHeld(eventStore, checkpoints, resumesWithoutTheLiveDelegate);
        assertThatThrownBy(() -> reader.run(mongoClient, databaseName, false, true))
                .as("what 0.34 throws when it resumes from the marked form")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a global checkpoint: " + lateCommit.stored());
        assertThat(requireNonNull(checkpoints.read(SUBSCRIPTION)).asString()).as("the checkpoint after 0.34 refused it").isEqualTo(lateCommit.stored());
        lateCommit.release();
        List<String> received = new CopyOnWriteArrayList<>();

        // When
        StreamCatchupSubscriptionModel restarted = catchup(eventStore, checkpoints);
        restarted.subscribe(SUBSCRIPTION, resumesWithoutTheLiveDelegate, event -> received.add(event.getId())).waitUntilStarted();
        eventStore.write("stream-d", List.of(eventAt("D", "Happened", TIME.plusMinutes(3))));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the restarted subscription received").contains("D"));

        // Then
        assertThat(received).as("what the restart delivered, the late commit below the stored position from the stored live start")
                .containsOnlyOnce("A").contains("C").doesNotContain("B");
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(checkpoints.read(SUBSCRIPTION)).as("the temporary checkpoint after the replay without the live delegate").isNull());
    }

    @Test
    void a_live_start_over_a_marked_form_a_replay_without_the_live_delegate_stored_keeps_its_live_start() {
        // Given
        MongoEventStore eventStore = eventStore(true);
        CheckpointStorage checkpoints = checkpointStorage(Storage.NATIVE);
        LateCommit lateCommit = storeMarkedCheckpointWhileALateCommitIsHeld(eventStore, checkpoints, resumingFromStorageWithoutTheLiveDelegate(checkpoints));
        lateCommit.release();
        List<String> received = new CopyOnWriteArrayList<>();

        // When
        StreamCatchupSubscriptionModel restarted = catchup(eventStore, checkpoints);
        restarted.subscribe(SUBSCRIPTION, StartAt.subscriptionModelDefault(), event -> received.add(event.getId())).waitUntilStarted();
        eventStore.write("stream-d", List.of(eventAt("D", "Happened", TIME.plusMinutes(3))));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the restarted subscription received").contains("D"));

        // Then
        assertThat(received).as("what the restart delivered, the late commit below the stored position from the stored live start")
                .containsOnlyOnce("A").contains("C").doesNotContain("B");
        assertThat(requireNonNull(checkpoints.read(SUBSCRIPTION)).asString()).as("the checkpoint stored at the handover")
                .isEqualTo(requireNonNull(CatchupCheckpoints.parse(new StringBasedCheckpoint(lateCommit.stored())).liveFrom()).asString());
    }

    // A user's start that resumes from storage when it holds a checkpoint, and from the beginning otherwise, and never
    // runs the live delegate
    private static StartAt resumingFromStorageWithoutTheLiveDelegate(CheckpointStorage checkpoints) {
        return StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(NativeMongoSubscriptionModel.class)) {
                return null;
            }
            return checkpoints.read(SUBSCRIPTION) == null ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : StartAt.subscriptionModelDefault();
        });
    }

    private record LateCommit(String stored, CountDownLatch releaseLateCommit, Future<?> write) {
        void release() {
            releaseLateCommit.countDown();
            try {
                write.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("The late commit failed", e);
            }
        }
    }

    // A reserves position 1 and is held before it commits, while B and C commit at 2 and 3. A replay without the live
    // delegate delivers B, stores the marked form for it, and stops while delivering C. Once released, A commits after
    // the live start that replay read.
    private LateCommit storeMarkedCheckpointWhileALateCommitIsHeld(MongoEventStore eventStore, CheckpointStorage checkpoints, StartAt startAt) {
        CountDownLatch aReserved = new CountDownLatch(1);
        AtomicBoolean held = new AtomicBoolean();
        MongoClient holdingClient = HoldingMongoClient.beforeEachSession(mongoClient, () -> {
            if (held.compareAndSet(false, true)) {
                aReserved.countDown();
                awaitLatch(releaseLateCommit);
            }
        });
        EventStoreConfig config = new EventStoreConfig.Builder().timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).withStreamPosition().build();
        MongoEventStore holdingStore = new MongoEventStore(holdingClient, databaseName, EVENTS, config);
        ExecutorService writer = Executors.newSingleThreadExecutor();
        executors.add(writer);
        Future<?> writeA = writer.submit(() -> holdingStore.write("stream-a", List.of(eventAt("A", "Happened", TIME))));
        awaitLatch(aReserved);
        eventStore.write("stream-b", List.of(eventAt("B", "Happened", TIME.plusMinutes(1))));
        eventStore.write("stream-c", List.of(eventAt("C", "Happened", TIME.plusMinutes(2))));

        StreamCatchupSubscriptionModel first = catchup(eventStore, checkpoints);
        CountDownLatch deliveringC = new CountDownLatch(1);
        first.subscribe(SUBSCRIPTION, startAt, (CloudEvent event) -> {
            if (event.getId().equals("C")) {
                deliveringC.countDown();
                awaitLatch(crashed);
            }
        });
        awaitLatch(deliveringC);
        first.shutdown();
        String stored = requireNonNull(checkpoints.read(SUBSCRIPTION), "a checkpoint stored for B").asString();
        assertThat(CatchupCheckpoints.parse(new StringBasedCheckpoint(stored))).as("the checkpoint the replay stored for B, %s", stored)
                .isInstanceOfSatisfying(CatchupCheckpoints.Position.class, position -> assertThat(position.position()).as("its position").isEqualTo(2));
        return new LateCommit(stored, releaseLateCommit, writeA);
    }

    private void verifyRefusedBy034AndResumedByThisRelease(MongoEventStore eventStore, Storage storage, String stored, boolean writesPosition) {
        Document document = requireNonNull(database.getCollection(CHECKPOINTS).find(new Document("_id", SUBSCRIPTION)).first());
        assertThat(document.getString("checkpoint")).as("the whole marked form in the checkpoint field").isEqualTo(stored);
        assertThat(document.keySet()).as("the fields of the stored document").noneMatch(field -> field.startsWith("catchup"));

        assertThatThrownBy(() -> reader.run(mongoClient, databaseName, storage == Storage.SPRING, writesPosition))
                .as("what 0.34 throws when it resumes from the marked form")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a global checkpoint: " + stored);
        assertThat(database.getCollection(CHECKPOINTS).find().into(new ArrayList<>()))
                .as("the checkpoints after 0.34 refused the marked form").containsExactly(document);

        List<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel restarted = catchup(eventStore, checkpointStorage(storage));
        Subscription subscription = restarted.subscribe(SUBSCRIPTION, StartAt.subscriptionModelDefault(), event -> received.add(event.getId()));
        subscription.waitUntilStarted();
        eventStore.write("stream-d", List.of(eventAt("D", "Happened", TIME.plusMinutes(3))));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the restarted subscription received").contains("D"));
        assertThat(received).as("the events after the stored checkpoint, replayed from it").contains("B", "C");
    }

    // Stores the marked form the way a crash leaves it, a replay without the live delegate that stored a checkpoint
    // for A and stopped while delivering B
    private String storeMarkedCheckpointByStoppingInTheReplay(MongoEventStore eventStore, Storage storage, StartAt origin) {
        eventStore.write("stream-a", List.of(eventAt("A", "Happened", TIME)));
        eventStore.write("stream-b", List.of(eventAt("B", "Happened", TIME.plusMinutes(1))));
        eventStore.write("stream-c", List.of(eventAt("C", "Happened", TIME.plusMinutes(2))));
        CheckpointStorage checkpoints = checkpointStorage(storage);
        StreamCatchupSubscriptionModel first = catchup(eventStore, checkpoints);
        CountDownLatch deliveringB = new CountDownLatch(1);
        StartAt withoutTheLiveDelegate = StartAt.dynamic(context -> context.hasSubscriptionModelType(NativeMongoSubscriptionModel.class) ? null : origin);

        first.subscribe(SUBSCRIPTION, withoutTheLiveDelegate, (CloudEvent event) -> {
            if (event.getId().equals("B")) {
                deliveringB.countDown();
                awaitLatch(crashed);
            }
        });

        awaitLatch(deliveringB);
        first.shutdown();
        Checkpoint stored = requireNonNull(checkpoints.read(SUBSCRIPTION), "a checkpoint stored for A");
        return stored.asString();
    }

    private MongoEventStore eventStore(boolean writesPosition) {
        EventStoreConfig.Builder config = new EventStoreConfig.Builder().timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM);
        return new MongoEventStore(mongoClient, databaseName, EVENTS, (writesPosition ? config.withStreamPosition() : config.withoutStreamPosition()).build());
    }

    private CheckpointStorage checkpointStorage(Storage storage) {
        return storage == Storage.NATIVE ? new NativeMongoCheckpointStorage(database, CHECKPOINTS) : new SpringMongoCheckpointStorage(new MongoTemplate(mongoClient, databaseName), CHECKPOINTS);
    }

    private StreamCatchupSubscriptionModel catchup(MongoEventStore eventStore, CheckpointStorage storage) {
        ExecutorService executor = Executors.newCachedThreadPool();
        executors.add(executor);
        NativeMongoSubscriptionModel live = new NativeMongoSubscriptionModel(database, EVENTS, TimeRepresentation.RFC_3339_STRING, executor);
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(live, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        models.add(model);
        return model;
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(60, TimeUnit.SECONDS)).as("the latch was counted down in time").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
