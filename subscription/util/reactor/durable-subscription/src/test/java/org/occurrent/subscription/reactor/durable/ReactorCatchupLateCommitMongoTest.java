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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A Mongo event store reserves an event's position before its transaction commits, so an event at a lower position can
 * become visible after one at a higher position. A durable catch-up subscription that stored a position past such an
 * event, and then restarted, must still deliver it, on the named path the reactive Spring Boot starter wires.
 * <p>
 * An event written while the catch-up was down, after it read its live start, must reach it once after the restart,
 * also when MongoDB drops the change stream history from that live start while the resumed replay runs. The container
 * has a small oplog so a test can push that history out.
 */
@Testcontainers
@Timeout(240)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactorCatchupLateCommitMongoTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String DATABASE = "reactorlatecommit";

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private static MongoClient mongoClient;

    private final AtomicBoolean holdNextCommit = new AtomicBoolean();
    private final CountDownLatch heldInCommit = new CountDownLatch(1);
    private final CountDownLatch releaseHeld = new CountDownLatch(1);
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private String eventCollectionName;
    private String checkpointCollectionName;

    // Holds the next commit after the store reserved its position and inserted its documents inside the transaction,
    // so they stay invisible to every other reader until releaseHeld
    class HoldingTransactionManager extends ReactiveMongoTransactionManager {
        HoldingTransactionManager(ReactiveMongoDatabaseFactory factory) {
            super(factory);
        }

        @Override
        protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager, ReactiveMongoTransactionObject transactionObject) {
            Mono<Void> commit = super.doCommit(synchronizationManager, transactionObject);
            if (!holdNextCommit.compareAndSet(true, false)) {
                return commit;
            }
            return Mono.fromRunnable(() -> {
                        heldInCommit.countDown();
                        awaitOrFail(releaseHeld, 60);
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .then(commit);
        }
    }

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void create_collections() {
        eventCollectionName = "events-" + UUID.randomUUID();
        checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
        reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, DATABASE);
    }

    @AfterEach
    void release() {
        releaseHeld.countDown();
    }

    @Test
    void a_stream_event_whose_lower_position_commits_after_a_catch_up_checkpoint_passed_it_is_delivered_after_a_restart() {
        ReactorMongoEventStore eventStore = eventStore(STREAM);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.write("stream-" + event.getId(), Flux.just(event)).then();

        verifyLateCommitIsDeliveredAfterRestart(append,
                live -> new ReactorCatchupSubscriptionModel(live, eventStore, Filter.all()));
    }

    @Test
    void a_dcb_event_whose_lower_position_commits_after_a_catch_up_checkpoint_passed_it_is_delivered_after_a_restart() {
        ReactorMongoEventStore eventStore = eventStore(STREAM, DCB);
        // Each event has a type and a tag of its own, so the events share no conflict marker and B and C commit while A
        // is held
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.append(List.of(DcbCloudEvents.withTags(event, List.of(Tag.parse("id:" + event.getId()))))).then();

        verifyLateCommitIsDeliveredAfterRestart(append,
                live -> new ReactorCatchupSubscriptionModel(live, (DcbEventStore) eventStore, DcbCriteria.all()));
    }

    @Test
    void a_stream_event_written_while_a_live_catch_up_was_down_before_it_stored_a_live_event_is_delivered_once_after_a_restart() {
        ReactorMongoEventStore eventStore = eventStore(STREAM);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.write("stream-" + event.getId(), Flux.just(event)).then();

        verifyEventWrittenInTheQuietWindowIsDeliveredOnce(append,
                live -> new ReactorCatchupSubscriptionModel(live, eventStore, Filter.all()));
    }

    @Test
    void a_dcb_event_written_while_a_live_catch_up_was_down_before_it_stored_a_live_event_is_delivered_once_after_a_restart() {
        ReactorMongoEventStore eventStore = eventStore(STREAM, DCB);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.append(List.of(DcbCloudEvents.withTags(event, List.of(Tag.parse("id:" + event.getId()))))).then();

        verifyEventWrittenInTheQuietWindowIsDeliveredOnce(append,
                live -> new ReactorCatchupSubscriptionModel(live, (DcbEventStore) eventStore, DcbCriteria.all()));
    }

    @Test
    void a_stream_event_written_while_a_catch_up_was_down_is_delivered_when_the_live_start_leaves_the_change_stream_history_during_the_resumed_replay() {
        ReactorMongoEventStore eventStore = eventStore(STREAM);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.write("stream-" + event.getId(), Flux.just(event)).then();

        verifyEventWrittenWhileDownIsDeliveredWhenTheLiveStartLeavesTheHistory(append,
                live -> new ReactorCatchupSubscriptionModel(live, eventStore, Filter.all()));
    }

    @Test
    void a_dcb_event_written_while_a_catch_up_was_down_is_delivered_when_the_live_start_leaves_the_change_stream_history_during_the_resumed_replay() {
        ReactorMongoEventStore eventStore = eventStore(STREAM, DCB);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.append(List.of(DcbCloudEvents.withTags(event, List.of(Tag.parse("id:" + event.getId()))))).then();

        verifyEventWrittenWhileDownIsDeliveredWhenTheLiveStartLeavesTheHistory(append,
                live -> new ReactorCatchupSubscriptionModel(live, (DcbEventStore) eventStore, DcbCriteria.all()));
    }

    @Test
    void a_stream_catch_up_started_the_way_the_spring_boot_starter_starts_it_resumes_a_replay_that_stopped_in_the_middle() {
        ReactorMongoEventStore eventStore = eventStore(STREAM);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.write("stream-" + event.getId(), Flux.just(event)).then();

        verifyTheStarterShapedStartResumesAReplayThatStoppedInTheMiddle(append,
                live -> new ReactorCatchupSubscriptionModel(live, eventStore, Filter.all()));
    }

    @Test
    void a_dcb_catch_up_started_the_way_the_spring_boot_starter_starts_it_resumes_a_replay_that_stopped_in_the_middle() {
        ReactorMongoEventStore eventStore = eventStore(STREAM, DCB);
        Function<CloudEvent, Mono<Void>> append = event -> eventStore.append(List.of(DcbCloudEvents.withTags(event, List.of(Tag.parse("id:" + event.getId()))))).then();

        verifyTheStarterShapedStartResumesAReplayThatStoppedInTheMiddle(append,
                live -> new ReactorCatchupSubscriptionModel(live, (DcbEventStore) eventStore, DcbCriteria.all()));
    }

    private void verifyTheStarterShapedStartResumesAReplayThatStoppedInTheMiddle(Function<CloudEvent, Mono<Void>> append,
                                                                                 Function<ReactorMongoSubscriptionModel, CheckpointAwareSubscriptionModel> catchupOver) {
        append.apply(event("A")).block(TIMEOUT);
        append.apply(event("B")).block(TIMEOUT);
        append.apply(event("C")).block(TIMEOUT);

        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        // The start the starter gives a subscription that starts at the beginning and resumes the default way
        StartAt startAt = StartAt.dynamic(ctx -> storage.read("sub").blockOptional().isPresent() ? StartAt.subscriptionModelDefault() : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        List<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A, stores position 1, then the process dies while handling B
        ReactorDurableSubscriptionModel first = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        CountDownLatch handlingB = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", null, startAt, cloudEvent -> Mono.fromRunnable(() -> {
            received.add(cloudEvent.getId());
            if (cloudEvent.getId().equals("B")) {
                handlingB.countDown();
                awaitOrFail(crashed, 60);
            }
        }));
        awaitOrFail(handlingB, 20);
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub").block(TIMEOUT)))).isEqualTo(1);
        first.shutdown();
        crashed.countDown();

        // W commits after the first process read its live start
        append.apply(event("W")).block(TIMEOUT);

        ReactorDurableSubscriptionModel second = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        try {
            second.subscribe("sub", null, startAt, cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())))
                    .waitUntilStarted().block(TIMEOUT);
            append.apply(event("D")).block(TIMEOUT);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "W", "D"));
        } finally {
            second.shutdown();
        }
    }

    private void verifyEventWrittenWhileDownIsDeliveredWhenTheLiveStartLeavesTheHistory(Function<CloudEvent, Mono<Void>> append,
                                                                                         Function<ReactorMongoSubscriptionModel, CheckpointAwareSubscriptionModel> catchupOver) {
        append.apply(event("A")).block(TIMEOUT);
        append.apply(event("B")).block(TIMEOUT);
        append.apply(event("C")).block(TIMEOUT);

        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        List<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A, stores position 1, then the process dies while handling B
        ReactorDurableSubscriptionModel first = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModelThatRestartsOnLostHistory()), storage);
        CountDownLatch handlingB = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", null, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> {
            received.add(cloudEvent.getId());
            if (cloudEvent.getId().equals("B")) {
                handlingB.countDown();
                awaitOrFail(crashed, 60);
            }
        }));
        awaitOrFail(handlingB, 20);
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub").block(TIMEOUT)))).isEqualTo(1);
        first.shutdown();
        crashed.countDown();

        // W commits after the first process read its live start
        append.apply(event("W")).block(TIMEOUT);

        // Second process: the resumed replay checks the stored live start, then MongoDB drops the history from it while
        // the replay handles the first event it delivers, B or C depending on whether the first process stored B on its
        // way down
        ReactorDurableSubscriptionModel second = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModelThatRestartsOnLostHistory()), storage);
        CountDownLatch handlingTheFirstResumedEvent = new CountDownLatch(1);
        CountDownLatch historyDropped = new CountDownLatch(1);
        try {
            Subscription subscription = second.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.fromRunnable(() -> {
                received.add(cloudEvent.getId());
                if (handlingTheFirstResumedEvent.getCount() > 0) {
                    handlingTheFirstResumedEvent.countDown();
                    awaitOrFail(historyDropped, 200);
                }
            }));
            awaitOrFail(handlingTheFirstResumedEvent, 20);
            dropChangeStreamHistoryUpToNow();
            historyDropped.countDown();
            subscription.waitUntilStarted().block(Duration.ofSeconds(60));
            append.apply(event("D")).block(TIMEOUT);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "W", "D"));
        } finally {
            second.shutdown();
        }
    }

    private void verifyEventWrittenInTheQuietWindowIsDeliveredOnce(Function<CloudEvent, Mono<Void>> append,
                                                                    Function<ReactorMongoSubscriptionModel, CheckpointAwareSubscriptionModel> catchupOver) {
        append.apply(event("A")).block(TIMEOUT);
        append.apply(event("B")).block(TIMEOUT);

        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        List<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers A and B and goes live, then the process stops before any live event
        ReactorDurableSubscriptionModel first = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        try {
            first.subscribe("sub", null, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())))
                    .waitUntilStarted().block(TIMEOUT);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).containsExactly("A", "B"));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(GlobalCheckpoint.positionOf(requireNonNull(storage.read("sub").block(TIMEOUT)))).isEqualTo(2));
        } finally {
            first.shutdown();
        }

        // W commits after the first process read its live start
        append.apply(event("W")).block(TIMEOUT);

        ReactorDurableSubscriptionModel second = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        try {
            second.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())))
                    .waitUntilStarted().block(TIMEOUT);
            // D commits after W, so once D is in, every delivery of W is in
            append.apply(event("D")).block(TIMEOUT);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("D"));
            assertThat(received.stream().filter("W"::equals)).as("deliveries of W, which committed while the catch-up was down").hasSize(1);
        } finally {
            second.shutdown();
        }
    }

    private void verifyLateCommitIsDeliveredAfterRestart(Function<CloudEvent, Mono<Void>> append,
                                                         Function<ReactorMongoSubscriptionModel, CheckpointAwareSubscriptionModel> catchupOver) {
        // A reserves position 1 and holds its transaction open
        holdNextCommit.set(true);
        Disposable writingA = append.apply(event("A")).subscribe();
        awaitOrFail(heldInCommit, 20);
        // B and C take positions 2 and 3 and commit
        append.apply(event("B")).block(TIMEOUT);
        append.apply(event("C")).block(TIMEOUT);

        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        List<String> received = new CopyOnWriteArrayList<>();

        // First process: the replay delivers B, stores position 2, then the process dies while handling C
        ReactorDurableSubscriptionModel first = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        CountDownLatch handlingC = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        first.subscribe("sub", null, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> {
            received.add(cloudEvent.getId());
            if (cloudEvent.getId().equals("C")) {
                handlingC.countDown();
                awaitOrFail(crashed, 60);
            }
        }));
        awaitOrFail(handlingC, 20);
        Checkpoint storedBeforeTheCrash = storage.read("sub").block(TIMEOUT);
        assertThat(GlobalCheckpoint.positionOf(requireNonNull(storedBeforeTheCrash))).isEqualTo(2);
        first.shutdown();
        crashed.countDown();

        // A commits at position 1, below the stored checkpoint
        releaseHeld.countDown();
        await().atMost(TIMEOUT).until(writingA::isDisposed);

        // Second process resumes from storage
        ReactorDurableSubscriptionModel second = new ReactorDurableSubscriptionModel(catchupOver.apply(liveModel()), storage);
        try {
            second.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())))
                    .waitUntilStarted().block(TIMEOUT);
            append.apply(event("D")).block(TIMEOUT);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("D"));
            assertThat(received).as("every committed event reaches the subscription").contains("A", "B", "C", "D");
        } finally {
            second.shutdown();
        }
    }

    private ReactorMongoEventStore eventStore(EventStoreCapability first, EventStoreCapability... rest) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new HoldingTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(first, rest)
                .build();
        return new ReactorMongoEventStore(reactiveMongoTemplate, config);
    }

    private ReactorMongoSubscriptionModel liveModel() {
        return new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    private ReactorMongoSubscriptionModel liveModelThatRestartsOnLostHistory() {
        return new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true));
    }

    private void dropChangeStreamHistoryUpToNow() {
        Document hostInfo = requireNonNull(Mono.from(mongoClient.getDatabase(DATABASE).runCommand(new Document("hostInfo", 1))).block(TIMEOUT));
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, requireNonNull(hostInfo.get("operationTime", BsonTimestamp.class)));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("test-event-" + id)
                .withTime(OffsetDateTime.now())
                .build();
    }

    private static void awaitOrFail(CountDownLatch latch, long seconds) {
        try {
            if (!latch.await(seconds, SECONDS)) {
                throw new AssertionError("Latch was not released within " + seconds + " seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting on a latch", e);
        }
    }
}
