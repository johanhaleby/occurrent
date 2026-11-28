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

package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.occurrent.filter.Filter.type;

/**
 * A competing consumer model directly over a native model, with no durable model storing a restart position, when the
 * native model holds a subscription paused because its restart from the present after lost history is refused. The
 * wrapped model pauses it as a history loss that a listener takes over does, and the competing consumer model listens
 * to neither. This records what the node does with the lease and the subscription then.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerHoldsARestartRefusedAfterHistoryLostTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration LEASE_TIME = Duration.ofSeconds(3);
    private static final String SUBSCRIPTION_ID = "s";

    private final AtomicBoolean historyLost = new AtomicBoolean();
    private final AtomicBoolean presentRefused = new AtomicBoolean();
    private final AtomicInteger reads = new AtomicInteger();
    // Open until a test closes them. The first listener on the wrapped model runs before the one of the node and blocks on these
    private volatile CountDownLatch heldPausedGate = new CountDownLatch(0);
    private volatile CountDownLatch refusalGate = new CountDownLatch(0);
    private final CountDownLatch heldPausedReachedTheTestListener = new CountDownLatch(1);
    private final CountDownLatch refusalReachedTheTestListener = new CountDownLatch(1);
    private final CountDownLatch heldPausedReachedTheListenerAfterTheNode = new CountDownLatch(1);
    // The refusals the test listener lets through to the node before it blocks on refusalGate
    private final AtomicInteger refusalsTold = new AtomicInteger();
    private final AtomicInteger heldPausedTold = new AtomicInteger();
    private volatile int refusalsLetThrough;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private String eventCollectionName;
    private NativeMongoLeaseCompetingConsumerStrategy strategyA;
    private NativeMongoLeaseCompetingConsumerStrategy strategyB;
    private NativeMongoSubscriptionModel wrappedA;
    private CompetingConsumerSubscriptionModel nodeA;
    private CompetingConsumerSubscriptionModel nodeB;

    @BeforeEach
    void two_nodes_on_one_lease_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        String locks = "locks-" + UUID.randomUUID();
        strategyA = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        strategyB = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locks).leaseTime(LEASE_TIME).build();
        nodeAOverAWrappedModelThatRestartsOnHistoryLoss(true);
        nodeB = new CompetingConsumerSubscriptionModel(new NativeMongoSubscriptionModel(database, eventCollectionName, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool()), strategyB);
    }

    private void nodeAOverAWrappedModelThatRestartsOnHistoryLoss(boolean restarts) {
        wrappedA = new NativeMongoSubscriptionModel(databaseThatSendsNoOperationTime(), collectionThatLosesItsHistoryAndRefusesThePresent(), TimeRepresentation.RFC_3339_STRING,
                Executors.newCachedThreadPool(), NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(restarts)
                        .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))).maxAwaitTime(Duration.ofSeconds(1)));
        wrappedA.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
                if (refusalsTold.incrementAndGet() > refusalsLetThrough) {
                    refusalReachedTheTestListener.countDown();
                    awaitUninterruptibly(refusalGate);
                }
            }

            @Override
            public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
                heldPausedTold.incrementAndGet();
                heldPausedReachedTheTestListener.countDown();
                awaitUninterruptibly(heldPausedGate);
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            }

            @Override
            public void handleStoredPositionMovedEarlier(String subscriptionId, CheckpointWriteConditionNotFulfilledException refusal) {
            }
        });
        nodeA = new CompetingConsumerSubscriptionModel(wrappedA, strategyA);
        wrappedA.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
                heldPausedReachedTheListenerAfterTheNode.countDown();
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            }

            @Override
            public void handleStoredPositionMovedEarlier(String subscriptionId, CheckpointWriteConditionNotFulfilledException refusal) {
            }
        });
    }

    @AfterEach
    void shutdown() {
        heldPausedGate.countDown();
        refusalGate.countDown();
        nodeA.shutdown();
        nodeB.shutdown();
        mongoClient.close();
    }

    @Timeout(value = 90, unit = SECONDS)
    @Test
    void a_node_gives_the_lease_back_while_its_wrapped_model_holds_the_subscription_paused_after_a_refused_restart_and_competes_again_only_after_a_resume() {
        // Given
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease before the history is lost").isTrue();

        // When the history is lost and the restart from the present is refused
        presentRefused.set(true);
        historyLost.set(true);
        await().atMost(15, SECONDS).until(() -> wrappedA.isPaused(SUBSCRIPTION_ID));

        // Then A gives the lease back and is paused
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back").isFalse());
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused").isTrue();
        assertThat(nodeA.isRunning(SUBSCRIPTION_ID)).as("node A is running").isFalse();
        int readsWhenHeld = reads.get();

        // And A does not hold the lease again, nor ask MongoDB for the present again, over four lease times with the lease free
        await().during(LEASE_TIME.multipliedBy(4)).atMost(LEASE_TIME.multipliedBy(4).plusSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> !strategyA.hasLock(SUBSCRIPTION_ID, "A"));
        assertThat(reads.get()).as("reads of MongoDB for the present while held, after four lease times").isEqualTo(readsWhenHeld);

        // And B gets the lease when it subscribes the same id, and its own wrapped model starts it, since its present is not refused
        Subscription b = nodeB.subscribe("B", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyB.hasLock(SUBSCRIPTION_ID, "B")).as("B gets the lease").isTrue());
        assertThat(b.waitUntilStarted(Duration.ofSeconds(10))).as("B starts").isTrue();
        assertThat(nodeB.isRunning(SUBSCRIPTION_ID)).as("node B is running").isTrue();
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is still paused").isTrue();
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is still held paused").isTrue();

        // When A is resumed and the present is no longer refused
        presentRefused.set(false);
        nodeA.resumeSubscription(SUBSCRIPTION_ID);

        // Then A does not take the lease from B while B holds it
        await().during(LEASE_TIME.multipliedBy(2)).atMost(LEASE_TIME.multipliedBy(2).plusSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> strategyB.hasLock(SUBSCRIPTION_ID, "B") && !strategyA.hasLock(SUBSCRIPTION_ID, "A"));

        // And A gets the lease and runs once B is cancelled, so A competes for it again
        nodeB.cancelSubscription(SUBSCRIPTION_ID);
        await().atMost(20, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gets the lease after the resume").isTrue());
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(nodeA.isRunning(SUBSCRIPTION_ID)).as("node A is running after the resume").isTrue());
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void waiting_without_a_timeout_on_a_resume_of_the_held_subscription_answers_with_the_refusal_instead_of_waiting_forever() {
        // Given
        Subscription resumed = resumeTheHeldSubscriptionOnNodeA();

        // When
        // Then
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThatThrownBy(resumed::waitUntilStarted).isInstanceOf(UnsupportedStartAtException.class));
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is held paused").isTrue();
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused").isTrue();
        assertThat(nodeA.subscriptionIds()).as("node A still knows the subscription").contains(SUBSCRIPTION_ID);
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void waiting_with_a_timeout_on_a_resume_of_the_held_subscription_answers_with_the_refusal_well_before_the_timeout() {
        // Given
        Subscription resumed = resumeTheHeldSubscriptionOnNodeA();

        // When
        // Then
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThatThrownBy(() -> resumed.waitUntilStarted(Duration.ofSeconds(30))).isInstanceOf(UnsupportedStartAtException.class));
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is held paused").isTrue();
        assertThat(nodeA.subscriptionIds()).as("node A still knows the subscription").contains(SUBSCRIPTION_ID);
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_late_report_that_the_wrapped_model_held_a_cancelled_subscription_paused_does_not_end_the_wait_of_a_handle_for_the_same_id_subscribed_again_before_the_node_acted_on_its_refusal() throws Exception {
        // Given generation 1 held paused, with the wrapped model's report of it blocked in front of the node's listener
        heldPausedGate = new CountDownLatch(1);
        Subscription generation1 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(generation1.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        assertThat(heldPausedReachedTheTestListener.await(15, SECONDS)).as("the wrapped model reported that it held generation 1 paused").isTrue();
        assertThat(heldPausedReachedTheListenerAfterTheNode.getCount()).as("the node's listener has not run for generation 1 yet").isEqualTo(1);

        // And generation 1 cancelled and the id subscribed again, with a start the wrapped model refuses and the node not told of yet
        refusalGate = new CountDownLatch(1);
        nodeA.cancelSubscription(SUBSCRIPTION_ID);
        Subscription generation2 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), refusedStartAt(), (CloudEvent __) -> {
        });
        assertThat(refusalReachedTheTestListener.await(15, SECONDS)).as("the wrapped model refused the start of generation 2").isTrue();

        // When / Then
        theReportForGeneration1ReachesTheNodeWhileTheRefusalOfGeneration2IsHeld(generation2);
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_late_report_that_the_wrapped_model_held_a_cancelled_subscription_paused_does_not_hold_the_consumer_of_the_same_id_subscribed_again_and_it_keeps_competing() throws Exception {
        // Given generation 1 held paused, with the wrapped model's report of it blocked in front of the node's listener
        heldPausedGate = new CountDownLatch(1);
        Subscription generation1 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(generation1.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        assertThat(heldPausedReachedTheTestListener.await(15, SECONDS)).as("the wrapped model reported that it held generation 1 paused").isTrue();
        assertThat(heldPausedReachedTheListenerAfterTheNode.getCount()).as("the node's listener has not run for generation 1 yet").isEqualTo(1);

        // And generation 1 cancelled and the id subscribed again, which the wrapped model starts and the node runs under its lease
        nodeA.cancelSubscription(SUBSCRIPTION_ID);
        presentRefused.set(false);
        Subscription generation2 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(generation2.waitUntilStarted(Duration.ofSeconds(10))).as("generation 2 started").isTrue();
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease for generation 2").isTrue());

        // When the report for generation 1 reaches the node
        heldPausedGate.countDown();
        assertThat(heldPausedReachedTheListenerAfterTheNode.await(15, SECONDS)).as("the node's listener ran for generation 1").isTrue();

        // Then generation 2 is not held as paused, and keeps its lease over two lease times
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused").isFalse();
        assertThat(nodeA.isRunning(SUBSCRIPTION_ID)).as("node A is running").isTrue();
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is paused").isFalse();
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease right after the report").isTrue();
        await().during(LEASE_TIME.multipliedBy(2)).atMost(LEASE_TIME.multipliedBy(2).plusSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> strategyA.hasLock(SUBSCRIPTION_ID, "A") && nodeA.isRunning(SUBSCRIPTION_ID));
        assertThat(generation2.waitUntilStarted(Duration.ofSeconds(1))).as("the handle of generation 2 still answers that it started").isTrue();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_late_report_that_the_wrapped_model_held_a_subscription_paused_does_not_end_the_wait_of_a_handle_for_the_same_id_after_a_refused_resume_freed_the_id() throws Exception {
        // Given generation 1 held paused after a history loss, with the report of it blocked in front of the node's listener
        heldPausedGate = new CountDownLatch(1);
        Subscription generation1 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(generation1.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        assertThat(heldPausedReachedTheTestListener.await(15, SECONDS)).as("the wrapped model reported that it held generation 1 paused").isTrue();

        // And a resume that the wrapped model refuses, which the node acted on by giving the lease and the id back
        refusalsLetThrough = 1;
        refusalGate = new CountDownLatch(1);
        wrappedA.resumeSubscription(SUBSCRIPTION_ID, StartAt.dynamic(__ -> {
            throw new UnsupportedStartAtException(StartAt.checkpoint(GlobalCheckpoint.of(0)), "refused on purpose");
        }));
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back after the refusal").isFalse());

        // And the id subscribed again with a start the wrapped model refuses and the node not told of yet
        Subscription generation2 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), refusedStartAt(), (CloudEvent __) -> {
        });
        assertThat(refusalReachedTheTestListener.await(15, SECONDS)).as("the wrapped model refused the start of generation 2").isTrue();

        // When / Then
        theReportForGeneration1ReachesTheNodeWhileTheRefusalOfGeneration2IsHeld(generation2);
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_late_report_that_the_wrapped_model_held_a_subscription_paused_does_not_end_the_wait_of_a_handle_for_the_same_id_after_a_lost_history_refused_it_and_the_node_freed_the_id() throws Exception {
        // Given a wrapped model that does not restart on lost history, and generation 1 held paused at a resume at the present, with the report of it blocked
        nodeA.shutdown();
        nodeAOverAWrappedModelThatRestartsOnHistoryLoss(false);
        Subscription generation1 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(generation1.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        nodeA.pauseSubscription(SUBSCRIPTION_ID);
        heldPausedGate = new CountDownLatch(1);
        presentRefused.set(true);
        wrappedA.resumeSubscription(SUBSCRIPTION_ID, StartAt.now());
        assertThat(heldPausedReachedTheTestListener.await(15, SECONDS)).as("the wrapped model reported that it held generation 1 paused").isTrue();

        // And a resume whose run loses its history, which the wrapped model refuses, and the node acted on by giving the lease and the id back
        refusalsLetThrough = 1;
        presentRefused.set(false);
        historyLost.set(true);
        wrappedA.resumeSubscription(SUBSCRIPTION_ID, StartAt.now());
        await().atMost(15, SECONDS).until(() -> !wrappedA.isRunning(SUBSCRIPTION_ID) && !wrappedA.isPaused(SUBSCRIPTION_ID));
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back after the refusal").isFalse());

        // And the id subscribed again with a start the wrapped model refuses and the node not told of yet
        refusalGate = new CountDownLatch(1);
        Subscription generation2 = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), refusedStartAt(), (CloudEvent __) -> {
        });
        assertThat(refusalReachedTheTestListener.await(15, SECONDS)).as("the wrapped model refused the start of generation 2").isTrue();

        // When / Then
        theReportForGeneration1ReachesTheNodeWhileTheRefusalOfGeneration2IsHeld(generation2);
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void waiting_on_a_resume_that_is_held_a_second_time_after_the_first_hold_was_acted_on_keeps_waiting_for_the_node_to_act_on_the_second_hold() throws Exception {
        // Given a first hold the node acted on by giving the lease back
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        assertThat(heldPausedReachedTheListenerAfterTheNode.await(15, SECONDS)).as("the node's listener ran for the first hold").isTrue();
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back for the first hold").isFalse());
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused by the first hold").isTrue();

        // And a resume that gets the lease back for A and is held a second time, with the report of the second hold blocked in front of the node's listener
        heldPausedGate = new CountDownLatch(1);
        Subscription resumed = nodeA.resumeSubscription(SUBSCRIPTION_ID);
        await().atMost(15, SECONDS).until(() -> heldPausedTold.get() == 2);

        // When the handle of the resume is waited on while the node has not acted on the second hold
        CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> resumed.waitUntilStarted(Duration.ofSeconds(20)));

        // Then it keeps waiting, though the first hold was acted on
        assertThatThrownBy(() -> waiting.get(1, SECONDS))
                .as("[waitUntilStarted on the resume ended before the node acted on the second hold, which the first hold ended]")
                .isInstanceOf(TimeoutException.class);

        // And it throws the refusal once the node acted on the second hold, with the lease given back
        heldPausedGate.countDown();
        assertThatThrownBy(() -> waiting.get(10, SECONDS)).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(UnsupportedStartAtException.class);
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back for the second hold").isFalse();
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused by the second hold").isTrue();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void waiting_on_a_resume_that_is_held_answers_with_the_refusal_well_before_the_timeout_when_the_node_resumes_the_subscription_before_it_acts_on_the_report_of_the_hold() throws Exception {
        // Given a first hold the node acted on, and a resume that is held again, with the report of the hold blocked in front of the node's listener
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        assertThat(heldPausedReachedTheListenerAfterTheNode.await(15, SECONDS)).as("the node's listener ran for the first hold").isTrue();
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A gave the lease back for the first hold").isFalse());
        heldPausedGate = new CountDownLatch(1);
        Subscription resumed = nodeA.resumeSubscription(SUBSCRIPTION_ID);
        await().atMost(15, SECONDS).until(() -> heldPausedTold.get() == 2);
        CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> resumed.waitUntilStarted(Duration.ofSeconds(30)));

        // And the node pausing and resuming the subscription, which the wrapped model then runs since the present is no longer refused, before it acts on the report
        presentRefused.set(false);
        nodeA.pauseSubscription(SUBSCRIPTION_ID);
        nodeA.resumeSubscription(SUBSCRIPTION_ID);
        await().atMost(15, SECONDS).until(() -> wrappedA.isRunning(SUBSCRIPTION_ID));
        assertThat(waiting.isDone()).as("the handle is still waiting for the node to act on the report").isFalse();

        // When the report of the hold that the resume ended reaches the node
        heldPausedGate.countDown();

        // Then the handle throws the refusal well within its timeout
        assertThatThrownBy(() -> waiting.get(10, SECONDS)).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(UnsupportedStartAtException.class);

        // And the consumer is not held paused, and keeps its lease
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease").isTrue());
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused").isFalse();
        assertThat(nodeA.isRunning(SUBSCRIPTION_ID)).as("node A is running").isTrue();
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is paused").isFalse();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_handle_that_answered_started_throws_the_refusal_from_its_next_wait_when_the_wrapped_model_loses_its_history_on_a_later_read_and_does_not_restart() {
        // Given a wrapped model that does not restart on lost history, and a subscription whose wait answered started
        nodeA.shutdown();
        nodeAOverAWrappedModelThatRestartsOnHistoryLoss(false);
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).as("answer while the change stream was open").isTrue();

        // When its history is lost on a later read
        historyLost.set(true);
        await().atMost(15, SECONDS).until(() -> !wrappedA.isRunning(SUBSCRIPTION_ID) && !wrappedA.isPaused(SUBSCRIPTION_ID));

        // Then the next wait throws the refusal, with the MongoDB error beneath it
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(5)))
                .as("thrown by the timed wait").isExactlyInstanceOf(UnsupportedStartAtException.class)
                .rootCause().isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE)));
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThatThrownBy(started::waitUntilStarted)
                .as("thrown by the wait with no timeout").isExactlyInstanceOf(UnsupportedStartAtException.class));
    }

    private void theReportForGeneration1ReachesTheNodeWhileTheRefusalOfGeneration2IsHeld(Subscription generation2) throws Exception {
        // When the report for generation 1 reaches the node, and the node still has not been told of the refusal of generation 2
        heldPausedGate.countDown();
        assertThat(heldPausedReachedTheListenerAfterTheNode.await(15, SECONDS)).as("the node's listener ran for generation 1").isTrue();
        CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> generation2.waitUntilStarted(Duration.ofSeconds(20)));

        // Then the handle of generation 2 keeps waiting for the node to act on the refusal
        assertThatThrownBy(() -> waiting.get(1, SECONDS))
                .as("[waitUntilStarted on generation 2 ended before the node was told of its refusal, which a report for generation 1 ended]")
                .isInstanceOf(TimeoutException.class);

        // And it throws the refusal once the node acted on it, with the id free then
        refusalGate.countDown();
        assertThatThrownBy(() -> waiting.get(10, SECONDS)).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(UnsupportedStartAtException.class);
        presentRefused.set(false);
        Subscription again = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(again.waitUntilStarted(Duration.ofSeconds(10))).as("the id is free to subscribe again right after the handle threw").isTrue();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // A catch-up's checkpoint that a native model has no change stream position for
    private static StartAt refusedStartAt() {
        return StartAt.dynamic(context -> StartAt.checkpoint(GlobalCheckpoint.of(0)));
    }

    private Subscription resumeTheHeldSubscriptionOnNodeA() {
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        presentRefused.set(true);
        historyLost.set(true);
        await().atMost(15, SECONDS).until(() -> wrappedA.isPaused(SUBSCRIPTION_ID));
        return nodeA.resumeSubscription(SUBSCRIPTION_ID);
    }

    private static AgnosticSubscriptionFilter filter() {
        return AgnosticSubscriptionFilter.filter(type("never-written"));
    }

    private MongoDatabase databaseThatSendsNoOperationTime() {
        MongoDatabase wrapped = spy(database);
        doAnswer(invocation -> MongoCommons.CURRENT_OPERATION_TIME_COMMAND.equals(invocation.getArgument(0)) ? new Document("ok", 1.0) : invocation.callRealMethod())
                .when(wrapped).runCommand(any(Bson.class));
        return wrapped;
    }

    // The read that records the present, which has no pipeline, answers nothing once presentRefused is set and is counted then.
    // The change stream of a subscription fails with lost history the next time it is read once historyLost is set
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatLosesItsHistoryAndRefusesThePresent() {
        MongoCollection<Document> wrapped = spy(database.getCollection(eventCollectionName));
        doAnswer(invocation -> {
            List<Bson> pipeline = invocation.getArgument(0);
            if (pipeline.isEmpty()) {
                if (!presentRefused.get()) {
                    return invocation.callRealMethod();
                }
                reads.incrementAndGet();
                MongoChangeStreamCursor<ChangeStreamDocument<Document>> nothing = mock(MongoChangeStreamCursor.class);
                ChangeStreamIterable<Document> iterable = mock(ChangeStreamIterable.class, RETURNS_SELF);
                doReturn(nothing).when(iterable).cursor();
                return iterable;
            }
            ChangeStreamIterable<Document> real = spy((ChangeStreamIterable<Document>) invocation.callRealMethod());
            doAnswer(cursorInvocation -> {
                MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = (MongoChangeStreamCursor<ChangeStreamDocument<Document>>) cursorInvocation.callRealMethod();
                MongoChangeStreamCursor<ChangeStreamDocument<Document>> losing = mock(MongoChangeStreamCursor.class, org.mockito.AdditionalAnswers.delegatesTo(cursor));
                doAnswer(read -> {
                    if (historyLost.compareAndSet(true, false)) {
                        throw new MongoCommandException(new BsonDocument(List.of(new BsonElement("code", new BsonInt32(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE)),
                                new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")))), new ServerAddress());
                    }
                    return cursor.tryNext();
                }).when(losing).tryNext();
                return losing;
            }).when(real).cursor();
            return real;
        }).when(wrapped).watch(anyList(), eq(Document.class));
        return wrapped;
    }
}
