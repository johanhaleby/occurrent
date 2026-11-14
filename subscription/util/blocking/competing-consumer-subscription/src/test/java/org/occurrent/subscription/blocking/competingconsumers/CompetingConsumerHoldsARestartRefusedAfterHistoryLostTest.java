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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
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
import java.util.concurrent.Executors;
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
        wrappedA = new NativeMongoSubscriptionModel(databaseThatSendsNoOperationTime(), collectionThatLosesItsHistoryAndRefusesThePresent(), TimeRepresentation.RFC_3339_STRING,
                Executors.newCachedThreadPool(), NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true)
                        .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))).maxAwaitTime(Duration.ofSeconds(1)));
        nodeA = new CompetingConsumerSubscriptionModel(wrappedA, strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(new NativeMongoSubscriptionModel(database, eventCollectionName, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool()), strategyB);
    }

    @AfterEach
    void shutdown() {
        nodeA.shutdown();
        nodeB.shutdown();
        mongoClient.close();
    }

    @Timeout(value = 90, unit = SECONDS)
    @Test
    void a_node_currently_keeps_the_lease_while_its_wrapped_model_holds_the_subscription_paused_after_a_refused_restart_and_does_not_ask_mongodb_again() throws InterruptedException {
        // Given
        Subscription started = nodeA.subscribe("A", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A holds the lease before the history is lost").isTrue();

        // When
        presentRefused.set(true);
        historyLost.set(true);
        await().atMost(15, SECONDS).until(() -> wrappedA.isPaused(SUBSCRIPTION_ID));
        int readsWhenHeld = reads.get();
        Thread.sleep(LEASE_TIME.multipliedBy(4).toMillis());
        int readsAfterFourLeaseTimes = reads.get();
        nodeB.subscribe("B", SUBSCRIPTION_ID, filter(), StartAt.now(), (CloudEvent __) -> {
        });
        Thread.sleep(LEASE_TIME.multipliedBy(2).toMillis());

        // Then
        assertThat(wrappedA.isPaused(SUBSCRIPTION_ID)).as("wrapped model on A is held paused").isTrue();
        assertThat(nodeA.isPaused(SUBSCRIPTION_ID)).as("node A is paused").isTrue();
        assertThat(nodeA.isRunning(SUBSCRIPTION_ID)).as("node A is running").isFalse();
        assertThat(strategyA.hasLock(SUBSCRIPTION_ID, "A")).as("A keeps the lease while its subscription is held paused").isTrue();
        assertThat(strategyB.hasLock(SUBSCRIPTION_ID, "B")).as("B does not get the lease").isFalse();
        assertThat(nodeB.isRunning(SUBSCRIPTION_ID)).as("node B is running without the lease").isFalse();
        assertThat(readsAfterFourLeaseTimes).as("reads of MongoDB for the present while held, after four lease times").isEqualTo(readsWhenHeld);
        assertThat(reads.get()).as("reads of MongoDB for the present while held, at the end").isEqualTo(readsWhenHeld);
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
