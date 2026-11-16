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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Starts subscriptions through a mongos, which reports a start position whose history is gone with the first reply of
 * the change stream rather than as the change stream opens. The events are on the second shard of a
 * {@code TwoShardMongoCluster}, which has a one megabyte oplog, so a test can make that oplog forget a position.
 */
@Timeout(600)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelThroughMongosTest {

    private static final int OPLOG_SIZE_MEGABYTES = 1;
    private static final int MONGOS_PORT = 27017;
    private static final Duration MAX_AWAIT_TIME = Duration.ofSeconds(10);

    private static TwoShardMongoCluster cluster;

    private String databaseName;
    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private ExecutorService executor;
    private SpringMongoSubscriptionModel subscriptionModel;

    @BeforeAll
    static void start_cluster() {
        cluster = TwoShardMongoCluster.start(OPLOG_SIZE_MEGABYTES);
    }

    @AfterAll
    static void stop_cluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void create_instances() {
        // The events are on the shard with the one-megabyte oplog
        databaseName = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
        mongoClient = MongoClients.create(new ConnectionString(cluster.getConnectionString()));
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        mongoTemplate.getDb().createCollection("events");
        executor = Executors.newCachedThreadPool();
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate,
                SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).executor(executor).restartSubscriptionsOnChangeStreamHistoryLost(false).maxAwaitTime(MAX_AWAIT_TIME));
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        executor.shutdownNow();
        mongoClient.close();
    }

    @Test
    void a_timed_wait_for_a_start_at_a_position_whose_history_is_gone_throws_the_refusal_and_frees_the_id() {
        assertRefusedFromTheWait(subscription -> subscription.waitUntilStarted(Duration.ofSeconds(60)));
    }

    @Test
    void an_untimed_wait_for_a_start_at_a_position_whose_history_is_gone_throws_the_refusal_and_frees_the_id() {
        assertRefusedFromTheWait(Subscription::waitUntilStarted);
    }

    @Test
    void a_start_at_now_does_not_wait_for_the_first_reply_of_the_change_stream() {
        // Given a quiet collection, so the first reply comes only when maxAwaitTime has passed
        // When
        long startedAt = System.nanoTime();
        boolean started = subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(5));
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        // Then
        assertThat(started).isTrue();
        assertThat(waited).isLessThan(MAX_AWAIT_TIME.dividedBy(2));
    }

    @Test
    void a_resume_at_now_does_not_wait_for_the_first_reply_of_the_change_stream() {
        // Given a started subscription on a quiet collection, paused
        String subscriptionId = UUID.randomUUID().toString();
        assertThat(subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(5))).isTrue();
        subscriptionModel.pauseSubscription(subscriptionId);

        // When
        long startedAt = System.nanoTime();
        boolean started = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now()).waitUntilStarted(Duration.ofSeconds(5));
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        // Then
        assertThat(started).isTrue();
        assertThat(waited).isLessThan(MAX_AWAIT_TIME.dividedBy(2));
    }

    @Test
    void a_start_of_a_model_stopped_when_a_subscription_at_now_was_made_throws_the_refusal_when_that_present_has_left_the_oplog() {
        // Given a subscription at the present made while the model is stopped, and an oplog that then forgets that present
        subscriptionModel.stop();
        String subscriptionId = UUID.randomUUID().toString();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        });
        MongoOperationTimeCheckpoint presentAfterSubscribe = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        dropHistoryFrom(presentAfterSubscribe.operationTime);

        // When
        Throwable thrownByStart = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> subscriptionModel.start(true)));
        Throwable thrownByWait = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> subscription.waitUntilStarted(Duration.ofSeconds(60))));

        // Then
        assertIsTheRefusalOfAHistoryLostFor(subscriptionId, thrownByStart);
        assertIsTheRefusalOfAHistoryLostFor(subscriptionId, thrownByWait);
    }

    @Test
    void a_resume_of_a_subscription_at_now_paused_before_its_first_reply_throws_the_refusal_when_that_present_has_left_the_oplog() {
        // Given a subscription at the present that started and is paused before its change stream's first reply, and an oplog that then forgets that present
        String subscriptionId = UUID.randomUUID().toString();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        });
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
        subscriptionModel.pauseSubscription(subscriptionId);
        MongoOperationTimeCheckpoint presentAfterPause = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        dropHistoryFrom(presentAfterPause.operationTime);

        // When
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        Throwable thrownByResumed = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> resumed.waitUntilStarted(Duration.ofSeconds(60))));
        Throwable thrownBySubscribed = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> subscription.waitUntilStarted(Duration.ofSeconds(60))));

        // Then
        assertIsTheRefusalOfAHistoryLostFor(subscriptionId, thrownByResumed);
        assertIsTheRefusalOfAHistoryLostFor(subscriptionId, thrownBySubscribed);
    }

    private static void assertIsTheRefusalOfAHistoryLostFor(String subscriptionId, Throwable thrown) {
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageStartingWith("There was not enough oplog to resume subscription \"" + subscriptionId + "\"")
                .rootCause().isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.getCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE));
    }

    private void assertRefusedFromTheWait(Consumer<Subscription> wait) {
        // Given a checkpoint the oplog of the shard no longer reaches back to
        MongoOperationTimeCheckpoint lost = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        dropHistoryFrom(lost.operationTime);
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, StartAt.checkpoint(lost), __ -> {
        });
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> wait.accept(subscription)));

        // Then
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageStartingWith("There was not enough oplog to resume subscription \"" + subscriptionId + "\"")
                .isInstanceOfSatisfying(UnsupportedStartAtException.class, refusal -> assertThat(refusal.startAt())
                        .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, refused -> assertThat(refused.checkpoint).isEqualTo(lost)))
                .rootCause().isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.getCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE));
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10))).as("the id is free to subscribe with again").isTrue();
    }

    // Writes ten megabytes into the shard through the mongos, then a small document every hundred milliseconds, until
    // the oldest entry of the shard's oplog is newer than the operation time, as ChangeStreamHistory does for a replica set
    private void dropHistoryFrom(BsonTimestamp operationTime) {
        cluster.mongoshOnSecondShard("const filler = connect('mongodb://localhost:" + MONGOS_PORT + "/" + databaseName + "').getCollection('filler');"
                + "const oplog = db.getSiblingDB('local').getCollection('oplog.rs');"
                + "const lostT = " + operationTime.getTime() + ", lostI = " + operationTime.getInc() + ";"
                + "const reaches = () => { const ts = oplog.find().sort({$natural: 1}).limit(1).next().ts; return ts.t < lostT || (ts.t === lostT && ts.i <= lostI); };"
                + "const deadline = Date.now() + 180000; let batches = 0;"
                + "while (reaches()) {"
                + "  if (Date.now() > deadline) { throw new Error('The oplog still reaches back to the operation time after 3 minutes'); }"
                + "  if (batches < 5) { filler.insertMany(Array.from({length: 20}, () => ({filler: 'x'.repeat(100000)}))); batches++; } else { filler.insertOne({filler: 'x'}); }"
                + "  sleep(100);"
                + "}"
                + "filler.drop();");
    }
}
