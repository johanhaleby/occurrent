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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Starts subscriptions against a real MongoDB whose oplog no longer reaches back to the position they ask for, and
 * with {@code restartSubscriptionsOnChangeStreamHistoryLost} off, which is its default.
 */
@Testcontainers
@Timeout(240)
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelHistoryLostAtStartTest {

    private static final String DATABASE = "historylostatstart";

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private MongoClient mongoClient;
    private MongoDatabase database;
    private ExecutorService executor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        executor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, "events", TimeRepresentation.RFC_3339_STRING, executor,
                NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false));
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        executor.shutdownNow();
        mongoClient.close();
    }

    @Test
    void a_subscription_whose_start_position_has_lost_its_history_is_refused_with_the_lost_checkpoint_and_its_id_is_freed() {
        // Given a checkpoint the oplog no longer reaches back to
        MongoOperationTimeCheckpoint lost = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, lost.operationTime);

        // When the subscription starts at the checkpoint, or at a position that resolves to it
        // Then
        assertRefusedWithTheLostCheckpoint(StartAt.checkpoint(lost), lost);
        assertRefusedWithTheLostCheckpoint(StartAt.dynamic(() -> StartAt.checkpoint(lost)), lost);
    }

    private void assertRefusedWithTheLostCheckpoint(StartAt startAt, MongoOperationTimeCheckpoint lost) {
        String subscriptionId = UUID.randomUUID().toString();

        Subscription subscription = subscriptionModel.subscribe(subscriptionId, startAt, __ -> {
        });
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            subscription.waitUntilStarted();
        }));

        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageStartingWith("There was not enough oplog to resume subscription \"" + subscriptionId + "\"")
                .hasMessageContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .isInstanceOfSatisfying(UnsupportedStartAtException.class, refusal -> assertThat(refusal.startAt())
                        .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, refused -> assertThat(refused.checkpoint).isEqualTo(lost)))
                .rootCause().isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE));
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10))).as("the id is free to subscribe with again").isTrue();
    }
}
