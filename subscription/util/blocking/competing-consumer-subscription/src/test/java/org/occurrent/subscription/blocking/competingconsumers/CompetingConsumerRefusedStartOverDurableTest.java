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
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A competing consumer model over a durable model over the native MongoDB model. The durable model asks a dynamic start
 * itself, with its own context. A start it answers null for is handed on as it was given, so the native model asks it
 * again with the native context and can refuse the answer after subscribe(..) returned. A start it answers the model
 * default for is resolved by the durable model, so the native model never asks the function.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerRefusedStartOverDurableTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration LEASE_TIME = Duration.ofSeconds(3);
    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    private static final String SUBSCRIPTION_ID = "s";

    private MongoClient mongoClient;
    private NativeMongoLeaseCompetingConsumerStrategy strategy;
    private NativeMongoCheckpointStorage checkpointStorage;
    private CompetingConsumerSubscriptionModel competing;
    private final List<Class<?>> modelsAskedTheStart = new CopyOnWriteArrayList<>();

    @BeforeEach
    void a_competing_consumer_model_over_a_durable_model_over_the_native_model() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        String eventCollection = requireNonNull(connectionString.getCollection());
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
        checkpointStorage = new NativeMongoCheckpointStorage(database, "checkpoints-" + UUID.randomUUID());
        DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, checkpointStorage);
        strategy = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, "locks-" + UUID.randomUUID()).leaseTime(LEASE_TIME).build();
        competing = new CompetingConsumerSubscriptionModel(durable, strategy);
    }

    @AfterEach
    void shutdown() {
        try {
            competing.shutdown();
        } finally {
            mongoClient.close();
        }
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_start_that_the_durable_model_hands_on_unanswered_and_that_the_native_model_refuses_is_reported_by_the_handle_and_frees_the_lease_and_the_subscription_id() {
        // Given a start that the durable model answers null for, and the native model answers a catch-up position for.
        // The competing consumer model answers a start, since null would make the subscription not compete
        StartAt refusedByTheNativeModel = StartAt.dynamic(context -> {
            modelsAskedTheStart.add(context.subscriptionModelType());
            if (context.subscriptionModelType() == CompetingConsumerSubscriptionModel.class) {
                return StartAt.now();
            }
            return context.subscriptionModelType() == NativeMongoSubscriptionModel.class ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : null;
        });

        // When
        Subscription subscription = competing.subscribe("A", SUBSCRIPTION_ID, null, refusedByTheNativeModel, __ -> {
        });

        // Then
        assertThatThrownBy(() -> subscription.waitUntilStarted(EVENTUALLY))
                .as("[waitUntilStarted answers although the native model refused the start through the durable model]")
                .isInstanceOf(UnsupportedStartAtException.class);
        assertThat(modelsAskedTheStart).as("the function is asked by the durable model and then by the native model")
                .containsSubsequence(DurableSubscriptionModel.class, NativeMongoSubscriptionModel.class);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategy.hasLock(SUBSCRIPTION_ID, "A"))
                .as("[A still holds the lease after the native model refused the start]").isFalse());
        await().atMost(EVENTUALLY).untilAsserted(() -> {
            assertThat(competing.isRunning(SUBSCRIPTION_ID)).as("[s still runs on A after the refusal]").isFalse();
            assertThat(competing.isPaused(SUBSCRIPTION_ID)).as("[s is still paused on A after the refusal]").isFalse();
        });
        assertThat(checkpointStorage.read(SUBSCRIPTION_ID)).as("the refused start recorded no checkpoint").isNull();
        Subscription again = competing.subscribe("A", SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        assertThat(again.waitUntilStarted(EVENTUALLY)).as("the subscription id is free to subscribe again on A").isTrue();
    }

    @Timeout(value = 60, unit = SECONDS)
    @Test
    void a_start_that_the_durable_model_answers_with_the_model_default_is_never_handed_to_the_native_model_as_the_position_it_would_refuse_so_nothing_is_refused() {
        // Given a start that the durable model answers with the model default, and that would be answered a refused position by the native model
        StartAt wouldBeRefusedByTheNativeModel = StartAt.dynamic(context -> {
            modelsAskedTheStart.add(context.subscriptionModelType());
            return context.subscriptionModelType() == NativeMongoSubscriptionModel.class ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : StartAt.subscriptionModelDefault();
        });

        // When
        Subscription subscription = competing.subscribe("A", SUBSCRIPTION_ID, null, wouldBeRefusedByTheNativeModel, __ -> {
        });

        // Then it starts and keeps the lease
        assertThat(subscription.waitUntilStarted(EVENTUALLY)).as("the handle answers that the subscription started").isTrue();
        await().during(LEASE_TIME.multipliedBy(2)).atMost(LEASE_TIME.multipliedBy(2).plus(EVENTUALLY)).untilAsserted(() -> {
            assertThat(strategy.hasLock(SUBSCRIPTION_ID, "A")).as("[A keeps the lease]").isTrue();
            assertThat(competing.isRunning(SUBSCRIPTION_ID)).as("[s runs on A]").isTrue();
            assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("[the handle throws no refusal]").doesNotThrowAnyException();
        });
        assertThat(modelsAskedTheStart).as("the function is asked by the durable model and never by the native model")
                .contains(DurableSubscriptionModel.class).doesNotContain(NativeMongoSubscriptionModel.class);
    }
}
