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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import com.mongodb.client.MongoDatabase;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A checkpoint an older Occurrent version stored in the legacy {@code subscriptionPosition} field gets this node's
 * write version when {@link DurableSubscriptionModel} subscribes with the model default, so a node with an older
 * version cannot store a later checkpoint over it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelLegacyCheckpointWriteVersionMongoTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String COLLECTION = "legacy-checkpoints";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static ConnectionString connectionString;

    @BeforeAll
    static void connect() {
        connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("durablelegacycheckpoint"));
        mongoClient = MongoClients.create(connectionString);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Test
    void a_checkpoint_stored_in_the_legacy_field_gets_this_nodes_version_when_subscribing_with_the_model_default_and_refuses_a_write_with_an_older_one() {
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        String collection = COLLECTION + "-" + UUID.randomUUID();
        database.getCollection(collection).insertOne(new Document("_id", SUBSCRIPTION_ID).append("subscriptionPosition", "p"));
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(database.getCollection(collection));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new PositionOnlyFeed(), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }).waitUntilStarted();

        assertAll(
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("p"),
                () -> assertThatThrownBy(() -> storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("later"), CheckpointWriteCondition.notOlderThan(1)))
                        .as("a node with an older version writing over it")
                        .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position after the refused write").isEqualTo("p")
        );
    }

    /**
     * Answers no global checkpoint and starts no feed, so a node using it stores only what it stores on subscribe.
     */
    private static final class PositionOnlyFeed implements CheckpointAwareSubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
