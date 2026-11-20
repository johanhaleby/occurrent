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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A default start with nothing stored records a first position, and a storage that does not evaluate write conditions
 * can only write it unconditionally, so a subscribe is refused unless {@code startWhenFirstPositionCanBeOverwritten}
 * is set. The blocking MongoDB model evaluates the start position on a thread of its own after {@code subscribe} has
 * returned and retries it for ever when it throws, so a refusal raised there never reaches the caller. When the model
 * has no position at subscribe time, and starting without one is allowed, the refusal must still reach the caller of
 * {@code subscribe}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class SpringMongoDefaultStartRefusalTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private PositionAvailableLater wrapped;
    private WritesOnlyUnconditionally storage;
    private DurableSubscriptionModel durable;

    @BeforeEach
    void create_the_models() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        wrapped = new PositionAvailableLater(mongoTemplate, requireNonNull(connectionString.getCollection()), TimeRepresentation.RFC_3339_STRING);
        storage = new WritesOnlyUnconditionally();
        durable = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
    }

    @AfterEach
    void shut_down() {
        durable.shutdown();
        mongoClient.close();
    }

    @Test
    void a_default_start_is_refused_on_subscribe_when_the_model_has_no_position_yet_and_starting_without_one_is_allowed() {
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        wrapped.hasAPosition = true;

        assertThat(thrown)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("startWhenFirstPositionCanBeOverwritten")
                .hasMessageContaining(SUBSCRIPTION_ID);
        assertThat(storage.checkpoints).as("checkpoints stored").isEmpty();
    }

    private static final class PositionAvailableLater extends SpringMongoSubscriptionModel {
        volatile boolean hasAPosition = false;

        PositionAvailableLater(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation) {
            super(mongoTemplate, eventCollection, timeRepresentation);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return hasAPosition ? super.globalCheckpoint() : null;
        }
    }

    // Writes unconditionally only, and answers the default false from evaluatesWriteConditions()
    private static final class WritesOnlyUnconditionally implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (!(condition instanceof CheckpointWriteCondition.Any)) {
                throw new UnsupportedOperationException("Only any() is supported, not " + condition);
            }
            return save(subscriptionId, checkpoint);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }
}
