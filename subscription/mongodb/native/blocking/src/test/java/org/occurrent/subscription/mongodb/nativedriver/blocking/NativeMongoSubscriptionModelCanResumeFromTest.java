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

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Timeout(240)
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelCanResumeFromTest {

    private static final String DATABASE = "canresumefrom";

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> events;
    private ExecutorService executor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void create_instances() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
        events = database.getCollection("events");
        executor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, "events", TimeRepresentation.RFC_3339_STRING, executor);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        executor.shutdownNow();
        mongoClient.close();
    }

    @Test
    void answers_true_while_the_oplog_has_the_history_and_false_once_it_is_gone() {
        MongoOperationTimeCheckpoint checkpoint = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        Checkpoint readBack = new StringBasedCheckpoint(checkpoint.asString());
        assertThat(subscriptionModel.canResumeFrom(checkpoint)).isTrue();
        assertThat(subscriptionModel.canResumeFrom(readBack)).isTrue();

        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, checkpoint.operationTime);

        // canResumeFrom relies on the server refusing the change stream with 286 when it is opened
        assertThatThrownBy(() -> events.watch().startAtOperationTime(checkpoint.operationTime).cursor().close())
                .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE));
        assertThat(subscriptionModel.canResumeFrom(checkpoint)).isFalse();
        assertThat(subscriptionModel.canResumeFrom(readBack)).isFalse();
    }

    @Test
    void answers_true_for_a_checkpoint_that_is_neither_a_resume_token_nor_an_operation_time() {
        assertThat(subscriptionModel.canResumeFrom(new StringBasedCheckpoint("position:3"))).isTrue();
    }

    @Test
    void throws_when_the_server_refuses_the_checkpoint_for_another_reason() {
        Checkpoint malformed = new MongoResumeTokenCheckpoint(new org.bson.BsonDocument("_data", new org.bson.BsonString("00")));

        assertThatThrownBy(() -> subscriptionModel.canResumeFrom(malformed)).isInstanceOf(MongoCommandException.class);
    }
}
