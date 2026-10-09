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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.MongoCommandException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Timeout(240)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelCanResumeFromTest {

    private static final String DATABASE = "canresumefrom";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private MongoClient mongoClient;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void create_instances() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        subscriptionModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(mongoClient, DATABASE), "events", TimeRepresentation.RFC_3339_STRING);
    }

    @AfterEach
    void shutdown() {
        mongoClient.close();
    }

    @Test
    void emits_true_while_the_oplog_has_the_history_and_false_once_it_is_gone() {
        MongoOperationTimeCheckpoint checkpoint = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint().block(TIMEOUT));
        Checkpoint readBack = new StringBasedCheckpoint(checkpoint.asString());
        assertThat(subscriptionModel.canResumeFrom(checkpoint).block(TIMEOUT)).isTrue();
        assertThat(subscriptionModel.canResumeFrom(readBack).block(TIMEOUT)).isTrue();

        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, checkpoint.operationTime);

        assertThat(subscriptionModel.canResumeFrom(checkpoint).block(TIMEOUT)).isFalse();
        assertThat(subscriptionModel.canResumeFrom(readBack).block(TIMEOUT)).isFalse();
    }

    @Test
    void emits_true_for_a_checkpoint_that_is_neither_a_resume_token_nor_an_operation_time() {
        assertThat(subscriptionModel.canResumeFrom(new StringBasedCheckpoint("position:3")).block(TIMEOUT)).isTrue();
    }

    @Test
    void errors_when_the_server_refuses_the_checkpoint_for_another_reason() {
        Checkpoint malformed = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("00")));

        assertThatThrownBy(() -> subscriptionModel.canResumeFrom(malformed).block(TIMEOUT)).hasRootCauseInstanceOf(MongoCommandException.class);
    }
}
