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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.test.StepVerifier;

import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A fail point makes the one {@code aggregate} that {@code canResumeFrom} sends fail the way a disconnect, an election
 * or a refused command does.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelCanResumeFromTransientFailureTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "can-resume-from-subscriber";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int NOT_WRITABLE_PRIMARY = 10107;
    private static final int UNAUTHORIZED = 13;
    private static final int HISTORY_LOST = 286;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private MongoClient adminClient;
    private MongoClient subscriberClient;
    private CommandLog commands;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private Checkpoint checkpoint;

    @BeforeEach
    void createModel() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".canresumefromtransient");
        String database = requireNonNull(connectionString.getDatabase());
        commands = new CommandLog();
        adminClient = MongoClients.create(connectionString);
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(SUBSCRIBER_APPLICATION_NAME).addCommandListener(commands).build());
        subscriptionModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, database), "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofMillis(10), Duration.ofMillis(50)));
        checkpoint = requireNonNull(subscriptionModel.globalCheckpoint().block(TIMEOUT));
    }

    @AfterEach
    void close() {
        FailPoint.off(adminClient);
        subscriberClient.close();
        adminClient.close();
    }

    @Test
    void asks_again_after_a_primary_that_stepped_down_failed_the_probe_and_emits_true() {
        FailPoint.failNext(adminClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", NOT_WRITABLE_PRIMARY));

        StepVerifier.create(subscriptionModel.canResumeFrom(checkpoint)).expectNext(true).expectComplete().verify(TIMEOUT);

        assertAll(
                () -> assertThat(commands.failed("aggregate")).as("probes the server failed").singleElement()
                        .satisfies(probe -> assertThat(probe.failure()).isInstanceOf(MongoNotPrimaryException.class)),
                () -> assertThat(commands.changeStreamsOpened()).as("probes sent").hasSize(2));
    }

    @Test
    void emits_false_without_asking_again_when_the_server_says_the_history_is_lost() {
        FailPoint.failNext(adminClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", HISTORY_LOST));

        Boolean canResume = subscriptionModel.canResumeFrom(checkpoint).block(TIMEOUT);

        assertAll(
                () -> assertThat(canResume).as("answer").isFalse(),
                () -> assertThat(commands.changeStreamsOpened()).as("probes sent").hasSize(1));
    }

    @Test
    void fails_without_asking_again_when_the_server_refuses_the_probe_for_a_reason_that_does_not_go_away() {
        FailPoint.failNext(adminClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", UNAUTHORIZED));

        assertThatThrownBy(() -> subscriptionModel.canResumeFrom(checkpoint).block(TIMEOUT))
                .hasRootCauseInstanceOf(MongoCommandException.class)
                .rootCause().satisfies(cause -> assertThat(((MongoCommandException) cause).getErrorCode()).isEqualTo(UNAUTHORIZED));

        assertThat(commands.changeStreamsOpened()).as("probes sent").hasSize(1);
    }
}
