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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.ChangeStreamHistory;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@Testcontainers
@Timeout(240)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelCanResumeFromTest {

    private static final String DATABASE = "canresumefrom";

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ChangeStreamHistory.container();

    private MongoClient mongoClient;
    private String databaseName;
    private MongoDatabase database;
    private MongoCollection<Document> events;
    private SpringMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        database = mongoClient.getDatabase(databaseName);
        events = database.getCollection("events");
        subscriptionModel = new SpringMongoSubscriptionModel(new MongoTemplate(mongoClient, databaseName), "events", TimeRepresentation.RFC_3339_STRING);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
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
        AtomicInteger probes = new AtomicInteger();
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, attempt -> { });
        Checkpoint malformed = new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("00")));

        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                assertThatThrownBy(() -> subscriptionModel.canResumeFrom(malformed)).hasRootCauseInstanceOf(MongoCommandException.class));
        assertThat(probes).hasValue(1);
    }

    @Test
    void throws_a_permanent_failure_of_the_probe_at_once_under_the_default_retry_strategy() {
        AtomicInteger probes = new AtomicInteger();
        MongoCommandException failure = commandFailure(13);
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, attempt -> {
            throw failure;
        });
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                assertThatThrownBy(() -> subscriptionModel.canResumeFrom(checkpoint)).isSameAs(failure));
        assertThat(probes).hasValue(1);
    }

    @Test
    void throws_a_permanent_failure_translated_by_spring_at_once_under_the_default_retry_strategy() {
        AtomicInteger probes = new AtomicInteger();
        UncategorizedMongoDbException failure = new UncategorizedMongoDbException("Command failed", commandFailure(13));
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, attempt -> {
            throw failure;
        });
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                assertThatThrownBy(() -> subscriptionModel.canResumeFrom(checkpoint)).isSameAs(failure));
        assertThat(probes).hasValue(1);
    }

    @Test
    void retries_a_lost_connection_under_the_default_retry_strategy_and_answers_true() {
        AtomicInteger probes = new AtomicInteger();
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, failOnTheFirstAttempt(transientFailure()));
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(10), () -> answer.set(subscriptionModel.canResumeFrom(checkpoint))));

        assertThat(thrown).isNull();
        assertThat(answer).hasValue(true);
        assertThat(probes).hasValue(2);
    }

    @Test
    void retries_a_command_failure_with_a_retryable_error_code_under_the_default_retry_strategy_and_answers_true() {
        AtomicInteger probes = new AtomicInteger();
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, failOnTheFirstAttempt(commandFailure(11600)));
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(10), () -> answer.set(subscriptionModel.canResumeFrom(checkpoint))));

        assertThat(thrown).isNull();
        assertThat(answer).hasValue(true);
        assertThat(probes).hasValue(2);
    }

    @Test
    void retries_a_lost_connection_translated_by_spring_under_the_default_retry_strategy_and_answers_true() {
        AtomicInteger probes = new AtomicInteger();
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("Connection reset", transientFailure());
        useModelWithTheDefaultRetryStrategyWhoseProbe(probes, failOnTheFirstAttempt(failure));
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(10), () -> answer.set(subscriptionModel.canResumeFrom(checkpoint))));

        assertThat(thrown).isNull();
        assertThat(answer).hasValue(true);
        assertThat(probes).hasValue(2);
    }

    @Test
    void retries_a_transient_failure_of_the_probe_and_answers_true_for_a_checkpoint_the_oplog_still_reaches() {
        AtomicInteger probes = new AtomicInteger();
        useModelWhoseProbe(RetryStrategy.fixed(Duration.ofMillis(10)), probes, failOnTheFirstAttempt(transientFailure()));
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> answer.set(subscriptionModel.canResumeFrom(checkpoint)));

        assertThat(thrown).isNull();
        assertThat(answer).hasValue(true);
        assertThat(probes).hasValue(2);
    }

    @Test
    void throws_a_transient_failure_of_the_probe_when_the_retry_strategy_does_not_retry() {
        AtomicInteger probes = new AtomicInteger();
        MongoSocketReadException failure = transientFailure();
        useModelWhoseProbe(RetryStrategy.none(), probes, failOnTheFirstAttempt(failure));
        Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

        assertThatThrownBy(() -> subscriptionModel.canResumeFrom(checkpoint)).isSameAs(failure);
        assertThat(probes).hasValue(1);
    }

    @Test
    void answers_false_without_retrying_the_probe_when_the_oplog_no_longer_reaches_the_checkpoint() {
        AtomicInteger probes = new AtomicInteger();
        useModelWhoseProbe(RetryStrategy.fixed(Duration.ofMillis(10)).maxAttempts(5), probes, attempt -> { });
        MongoOperationTimeCheckpoint checkpoint = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        ChangeStreamHistory.dropHistoryFrom(mongoDBContainer, DATABASE, checkpoint.operationTime);

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> answer.set(subscriptionModel.canResumeFrom(checkpoint)));

        assertThat(thrown).isNull();
        assertThat(answer).hasValue(false);
        assertThat(probes).hasValue(1);
    }

    @Test
    void logs_each_failed_probe_attempt_at_warn_with_the_collection_name() {
        Logger modelLog = (Logger) LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level originalLevel = modelLog.getLevel();
        modelLog.addAppender(appender);
        modelLog.setLevel(Level.WARN);
        AtomicInteger probes = new AtomicInteger();
        AtomicReference<Boolean> answer = new AtomicReference<>();
        try {
            useModelWhoseProbe(RetryStrategy.fixed(Duration.ofMillis(10)), probes, failOnTheFirstAttempts(2, transientFailure()));
            Checkpoint checkpoint = requireNonNull(subscriptionModel.globalCheckpoint());

            answer.set(subscriptionModel.canResumeFrom(checkpoint));
        } finally {
            modelLog.setLevel(originalLevel);
            modelLog.detachAppender(appender);
            appender.stop();
        }

        List<ILoggingEvent> probeFailures = appender.list.stream()
                .filter(event -> event.getFormattedMessage().contains("collection events can resume"))
                .collect(Collectors.toList());
        assertThat(answer).hasValue(true);
        assertThat(probes).hasValue(3);
        assertThat(probeFailures).hasSize(2).allSatisfy(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("events", "MongoSocketReadException");
            assertThat(event.getThrowableProxy().getClassName()).isEqualTo(MongoSocketReadException.class.getName());
        });
    }

    private static MongoCommandException commandFailure(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString("command failed with code " + code));
        return new MongoCommandException(response, new ServerAddress());
    }

    private static MongoSocketReadException transientFailure() {
        return new MongoSocketReadException("Prematurely reached end of stream", new ServerAddress(), new IOException("connection reset"));
    }

    private static IntConsumer failOnTheFirstAttempt(RuntimeException failure) {
        return failOnTheFirstAttempts(1, failure);
    }

    private static IntConsumer failOnTheFirstAttempts(int attempts, RuntimeException failure) {
        return attempt -> {
            if (attempt <= attempts) {
                throw failure;
            }
        };
    }

    // Swaps in a model whose probes are numbered from 1 and reported to beforeProbe before they run
    private void useModelWhoseProbe(RetryStrategy retryStrategy, AtomicInteger probes, IntConsumer beforeProbe) {
        subscriptionModel.shutdown();
        subscriptionModel = new SpringMongoSubscriptionModel(observed(probes, beforeProbe), "events", TimeRepresentation.RFC_3339_STRING, retryStrategy);
    }

    // The same, with the retry strategy the constructor without one gives the model
    private void useModelWithTheDefaultRetryStrategyWhoseProbe(AtomicInteger probes, IntConsumer beforeProbe) {
        subscriptionModel.shutdown();
        subscriptionModel = new SpringMongoSubscriptionModel(observed(probes, beforeProbe), "events", TimeRepresentation.RFC_3339_STRING);
    }

    private MongoTemplate observed(AtomicInteger probes, IntConsumer beforeProbe) {
        MongoTemplate observed = spy(new MongoTemplate(mongoClient, databaseName));
        doAnswer(invocation -> {
            Document command = invocation.getArgument(0, Document.class);
            if (command.containsKey("aggregate")) {
                beforeProbe.accept(probes.incrementAndGet());
            }
            return invocation.callRealMethod();
        }).when(observed).executeCommand(any(Document.class));
        return observed;
    }
}
