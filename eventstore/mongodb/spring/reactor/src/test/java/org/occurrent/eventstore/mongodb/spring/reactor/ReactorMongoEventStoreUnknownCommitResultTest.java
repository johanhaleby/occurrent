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

package org.occurrent.eventstore.mongodb.spring.reactor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.all;

/**
 * A commit that MongoDB cannot report the result of is sent again. The commit is forced to fail with a
 * {@code failCommand} fail point on {@code commitTransaction} that runs the commit and then answers with a write
 * concern error, which is how MongoDB reports a commit that was applied but whose result the client cannot be sure of.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreUnknownCommitResultTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final int WRITE_CONCERN_FAILED = 64;
    private static final int MAX_TIME_MS_EXPIRED = 50;

    // enableTestCommands is what lets the server accept configureFailPoint. withCommand replaces the whole command
    // line, so the replica set name ReplicaSetReadyMongoDBContainer configures is repeated.
    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoDatabaseFactory databaseFactory;
    private static final String STREAM_ID = "stream";
    private ListAppender<ILoggingEvent> logAppender;
    private Logger configLogger;

    @BeforeEach
    void connect_and_capture_the_config_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        mongoClient = MongoClients.create(connectionString);
        // A database of its own, since a commit that fails can leave its transaction open, and that holds locks until it times out
        String database = "unknowncommitresult" + UUID.randomUUID().toString().replace("-", "");
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        // The template and the transaction manager must share a factory, or the template runs its writes outside the manager's transaction
        databaseFactory = mongoTemplate.getMongoDatabaseFactory();
        logAppender = new ListAppender<>();
        logAppender.start();
        configLogger = (Logger) LoggerFactory.getLogger(EventStoreConfig.class);
        configLogger.addAppender(logAppender);
    }

    @AfterEach
    void switch_off_fail_point_and_close_client() {
        configLogger.detachAppender(logAppender);
        configureFailPoint(new Document("mode", "off"));
        mongoClient.close();
    }

    @Test
    void a_stream_write_whose_commit_result_is_unknown_is_committed_again_and_its_event_is_written_once() {
        ReactorMongoEventStore eventStore = storeOn(new CommitRetryingReactiveMongoTransactionManager(databaseFactory));
        failNextCommitWithAnUnknownResult();

        WriteResult result = eventStore.write(STREAM_ID, Flux.just(event("NameDefined"))).block();

        assertThat(requireNonNull(result).newStreamVersion()).isEqualTo(1L);
        assertThat(eventStore.count(Filter.streamId(STREAM_ID)).block()).isEqualTo(1L);
    }

    @Test
    void a_dcb_append_whose_commit_result_is_unknown_is_committed_again_and_its_event_is_written_once() {
        ReactorMongoEventStore eventStore = storeOn(new CommitRetryingReactiveMongoTransactionManager(databaseFactory));
        failNextCommitWithAnUnknownResult();

        eventStore.append(List.of(DcbCloudEvents.withTags(event("NameDefined"), List.of(Tag.parse("name:1"))))).block();

        assertThat(eventStore.count(all()).block()).isEqualTo(1L);
    }

    @Test
    void a_write_inside_a_callers_transaction_is_committed_again_by_the_manager_of_that_transaction() {
        CommitRetryingReactiveMongoTransactionManager transactionManager = new CommitRetryingReactiveMongoTransactionManager(databaseFactory);
        ReactorMongoEventStore eventStore = storeOn(transactionManager);
        failNextCommitWithAnUnknownResult();

        // The store joins this transaction, so it is the caller's commit that has an unknown result.
        TransactionalOperator.create(transactionManager).transactional(eventStore.write(STREAM_ID, Flux.just(event("NameDefined")))).block();

        assertThat(eventStore.count(Filter.streamId(STREAM_ID)).block()).isEqualTo(1L);
    }

    @Test
    void a_write_inside_a_callers_transaction_is_not_committed_again_when_that_transaction_has_a_plain_manager() {
        ReactorMongoEventStore eventStore = storeOn(new CommitRetryingReactiveMongoTransactionManager(databaseFactory));
        failNextCommitWithAnUnknownResult();

        // The caller's manager commits, and it does not retry. The store's own manager is not involved.
        TransactionalOperator callersTransaction = TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory));
        assertThatThrownBy(() -> callersTransaction.transactional(eventStore.write(STREAM_ID, Flux.just(event("NameDefined")))).block())
                .isInstanceOf(TransactionSystemException.class);
    }

    @Test
    void a_commit_that_keeps_having_an_unknown_result_fails_with_that_error_when_the_time_is_up() {
        ReactorMongoEventStore eventStore = storeOn(new CommitRetryingReactiveMongoTransactionManager(databaseFactory, null, Duration.ofMillis(300)));
        configureFailPoint(new Document("mode", "alwaysOn")
                .append("data", new Document("failCommands", List.of("commitTransaction"))
                        .append("writeConcernError", new Document("code", WRITE_CONCERN_FAILED).append("errmsg", "the result of the commit is unknown"))));

        assertThatThrownBy(() -> eventStore.write(STREAM_ID, Flux.just(event("NameDefined"))).block())
                .isInstanceOf(TransactionSystemException.class)
                .cause()
                .isInstanceOf(MongoException.class)
                .satisfies(cause -> assertThat(((MongoException) cause).hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)).isTrue());
    }

    @Test
    void a_commit_that_hits_max_time_ms_is_not_sent_again() {
        ReactorMongoEventStore eventStore = storeOn(new CommitRetryingReactiveMongoTransactionManager(databaseFactory));
        // One failure only. Were the commit sent again, it would succeed and the write would not throw.
        configureFailPoint(new Document("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("commitTransaction")).append("errorCode", MAX_TIME_MS_EXPIRED)));

        assertThatThrownBy(() -> eventStore.write(STREAM_ID, Flux.just(event("NameDefined"))).block())
                .isInstanceOf(TransactionSystemException.class);
        assertThat(eventStore.count(Filter.streamId(STREAM_ID)).block()).isZero();
    }

    @Test
    void the_transaction_options_the_manager_is_built_with_apply_to_its_transactions() {
        CommitRetryingReactiveMongoTransactionManager transactionManager = new CommitRetryingReactiveMongoTransactionManager(
                databaseFactory, TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).build());

        // The session sits in a package-private holder in Spring Data MongoDB, so it is read through reflection.
        ReadConcern readConcern = TransactionalOperator.create(transactionManager)
                .execute(status -> TransactionSynchronizationManager.forCurrentTransaction()
                        .map(synchronizationManager -> sessionOf(synchronizationManager.getResource(databaseFactory)))
                        .map(session -> session.getTransactionOptions().getReadConcern()))
                .single()
                .block();

        assertThat(readConcern).isEqualTo(ReadConcern.SNAPSHOT);
    }

    private static ClientSession sessionOf(Object resourceHolder) {
        try {
            Method getSession = resourceHolder.getClass().getDeclaredMethod("getSession");
            getSession.setAccessible(true);
            return (ClientSession) getSession.invoke(resourceHolder);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void a_plain_transaction_manager_makes_the_config_warn_once() {
        configFor(new ReactiveMongoTransactionManager(databaseFactory));

        assertThat(warnings()).singleElement()
                .satisfies(message -> assertThat(message).contains(ReactiveMongoTransactionManager.class.getName()).contains("CommitRetryingReactiveMongoTransactionManager"));
    }

    @Test
    void the_commit_retrying_transaction_manager_makes_no_warning() {
        configFor(new CommitRetryingReactiveMongoTransactionManager(databaseFactory));

        assertThat(warnings()).isEmpty();
    }

    private List<String> warnings() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel().equals(Level.WARN))
                .filter(event -> event.getFormattedMessage().contains("send a commit again"))
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private static EventStoreConfig configFor(ReactiveTransactionManager transactionManager) {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
    }

    private ReactorMongoEventStore storeOn(ReactiveTransactionManager transactionManager) {
        return new ReactorMongoEventStore(mongoTemplate, configFor(transactionManager));
    }

    private void failNextCommitWithAnUnknownResult() {
        configureFailPoint(new Document("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("commitTransaction"))
                        .append("writeConcernError", new Document("code", WRITE_CONCERN_FAILED).append("errmsg", "the result of the commit is unknown"))));
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        Mono.from(mongoClient.getDatabase("admin").runCommand(command)).block();
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
