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

package org.occurrent.springboot.mongo.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.service.spring.reactor.SpringReactiveTransactionExecutor;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive counterpart of {@code SpringTransactionExecutorMongoErrorLabelTest}. What
 * {@link SpringReactiveTransactionExecutor} retries is decided by the error MongoDB returns, so each case is a real
 * server answer. A conflict arrives labelled {@code TransientTransactionError} and is retried. A duplicate key, a
 * validation failure and a duplicate from a write outside the transaction carry no label and run the unit of work once.
 * The unit of work fails the test on a surplus run instead of waiting out the two minutes of retries.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringReactiveTransactionExecutorMongoErrorLabelTest {

    private static final int WRITE_CONFLICT = 112;
    private static final String COLLECTION = "label_test";

    // enableTestCommands is what lets the server accept configureFailPoint. withCommand replaces the whole command
    // line, so the replica set name ReplicaSetReadyMongoDBContainer configures is repeated.
    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private MongoClient mongoClient;
    private String database;
    // Joins the transaction the executor opens, as the reactive event store does.
    private ReactiveMongoTemplate inTransaction;
    // Never joins it, so a write through this template is outside any transaction. Spring translates its errors like the
    // other template's.
    private ReactiveMongoTemplate outsideTemplate;
    // The rival's writes go straight to the driver.
    private MongoCollection<Document> outsideTransaction;
    private SpringReactiveTransactionExecutor executor;

    @BeforeEach
    void connect_and_start_from_an_empty_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactiveexecutorlabels");
        mongoClient = MongoClients.create(connectionString);
        database = requireNonNull(connectionString.getDatabase());
        SimpleReactiveMongoDatabaseFactory databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, database);
        inTransaction = new ReactiveMongoTemplate(databaseFactory);
        outsideTemplate = new ReactiveMongoTemplate(databaseFactory);
        outsideTemplate.setSessionSynchronization(SessionSynchronization.NEVER);
        executor = new SpringReactiveTransactionExecutor(new ReactiveMongoTransactionManager(databaseFactory));

        Mono.from(mongoClient.getDatabase(database).getCollection(COLLECTION).drop()).block();
        Mono.from(mongoClient.getDatabase(database).createCollection(COLLECTION)).block();
        outsideTransaction = mongoClient.getDatabase(database).getCollection(COLLECTION);
    }

    @AfterEach
    void switch_off_fail_point_and_close_client() {
        configureFailPoint(new Document("mode", "off"));
        mongoClient.close();
    }

    @Test
    void a_write_conflict_labelled_by_the_server_is_retried_until_it_commits() {
        configureFailPoint(new Document("mode", new Document("times", 3))
                .append("data", new Document("failCommands", List.of("insert"))
                        .append("errorCode", WRITE_CONFLICT)
                        .append("errorLabels", List.of("TransientTransactionError"))));
        AtomicInteger runs = new AtomicInteger();

        Mono<Document> insert = executor.inTransaction(() -> Mono.defer(() -> {
            runs.incrementAndGet();
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        }));

        StepVerifier.create(insert).expectNextCount(1).verifyComplete();
        assertThat(runs).hasValue(4);
        assertThat(Mono.from(outsideTransaction.countDocuments()).block()).isEqualTo(1L);
    }

    @Test
    void a_duplicate_key_in_the_transaction_runs_the_unit_of_work_once() {
        Mono.from(outsideTransaction.insertOne(new Document("_id", "a"))).block();
        AtomicInteger runs = new AtomicInteger();

        Mono<Document> insert = executor.inTransaction(() -> Mono.defer(() -> {
            failOnASecondRun(runs);
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        }));

        StepVerifier.create(insert).expectError(DuplicateKeyException.class).verify();
        assertThat(runs).hasValue(1);
    }

    @Test
    void a_validation_failure_in_the_transaction_runs_the_unit_of_work_once() {
        Mono.from(mongoClient.getDatabase(database)
                .runCommand(new Document("collMod", COLLECTION)
                        .append("validator", new Document("$jsonSchema", new Document("required", List.of("mandatory")))))).block();
        AtomicInteger runs = new AtomicInteger();

        Mono<Document> insert = executor.inTransaction(() -> Mono.defer(() -> {
            failOnASecondRun(runs);
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        }));

        StepVerifier.create(insert).expectError(DataIntegrityViolationException.class).verify();
        assertThat(runs).hasValue(1);
    }

    /**
     * 0.34.0 retried this: the first run checks, a rival inserts the document before the write, the duplicate comes
     * back unlabelled because the write is outside the transaction, and the second run sees the rival's document and
     * skips the write. It is the one case that retried before and does not now.
     */
    @Test
    void a_duplicate_from_a_write_outside_any_transaction_runs_the_unit_of_work_once() {
        AtomicInteger runs = new AtomicInteger();

        Mono<Void> unit = executor.inTransaction(() -> Mono.defer(() -> {
            runs.incrementAndGet();
            return inTransaction.exists(Query.query(Criteria.where("_id").is("a")), COLLECTION)
                    .flatMap(exists -> exists
                            ? Mono.<Void>empty()
                            // The rival wins the race between the check and the write.
                            : insertOutsideTransaction().then(outsideTemplate.insert(new Document("_id", "a"), COLLECTION).then()));
        }));

        StepVerifier.create(unit).expectError(DuplicateKeyException.class).verify();
        assertThat(runs).hasValue(1);
    }

    /**
     * The same race through a template that joins the transaction. The rival commits after the transaction's snapshot,
     * so the server answers the duplicate with a write conflict labelled TransientTransactionError and not with a
     * duplicate key. That is what to do instead of writing outside the transaction.
     */
    @Test
    void a_duplicate_from_a_rival_that_commits_after_the_snapshot_is_a_labelled_conflict_and_is_retried() {
        AtomicInteger runs = new AtomicInteger();

        Mono<Void> unit = executor.inTransaction(() -> Mono.defer(() -> {
            int run = runs.incrementAndGet();
            return inTransaction.exists(Query.query(Criteria.where("_id").is("a")), COLLECTION)
                    .flatMap(exists -> exists
                            ? Mono.<Void>empty()
                            : (run == 1 ? insertOutsideTransaction() : Mono.<Void>empty())
                            .then(inTransaction.insert(new Document("_id", "a"), COLLECTION).then()));
        }));

        StepVerifier.create(unit).verifyComplete();
        assertThat(runs).hasValue(2);
    }

    private Mono<Void> insertOutsideTransaction() {
        return Mono.from(outsideTransaction.insertOne(new Document("_id", "a"))).then();
    }

    private static void failOnASecondRun(AtomicInteger runs) {
        if (runs.incrementAndGet() > 1) {
            throw new AssertionError("The unit of work ran again, so the error was retried");
        }
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        Mono.from(mongoClient.getDatabase("admin").runCommand(command)).block();
    }
}
