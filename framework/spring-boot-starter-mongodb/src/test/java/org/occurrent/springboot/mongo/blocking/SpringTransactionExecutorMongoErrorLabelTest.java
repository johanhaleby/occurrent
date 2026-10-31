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

package org.occurrent.springboot.mongo.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.service.spring.SpringTransactionExecutor;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@link SpringTransactionExecutor} retries is decided by the error MongoDB returns, so each case here is a real
 * server answer and not a hand-built exception. A conflict arrives labelled {@code TransientTransactionError} and is
 * retried. A duplicate key, a validation failure and a duplicate from a write outside the transaction carry no label
 * and run the unit of work once. The conflict is forced with a {@code failCommand} fail point rather than a race, so
 * the number of failed attempts is exact. The unit of work fails the test on a surplus run instead of waiting out the
 * two minutes of retries.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringTransactionExecutorMongoErrorLabelTest {

    private static final int WRITE_CONFLICT = 112;
    private static final String COLLECTION = "label_test";

    // enableTestCommands is what lets the server accept configureFailPoint. withCommand replaces the whole command
    // line, so the replica set name ReplicaSetReadyMongoDBContainer configures is repeated.
    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private MongoClient mongoClient;
    // Joins the transaction the executor opens, which is how the event store writes inside one.
    private MongoTemplate inTransaction;
    // Never joins it, so a write through this template is outside any transaction.
    private MongoTemplate outsideTransaction;
    private SpringTransactionExecutor executor;

    @BeforeEach
    void connect_and_start_from_an_empty_collection() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".executorlabels");
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, database);
        inTransaction = new MongoTemplate(databaseFactory);
        inTransaction.setSessionSynchronization(SessionSynchronization.ALWAYS);
        outsideTransaction = new MongoTemplate(databaseFactory);
        outsideTransaction.setSessionSynchronization(SessionSynchronization.NEVER);
        executor = new SpringTransactionExecutor(new MongoTransactionManager(databaseFactory));

        outsideTransaction.dropCollection(COLLECTION);
        outsideTransaction.createCollection(COLLECTION);
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

        executor.inTransaction(() -> {
            runs.incrementAndGet();
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        });

        assertThat(runs).hasValue(4);
        assertThat(outsideTransaction.count(new Query(), COLLECTION)).isEqualTo(1);
    }

    @Test
    void a_duplicate_key_in_the_transaction_runs_the_unit_of_work_once() {
        outsideTransaction.insert(new Document("_id", "a"), COLLECTION);
        AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> executor.inTransaction(() -> {
            failOnASecondRun(runs);
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        })).isInstanceOf(DuplicateKeyException.class);

        assertThat(runs).hasValue(1);
    }

    @Test
    void a_validation_failure_in_the_transaction_runs_the_unit_of_work_once() {
        outsideTransaction.getDb().runCommand(new Document("collMod", COLLECTION)
                .append("validator", new Document("$jsonSchema", new Document("required", List.of("mandatory")))));
        AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> executor.inTransaction(() -> {
            failOnASecondRun(runs);
            return inTransaction.insert(new Document("_id", "a"), COLLECTION);
        })).isInstanceOf(DataIntegrityViolationException.class);

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
        MongoDatabase rival = outsideTransaction.getDb();

        assertThatThrownBy(() -> executor.inTransaction(() -> {
            runs.incrementAndGet();
            if (!outsideTransaction.exists(Query.query(Criteria.where("_id").is("a")), COLLECTION)) {
                // The rival wins the race between the check and the write.
                rival.getCollection(COLLECTION).insertOne(new Document("_id", "a"));
                outsideTransaction.insert(new Document("_id", "a"), COLLECTION);
            }
            return null;
        })).isInstanceOf(DuplicateKeyException.class);

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
        MongoDatabase rival = outsideTransaction.getDb();

        executor.inTransaction(() -> {
            runs.incrementAndGet();
            if (!inTransaction.exists(Query.query(Criteria.where("_id").is("a")), COLLECTION)) {
                if (runs.get() == 1) {
                    rival.getCollection(COLLECTION).insertOne(new Document("_id", "a"));
                }
                inTransaction.insert(new Document("_id", "a"), COLLECTION);
            }
            return null;
        });

        assertThat(runs).hasValue(2);
    }

    private static void failOnASecondRun(AtomicInteger runs) {
        if (runs.incrementAndGet() > 1) {
            throw new AssertionError("The unit of work ran again, so the error was retried");
        }
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        mongoClient.getDatabase("admin").runCommand(command);
    }
}
