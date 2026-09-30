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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * When MongoDB refuses to write one consumer's lease, the other consumers of the same refresh round are still refreshed.
 * The refusal is a collection validator that rejects any write to that one lease document.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease refresh round with one lease that cannot be written")
@Timeout(30)
class MongoLeaseRefreshRoundIsolationTest {

    private static final String DATABASE = "mongoleaserefreshroundisolation";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String FAILING = "failing";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private String collectionName;
    private MongoCollection<BsonDocument> locks;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void startWithNoLocks() {
        collectionName = "competing-consumer-locks-" + UUID.randomUUID();
        locks = database.getCollection(collectionName, BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void refreshes_every_other_lease_when_one_lease_cannot_be_written() throws InterruptedException {
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        MongoLeaseCompetingConsumerStrategySupport support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        List<String> healthy = IntStream.range(0, 10).mapToObj(i -> "healthy-" + i).toList();
        assertThat(support.registerCompetingConsumer(locks, FAILING, "the-node")).isTrue();
        healthy.forEach(id -> assertThat(support.registerCompetingConsumer(locks, id, "the-node")).isTrue());
        Map<String, Object> expiresAtBefore = expiresAtOf(healthy);

        database.runCommand(new Document("collMod", collectionName)
                .append("validator", new Document("$expr", new Document("$ne", List.of("$_id", FAILING))))
                .append("validationLevel", "strict").append("validationAction", "error"));
        // expiresAt is computed on the database clock, so a refresh a few milliseconds later writes a later one
        Thread.sleep(50);

        scheduledRefresh.get().run();

        Map<String, Object> expiresAtAfter = expiresAtOf(healthy);
        assertThat(healthy)
                .as("every healthy lease is refreshed in the same round as the one MongoDB refuses to write")
                .allSatisfy(id -> assertThat(expiresAtAfter.get(id)).as(id).isNotEqualTo(expiresAtBefore.get(id)));
        assertThat(healthy).allSatisfy(id -> assertThat(support.hasLock(id, "the-node")).as(id).isTrue());
        support.shutdown();
    }

    private Map<String, Object> expiresAtOf(List<String> subscriptionIds) {
        return subscriptionIds.stream().collect(Collectors.toMap(Function.identity(),
                id -> database.getCollection(collectionName).find(eq("_id", id)).first().get("expiresAt")));
    }
}
