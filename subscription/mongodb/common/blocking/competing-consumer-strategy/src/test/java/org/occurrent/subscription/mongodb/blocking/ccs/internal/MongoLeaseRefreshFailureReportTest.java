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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A database that answers reads while it refuses writes leaves every node unable to take or refresh a lease, so no node
 * runs the subscription while every node reports itself as standing by and the database looks healthy. The strategy
 * therefore reports whether its latest refresh attempt failed. The rounds are run by hand, so what each one saw is
 * known.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy reporting its failing lease refresh")
@Timeout(30)
class MongoLeaseRefreshFailureReportTest {

    private static final String DATABASE = "mongoleaserefreshfailurereport";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";
    private static final String NODE = "the-node";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;
    private final AtomicBoolean writesFail = new AtomicBoolean();
    private final AtomicInteger writesRefused = new AtomicInteger();
    private final AtomicReference<Runnable> round = new AtomicReference<>();
    private MongoLeaseCompetingConsumerStrategySupport support;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void aNodeHoldingALeaseWhoseRoundsAreRunByHand() {
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()));
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(writesFail.get() ? starving(locks) : locks));
        assertThat(support.registerCompetingConsumer(locks, SUBSCRIPTION, NODE)).isTrue();
    }

    @AfterEach
    void shutDownAndDropTheLocks() {
        support.shutdown();
        locks.drop();
    }

    @Test
    void reports_no_failure_before_any_round_has_failed() {
        round.get().run();

        assertThat(support.leaseRefreshFailure()).isEmpty();
    }

    @Test
    void reports_the_failure_of_a_round_whose_lease_writes_are_refused_while_its_reads_work() {
        writesFail.set(true);

        round.get().run();

        LeaseRefreshFailure failure = support.leaseRefreshFailure().orElseThrow(() -> new AssertionError("expected the refresh to be reported as failing"));
        assertThat(failure.cause()).hasMessage("MongoDB refuses writes (1)");
        assertThat(failure.since()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    void keeps_when_the_failures_began_while_the_latest_cause_replaces_the_earlier_one() {
        writesFail.set(true);
        round.get().run();
        LeaseRefreshFailure first = support.leaseRefreshFailure().orElseThrow();
        await().until(() -> Instant.now().isAfter(first.since()));

        round.get().run();

        LeaseRefreshFailure second = support.leaseRefreshFailure().orElseThrow(() -> new AssertionError("expected the refresh to still be reported as failing"));
        assertThat(second.since()).isEqualTo(first.since());
        assertThat(second.cause()).hasMessage("MongoDB refuses writes (2)");
    }

    @Test
    void reports_no_failure_once_a_round_refreshes_every_lease_again() {
        writesFail.set(true);
        round.get().run();
        assertThat(support.leaseRefreshFailure()).isPresent();
        writesFail.set(false);

        round.get().run();

        assertThat(support.leaseRefreshFailure()).isEmpty();
    }

    @Test
    void starts_a_new_run_of_failures_from_when_the_next_failure_happens_after_a_round_that_worked() {
        writesFail.set(true);
        round.get().run();
        Instant firstRunBegan = support.leaseRefreshFailure().orElseThrow().since();
        await().until(() -> Instant.now().isAfter(firstRunBegan));
        writesFail.set(false);
        round.get().run();
        writesFail.set(true);

        round.get().run();

        assertThat(support.leaseRefreshFailure().orElseThrow().since()).isAfter(firstRunBegan);
    }

    @Test
    void reports_no_failure_for_a_round_that_fails_after_shutdown_since_that_is_the_shutdown_cutting_it_short() {
        support.shutdown();
        writesFail.set(true);

        round.get().run();

        assertThat(support.leaseRefreshFailure()).isEmpty();
    }

    /**
     * A collection that answers reads and refuses every call that writes, standing in for a database that is read only
     * from this node's point of view. Everything else is forwarded.
     */
    private static final Set<String> WRITES = Set.of("findOneAndUpdate", "deleteOne", "updateOne");

    @SuppressWarnings("unchecked")
    private MongoCollection<BsonDocument> starving(MongoCollection<BsonDocument> delegate) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (WRITES.contains(method.getName())) {
                        throw new IllegalStateException("MongoDB refuses writes (" + writesRefused.incrementAndGet() + ")");
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? starving((MongoCollection<BsonDocument>) another)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
