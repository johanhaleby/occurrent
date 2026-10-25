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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A scheduled executor stops running a task once one execution throws, and nothing reads what the execution threw.
 * Throwing an {@link Error} on from a refresh round would reach nobody. It would only leave the node unable to refresh a
 * lease it holds, or to take one that has become free, for as long as it runs, so the round is logged and the next one
 * runs. The same goes for the consumers within one round. An {@link Error} from one consumer's refresh must not leave
 * the consumers after it unrefreshed, which would let their leases expire.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease refresh that throws an Error")
@Timeout(60)
class MongoLeaseRefreshErrorTest {

    private static final String DATABASE = "mongoleaserefresherror";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final Duration ROUND_EVERY = Duration.ofMillis(100);
    private static final String NODE = "the-node";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private String collectionName;
    private MongoCollection<BsonDocument> locks;
    private MongoLeaseCompetingConsumerStrategySupport support;
    private MongoLeaseCompetingConsumerStrategySupport otherNode;

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
    void shutDownAndDropTheLocks() {
        if (support != null) {
            support.shutdown();
        }
        if (otherNode != null) {
            otherNode.shutdown();
        }
        locks.drop();
    }

    @Test
    void refreshes_a_lease_it_holds_in_the_round_after_the_one_that_threw() {
        // Given
        AtomicInteger rounds = new AtomicInteger();
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), everyRound());
        assertThat(support.registerCompetingConsumer(locks, "a-subscription", NODE)).isTrue();
        Object expiresAtBefore = expiresAtOf("a-subscription");

        // When
        support.scheduleRefresh(refreshOrAcquire -> () -> {
            if (rounds.incrementAndGet() == 1) {
                throw new Error("the first round fails");
            }
            refreshOrAcquire.accept(locks);
        });

        // Then
        // Continuing is the right behaviour. Rethrowing from the round reaches nobody, because the ScheduledFuture is
        // discarded, and it cancels every later round, so the lease is not refreshed again before it expires.
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(expiresAtOf("a-subscription")).as("the lease is refreshed by a later round").isNotEqualTo(expiresAtBefore));
        assertThat(support.hasLock("a-subscription", NODE)).isTrue();
    }

    @Test
    void acquires_a_lease_it_did_not_get_at_registration_in_the_round_after_the_one_that_threw() {
        // Given
        AtomicInteger rounds = new AtomicInteger();
        otherNode = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), new ScheduledRefresh((lease, scheduler) -> {
        }));
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), everyRound());
        assertThat(otherNode.registerCompetingConsumer(locks, "a-subscription", "the-other-node")).isTrue();
        assertThat(support.registerCompetingConsumer(locks, "a-subscription", NODE)).isFalse();
        otherNode.unregisterCompetingConsumer(locks, "a-subscription", "the-other-node");

        // When
        support.scheduleRefresh(refreshOrAcquire -> () -> {
            if (rounds.incrementAndGet() == 1) {
                throw new Error("the first round fails");
            }
            refreshOrAcquire.accept(locks);
        });

        // Then
        // Continuing is the right behaviour. Rethrowing from the round reaches nobody, because the ScheduledFuture is
        // discarded, and it cancels every later round, so this node never takes the lease that became free.
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(support.hasLock("a-subscription", NODE)).as("the free lease is taken by a later round").isTrue());
    }

    /**
     * The consumers of a round are refreshed in no particular order, so each of them takes its turn as the one whose
     * refresh throws. For all but the last consumer of the round, the consumers after it are the ones that go unrefreshed
     * when the Error stops the round.
     */
    @ParameterizedTest
    @ValueSource(strings = {"c0", "c1", "c2", "c3"})
    @DisplayName("refreshes the other consumers of the round when the refresh of one of them throws an Error")
    void refreshes_the_other_consumers_of_the_round_when_the_refresh_of_one_of_them_throws_an_Error(String failing) throws InterruptedException {
        // Given
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(throwingAnErrorForTheLeaseOf(failing, locks)));
        List<String> consumers = List.of("c0", "c1", "c2", "c3");
        consumers.forEach(id -> assertThat(support.registerCompetingConsumer(locks, id, NODE)).isTrue());
        List<String> others = consumers.stream().filter(id -> !id.equals(failing)).toList();
        Map<String, Object> expiresAtBefore = expiresAtOf(others);
        // expiresAt is computed on the database clock, so a refresh a few milliseconds later writes a later one
        Thread.sleep(50);

        // When
        Throwable thrown = catchThrowable(scheduledRefresh.get()::run);

        // Then
        // Continuing is the right behaviour. A consumer whose refresh throws must not leave the consumers after it
        // unrefreshed, since their leases would expire and another node would take them over. The Error itself is
        // still thrown, once every consumer has had its turn, so the schedule logs it.
        Map<String, Object> expiresAtAfter = expiresAtOf(others);
        assertAll(
                () -> assertThat(thrown).isInstanceOf(LinkageError.class),
                () -> assertThat(others).allSatisfy(id ->
                        assertThat(expiresAtAfter.get(id)).as("the lease of " + id).isNotEqualTo(expiresAtBefore.get(id)))
        );
    }

    /**
     * Rounds run by a real schedule, the first one right away, so register before scheduling the refresh.
     */
    private static ScheduledRefresh everyRound() {
        return new ScheduledRefresh((lease, scheduler) -> scheduler.fixedRate(Duration.ZERO, ROUND_EVERY));
    }

    private Object expiresAtOf(String subscriptionId) {
        return database.getCollection(collectionName).find(eq("_id", subscriptionId)).first().get("expiresAt");
    }

    private Map<String, Object> expiresAtOf(List<String> subscriptionIds) {
        return subscriptionIds.stream().collect(Collectors.toMap(Function.identity(), this::expiresAtOf));
    }

    /**
     * A collection that throws an {@link Error} for every write to the lease of {@code subscriptionId}, and forwards
     * everything else. Writes are told apart by the filter they are given, which names the lease by its {@code _id}.
     */
    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> throwingAnErrorForTheLeaseOf(String subscriptionId, MongoCollection<BsonDocument> delegate) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    boolean write = method.getName().equals("updateOne") || method.getName().equals("findOneAndUpdate");
                    if (write && args[0] instanceof Bson filter
                            && filter.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()).toJson().contains("\"" + subscriptionId + "\"")) {
                        throw new LinkageError("refreshing the lease of " + subscriptionId + " fails");
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? throwingAnErrorForTheLeaseOf(subscriptionId, (MongoCollection<BsonDocument>) another)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
