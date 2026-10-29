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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A scheduled executor stops running a task once one execution throws, and nothing reads what the execution threw.
 * Rethrowing an {@link Error} from a refresh round would reach nobody. It would only leave the node unable to refresh a
 * lease it holds, or to take one that has become free, for as long as it runs, so the round is logged and the next one
 * runs. The same goes for the consumers within one round. An {@link Error} from one consumer's refresh must not leave
 * the consumers after it unrefreshed, which would let their leases expire. And a grant the notifier threw an
 * {@link Error} on must reach the listeners in a later round, since no other round decides it again. So must a change
 * whose hand-over or telling throws, or whose task the notifier never ran.
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

    @Test
    void grants_a_lease_in_the_round_after_the_notifier_threw_an_Error_on_taking_its_grant() throws InterruptedException {
        // Given
        AtomicReference<Runnable> round = new AtomicReference<>();
        NotifierThatThrowsWhenTold notifier = new NotifierThatThrowsWhenTold();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()), notifier);
        otherNode = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), new ScheduledRefresh((lease, scheduler) -> {
        }));
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        List<String> granted = new CopyOnWriteArrayList<>();
        support.addListener(new CompetingConsumerListener() {
            @Override
            public void onConsumeGranted(String subscriptionId, String subscriberId) {
                granted.add(subscriptionId);
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            }
        });
        assertThat(otherNode.registerCompetingConsumer(locks, "a-subscription", "the-other-node")).isTrue();
        assertThat(support.registerCompetingConsumer(locks, "a-subscription", NODE)).isFalse();
        otherNode.unregisterCompetingConsumer(locks, "a-subscription", "the-other-node");

        // When
        // Thrown deliberately. Nothing here runs out of memory.
        notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));
        // The round that takes the lease, whose grant the notifier throws on, and the round after it
        Throwable thrown = catchThrowable(round.get()::run);
        notifier.takeEveryTask();
        round.get().run();
        notifier.awaitEveryTask();

        // Then
        // A round decides a grant once, and every later round finds the lease held and decides nothing. Dropping the
        // grant would leave the subscription paused on the node that holds its lease, with no other node able to take it.
        assertAll(
                () -> assertThat(thrown).as("thrown by the round whose grant the notifier threw on").isNull(),
                () -> assertThat(granted).containsExactly("a-subscription")
        );
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Decision.class)
    @DisplayName("tells the listeners of a decision in the next round when handing it over threw an Error")
    void tells_the_listeners_of_a_decision_in_the_next_round_when_handing_it_over_threw_an_Error(Decision decision) throws InterruptedException {
        // Given
        AtomicReference<Runnable> round = new AtomicReference<>();
        AtomicLong clock = new AtomicLong();
        ScheduledRefresh handOverThatThrowsOnce = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh())) {
            private final AtomicBoolean thrown = new AtomicBoolean();

            @Override
            void notifyInBackground(String subscriptionId, Runnable tell) {
                if (thrown.compareAndSet(false, true)) {
                    // Thrown deliberately. Nothing here runs out of memory.
                    throw new LinkageError("handing over the tell for " + subscriptionId + " fails");
                }
                super.notifyInBackground(subscriptionId, tell);
            }
        };
        otherNode = aNodeThatNeverRefreshes();
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), handOverThatThrowsOnce, clock::get)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        assertThat(support.registerCompetingConsumer(locks, "the-other-subscription", NODE)).isTrue();
        Calls calls = new Calls();
        support.addListener(calls);
        decision.arrange(this, clock);
        int before = calls.size();
        Object otherLeaseExpiresAtBefore = expiresAtOf("the-other-subscription");
        // expiresAt is computed on the database clock, so a refresh a few milliseconds later writes a later one
        Thread.sleep(50);

        // When
        Throwable thrown = catchThrowable(round.get()::run);
        round.get().run();

        // Then
        // Nothing else decides the change again, so the round after the one whose hand-over threw has to tell it, once.
        // The round that threw must still have refreshed the consumer after it, or that lease would expire.
        assertAll(
                () -> assertThat(thrown).as("thrown by the round whose hand-over threw").isNull(),
                () -> assertThat(calls.since(before)).as("what the listeners were told").containsExactly(decision.told),
                () -> assertThat(expiresAtOf("the-other-subscription")).as("the lease of the other consumer").isNotEqualTo(otherLeaseExpiresAtBefore)
        );
    }

    @Test
    void tells_the_listeners_of_a_grant_in_the_next_round_when_telling_them_threw_an_Error_in_the_strategys_own_code() throws InterruptedException {
        // Given
        Thread testThread = Thread.currentThread();
        AtomicBoolean thrownOnce = new AtomicBoolean();
        LongSupplier clockThatThrowsOnceInTheTell = () -> {
            if (Thread.currentThread() != testThread && thrownOnce.compareAndSet(false, true)) {
                // Thrown deliberately. Nothing here runs out of memory.
                throw new LinkageError("reading the clock in the tell fails");
            }
            return System.nanoTime();
        };
        AtomicReference<Runnable> round = new AtomicReference<>();
        NotifierThatThrowsWhenTold notifier = new NotifierThatThrowsWhenTold();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()), notifier);
        otherNode = aNodeThatNeverRefreshes();
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held, clockThatThrowsOnceInTheTell)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        Calls calls = new Calls();
        support.addListener(calls);
        assertThat(otherNode.registerCompetingConsumer(locks, "a-subscription", "the-other-node")).isTrue();
        assertThat(support.registerCompetingConsumer(locks, "a-subscription", NODE)).isFalse();
        otherNode.unregisterCompetingConsumer(locks, "a-subscription", "the-other-node");

        // When
        // The round that takes the lease, whose tell the clock breaks on the notifier's thread, and the round after it
        round.get().run();
        notifier.awaitEveryTask();
        round.get().run();
        notifier.awaitEveryTask();

        // Then
        // A round decides a grant once, so dropping the tell would leave the subscription paused on the node that holds its lease
        assertAll(
                () -> assertThat(thrownOnce).as("the clock threw in the tell").isTrue(),
                () -> assertThat(calls.since(0)).as("what the listeners were told").containsExactly("granted"),
                () -> assertThat(support.hasLock("a-subscription", NODE)).isTrue()
        );
    }

    @Test
    void tells_the_listeners_of_a_change_in_the_next_round_when_the_notifier_took_its_task_and_never_ran_it() throws InterruptedException {
        // Given
        AtomicReference<Runnable> round = new AtomicReference<>();
        NotifierThatDropsTheFirstTask notifier = new NotifierThatDropsTheFirstTask();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()), notifier);
        otherNode = aNodeThatNeverRefreshes();
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        Calls calls = new Calls();
        support.addListener(calls);
        assertThat(otherNode.registerCompetingConsumer(locks, "a-subscription", "the-other-node")).isTrue();
        assertThat(support.registerCompetingConsumer(locks, "a-subscription", NODE)).isFalse();
        otherNode.unregisterCompetingConsumer(locks, "a-subscription", "the-other-node");

        // When
        // The round that takes the lease, whose task the notifier drops, and the round after it
        round.get().run();
        round.get().run();
        notifier.awaitEveryTask();

        // Then
        // Nothing says the dropped task was lost, so the grant has to come from the round after it
        assertThat(calls.since(0)).as("after the round that followed the dropped task").containsExactly("granted");

        // When
        expireLeaseOf("a-subscription");
        assertThat(otherNode.registerCompetingConsumer(locks, "a-subscription", "the-other-node")).isTrue();
        round.get().run();
        notifier.awaitEveryTask();

        // Then
        assertThat(calls.since(0)).as("after the round that found the lease taken").containsExactly("granted", "prohibited");
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
     * Logging what a refresh threw can throw as well, as it does when memory runs out, and that must not end the round
     * either.
     */
    @ParameterizedTest
    @ValueSource(strings = {"c0", "c1", "c2", "c3"})
    @DisplayName("refreshes the other consumers of the round when the refresh of one of them throws an Error that cannot be logged")
    void refreshes_the_other_consumers_of_the_round_when_the_refresh_of_one_of_them_throws_an_Error_that_cannot_be_logged(String failing) throws InterruptedException {
        // Given
        AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
        support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(
                        throwingForTheLeaseOf(failing, locks, ErrorThatThrowsWhenLogged::new)));
        List<String> consumers = List.of("c0", "c1", "c2", "c3");
        consumers.forEach(id -> assertThat(support.registerCompetingConsumer(locks, id, NODE)).isTrue());
        List<String> others = consumers.stream().filter(id -> !id.equals(failing)).toList();
        Map<String, Object> expiresAtBefore = expiresAtOf(others);
        // expiresAt is computed on the database clock, so a refresh a few milliseconds later writes a later one
        Thread.sleep(50);

        // When
        Throwable thrown = catchThrowable(scheduledRefresh.get()::run);

        // Then
        // The round throws what the refresh threw, not what logging it threw, and the consumers after the failing one
        // are refreshed either way, since their leases would otherwise expire and another node would take them over.
        Map<String, Object> expiresAtAfter = expiresAtOf(others);
        assertAll(
                () -> assertThat(thrown).isInstanceOf(ErrorThatThrowsWhenLogged.class),
                () -> assertThat(others).allSatisfy(id ->
                        assertThat(expiresAtAfter.get(id)).as("the lease of " + id).isNotEqualTo(expiresAtBefore.get(id)))
        );
    }

    /**
     * Rounds run at a fixed rate, the first one right away, so register before scheduling the refresh.
     */
    private static ScheduledRefresh everyRound() {
        return new ScheduledRefresh((lease, scheduler) -> scheduler.fixedRate(Duration.ZERO, ROUND_EVERY));
    }

    private static MongoLeaseCompetingConsumerStrategySupport aNodeThatNeverRefreshes() {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), new ScheduledRefresh((lease, scheduler) -> {
        }));
    }

    private void expireLeaseOf(String subscriptionId) {
        locks.updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minus(Duration.ofSeconds(2))));
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
    private static MongoCollection<BsonDocument> throwingAnErrorForTheLeaseOf(String subscriptionId, MongoCollection<BsonDocument> delegate) {
        return throwingForTheLeaseOf(subscriptionId, delegate, () -> new LinkageError("refreshing the lease of " + subscriptionId + " fails"));
    }

    /**
     * Like {@link #throwingAnErrorForTheLeaseOf}, throwing what {@code error} makes.
     */
    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> throwingForTheLeaseOf(String subscriptionId, MongoCollection<BsonDocument> delegate, Supplier<Error> error) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    boolean write = method.getName().equals("updateOne") || method.getName().equals("findOneAndUpdate");
                    if (write && args[0] instanceof Bson filter
                            && filter.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()).toJson().contains("\"" + subscriptionId + "\"")) {
                        throw error.get();
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof MongoCollection<?> another
                                ? throwingForTheLeaseOf(subscriptionId, (MongoCollection<BsonDocument>) another, error)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /**
     * What a round decides about {@code a-subscription} and hands over, each arranged by {@link #arrange} against a
     * node that holds the other subscription. {@link #told} is the one call the listeners are to hear of it.
     */
    private enum Decision {
        GRANT("a grant", "granted") {
            @Override
            void arrange(MongoLeaseRefreshErrorTest test, AtomicLong clock) {
                assertThat(test.otherNode.registerCompetingConsumer(test.locks, "a-subscription", "the-other-node")).isTrue();
                assertThat(test.support.registerCompetingConsumer(test.locks, "a-subscription", NODE)).isFalse();
                test.otherNode.unregisterCompetingConsumer(test.locks, "a-subscription", "the-other-node");
            }
        },
        PROHIBITION("a prohibition", "prohibited") {
            @Override
            void arrange(MongoLeaseRefreshErrorTest test, AtomicLong clock) {
                assertThat(test.support.registerCompetingConsumer(test.locks, "a-subscription", NODE)).isTrue();
                test.expireLeaseOf("a-subscription");
                assertThat(test.otherNode.registerCompetingConsumer(test.locks, "a-subscription", "the-other-node")).isTrue();
            }
        },
        REGRANT_AFTER_HAS_LOCK_ANSWERED_NO("a regrant after hasLock answered no", "granted") {
            @Override
            void arrange(MongoLeaseRefreshErrorTest test, AtomicLong clock) {
                assertThat(test.support.registerCompetingConsumer(test.locks, "a-subscription", NODE)).isTrue();
                clock.addAndGet(LEASE.toNanos());
                assertThat(test.support.hasLock("a-subscription", NODE)).isFalse();
            }
        };

        private final String label;
        private final String told;

        Decision(String label, String told) {
            this.label = label;
            this.told = told;
        }

        abstract void arrange(MongoLeaseRefreshErrorTest test, AtomicLong clock);

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * Records each call it is told of, as {@code granted} or {@code prohibited}.
     */
    private static final class Calls implements CompetingConsumerListener {
        private final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            calls.add("granted");
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            calls.add("prohibited");
        }

        int size() {
            return calls.size();
        }

        List<String> since(int from) {
            List<String> all = List.copyOf(calls);
            return all.subList(from, all.size());
        }
    }
}
