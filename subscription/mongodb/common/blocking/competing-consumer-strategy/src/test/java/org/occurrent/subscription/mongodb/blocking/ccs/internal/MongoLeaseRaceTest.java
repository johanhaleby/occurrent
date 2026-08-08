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
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What happens when the scheduled refresh and an application thread are inside the same consumer at the same time.
 * The support class reads a consumer's status, makes the MongoDB call that status decides, and writes the result
 * back, and until <a href="https://github.com/johanhaleby/occurrent/issues/651">#651</a> nothing stopped two threads
 * from being in that sequence at once. The one that finished last wrote a result it had worked out against a status
 * that was no longer there.
 * <p>
 * <strong>These interleave on purpose, they do not hope to.</strong> Every test here holds a thread inside its
 * MongoDB call and lets the other one run, using a collection that stands still when asked rather than a sleep and a
 * repeat count. A test built the other way passes on a fast machine whatever the code does.
 * <p>
 * The lease that decides which <em>node</em> consumes is not what any of this is about. That is the lock document,
 * and {@link MongoLeaseTimingTest} covers it. This is about one node's view of its own consumers.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("two threads inside one competing consumer")
// Every wait here has a limit of its own, so this only has to catch a thread that never comes back, which is what
// a lock held across a listener callback would look like.
@Timeout(30)
class MongoLeaseRaceTest {

    private static final String DATABASE = "mongoleaserace";
    private static final Duration LEASE = Duration.ofMinutes(10);
    /**
     * Long enough that a thread which is going to reach MongoDB has done so, and only ever waited out in full when
     * the thread is correctly blocked and the test is about to assert exactly that.
     */
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;
    private MutableClock clock;

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
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
        clock = new MutableClock(Instant.parse("2026-08-08T12:00:00Z"));
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void does_not_leave_the_lease_with_a_consumer_unregistered_while_the_refresh_was_taking_it() throws Exception {
        String subscription = "a-subscription";
        HeldCall held = new HeldCall();
        Node rival = new Node("the-rival", locks);
        Node node = new Node("the-node", intercepting(locks, held));

        assertThat(rival.register(subscription)).isTrue();
        assertThat(node.register(subscription)).isFalse();
        // The rival stops refreshing, so the node's next round is a round it wins.
        clock.advanceBy(LEASE.plusMillis(1));

        Thread refresh = run(node::refresh);
        assertThat(held.awaitArrival()).isEqualTo(subscription);
        CountDownLatch unregisterReachedMongo = new CountDownLatch(1);
        Thread unregister = run(() -> node.unregister(subscription, intercepting(locks, signalling(unregisterReachedMongo))));

        assertThat(unregisterReachedMongo.await(LONG_ENOUGH.toMillis(), TimeUnit.MILLISECONDS))
                .as("the unregister has to wait for the acquire it landed in the middle of, or it gives up a lease "
                        + "the node has not taken yet and the acquire puts the consumer back afterwards")
                .isFalse();
        held.letGo();
        refresh.join();
        unregister.join();

        assertThat(node.hasLock(subscription)).isFalse();
        assertThat(new Node("a-late-comer", locks).register(subscription))
                .as("a consumer nobody registered holding the lease is the whole cost of this race, since no node "
                        + "can take that subscription over for as long as the process lives")
                .isTrue();
    }

    @Test
    void reports_one_change_of_status_once_when_two_threads_acquire_at_the_same_time() throws Exception {
        String subscription = "a-subscription";
        HeldCall held = new HeldCall();
        Node rival = new Node("the-rival", locks);
        Node node = new Node("the-node", intercepting(locks, held));

        assertThat(rival.register(subscription)).isTrue();
        assertThat(node.register(subscription)).isFalse();
        clock.advanceBy(LEASE.plusMillis(1));

        Thread refresh = run(node::refresh);
        held.awaitArrival();
        CountDownLatch registerReachedMongo = new CountDownLatch(1);
        Thread register = run(() -> node.register(subscription, intercepting(locks, signalling(registerReachedMongo))));

        assertThat(registerReachedMongo.await(LONG_ENOUGH.toMillis(), TimeUnit.MILLISECONDS))
                .as("both threads reading the old status before either writes is what makes them both call this a "
                        + "change from not having the lease to having it")
                .isFalse();
        held.letGo();
        refresh.join();
        register.join();

        assertThat(node.changes)
                .as("the consumer went from not having the lease to having it once, so a listener driven by "
                        + "callbacks has to hear about it once")
                .containsExactly("granted:" + subscription);
        assertThat(node.hasLock(subscription)).isTrue();
    }

    @Test
    void leaves_the_consumer_as_it_was_when_the_lease_call_fails() throws Exception {
        String subscription = "a-subscription";
        Node node = new Node("the-node", locks);
        assertThat(node.register(subscription)).isTrue();

        assertThatThrownBy(() -> node.register(subscription, intercepting(locks, failing())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("MongoDB is having a bad day");

        assertThat(node.hasLock(subscription))
                .as("a call that never reached MongoDB changed nothing, so the consumer is what it was")
                .isTrue();
        assertThat(node.changes).containsExactly("granted:" + subscription);

        // On another thread on purpose. The lock is reentrant, so the thread that threw is let back in whether the
        // failed call gave the consumer back or not, and asking it again would prove nothing.
        AtomicReference<Boolean> acquiredAfterwards = new AtomicReference<>();
        Thread afterwards = run(() -> acquiredAfterwards.set(node.register(subscription)));
        afterwards.join(LONG_ENOUGH.multipliedBy(2).toMillis());

        assertThat(acquiredAfterwards.get())
                .as("and the consumer was given back, or every later call for it waits behind one that already threw")
                .isTrue();
    }

    @Test
    void does_not_hold_a_consumer_while_telling_a_listener_about_it() throws Exception {
        String subscription = "a-subscription";
        HeldCall held = new HeldCall();
        // The listeners a subscription model registers are synchronized on the model, and an application thread
        // holds that same monitor while it pauses or unregisters. This stands in for the model.
        Object model = new Object();
        Node rival = new Node("the-rival", locks);
        Node node = new Node("the-node", intercepting(locks, held), model);

        assertThat(rival.register(subscription)).isTrue();
        assertThat(node.register(subscription)).isFalse();
        clock.advanceBy(LEASE.plusMillis(1));

        Thread refresh = run(node::refresh);
        held.awaitArrival();
        CountDownLatch holdingTheModel = new CountDownLatch(1);
        CountDownLatch unregisterReachedMongo = new CountDownLatch(1);
        Thread application = run(() -> {
            synchronized (model) {
                holdingTheModel.countDown();
                node.unregister(subscription, intercepting(locks, signalling(unregisterReachedMongo)));
            }
        });
        holdingTheModel.await();
        assertThat(unregisterReachedMongo.await(LONG_ENOUGH.toMillis(), TimeUnit.MILLISECONDS)).isFalse();

        held.letGo();
        // Telling the listener while still holding this consumer is what closes the circle. The round then waits for
        // the model the application thread holds, and the application thread waits for the consumer the round holds,
        // and neither of these joins comes back.
        refresh.join();
        application.join();

        assertThat(node.hasLock(subscription)).isFalse();
    }

    private static Thread run(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }

    /**
     * One competing consumer strategy with its own view of its consumers, standing in for one application instance.
     * Its refresh is held rather than scheduled, so {@link #refresh()} is the only thing that runs it.
     */
    private class Node {
        private final String subscriberId;
        private final MongoLeaseCompetingConsumerStrategySupport support;
        private final AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        private final List<String> changes = Collections.synchronizedList(new ArrayList<>());

        private Node(String subscriberId, MongoCollection<BsonDocument> collectionForRefresh) {
            this(subscriberId, collectionForRefresh, new Object());
        }

        /**
         * @param model What the listener holds while it runs, standing in for a subscription model whose callbacks
         *              are synchronized on itself.
         */
        private Node(String subscriberId, MongoCollection<BsonDocument> collectionForRefresh, Object model) {
            this.subscriberId = subscriberId;
            ScheduledRefresh heldRefresh = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
            this.support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, clock, RetryStrategy.none(), heldRefresh)
                    .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(collectionForRefresh));
            this.support.addListener(new CompetingConsumerListener() {
                @Override
                public void onConsumeGranted(String subscriptionId, String subscriberId) {
                    record("granted:" + subscriptionId);
                }

                @Override
                public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                    record("prohibited:" + subscriptionId);
                }

                private void record(String change) {
                    synchronized (model) {
                        changes.add(change);
                    }
                }
            });
        }

        private boolean register(String subscriptionId) {
            return register(subscriptionId, locks);
        }

        private boolean register(String subscriptionId, MongoCollection<BsonDocument> collection) {
            return support.registerCompetingConsumer(collection, subscriptionId, subscriberId);
        }

        private void unregister(String subscriptionId, MongoCollection<BsonDocument> collection) {
            support.unregisterCompetingConsumer(collection, subscriptionId, subscriberId);
        }

        private void release(String subscriptionId, MongoCollection<BsonDocument> collection) {
            support.releaseCompetingConsumer(collection, subscriptionId, subscriberId);
        }

        private boolean hasLock(String subscriptionId) {
            return support.hasLock(subscriptionId, subscriberId);
        }

        private void refresh() {
            scheduledRefresh.get().run();
        }
    }

    /**
     * What a collection does when the code under test calls it. Everything else it forwards.
     */
    @FunctionalInterface
    private interface Intercept {
        void before(String subscriptionId);
    }

    /**
     * Holds the first call that arrives until the test lets it go, and tells the test which subscription it was for.
     */
    private static class HeldCall implements Intercept {
        private final CountDownLatch arrived = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicReference<String> heldFor = new AtomicReference<>();
        private final AtomicBoolean stillArmed = new AtomicBoolean(true);

        @Override
        public void before(String subscriptionId) {
            if (stillArmed.compareAndSet(true, false)) {
                heldFor.set(subscriptionId);
                arrived.countDown();
                awaitUninterruptibly(released);
            }
        }

        private String awaitArrival() {
            awaitUninterruptibly(arrived);
            return heldFor.get();
        }

        private void letGo() {
            released.countDown();
        }

        private static void awaitUninterruptibly(CountDownLatch latch) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    private static Intercept signalling(CountDownLatch reached) {
        return __ -> reached.countDown();
    }

    private static Intercept failing() {
        return __ -> {
            throw new IllegalStateException("MongoDB is having a bad day");
        };
    }

    /**
     * The collection the code under test is handed, standing between it and the real one. Every call the strategy
     * makes to the server goes through {@code intercept} first, which is what lets a test hold a thread exactly
     * where the race condition is rather than somewhere near it.
     */
    private static final Set<String> CALLS_TO_THE_SERVER = Set.of("findOneAndUpdate", "deleteOne", "updateOne");

    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> intercepting(MongoCollection<BsonDocument> delegate, Intercept intercept) {
        return (MongoCollection<BsonDocument>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (CALLS_TO_THE_SERVER.contains(method.getName())) {
                        intercept.before(subscriptionIdIn(args));
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        // withWriteConcern and its like hand back another collection, which has to stand between the
                        // code and the server as well or the call the strategy actually makes goes around this.
                        return result instanceof MongoCollection<?> another
                                ? intercepting((MongoCollection<BsonDocument>) another, intercept)
                                : result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /**
     * Every call the strategy makes to the server filters on the subscription id, which is the lock document's id.
     */
    private static @Nullable String subscriptionIdIn(Object[] args) {
        if (args == null || args.length == 0 || !(args[0] instanceof Bson filter)) {
            return null;
        }
        BsonDocument asDocument = filter.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
        if (asDocument.containsKey("_id")) {
            return asDocument.getString("_id").getValue();
        }
        return asDocument.getArray("$and", new BsonArray()).stream()
                .map(BsonValue::asDocument)
                .filter(document -> document.containsKey("_id"))
                .map(document -> document.getString("_id").getValue())
                .findFirst()
                .orElse(null);
    }

    private static class MutableClock extends Clock {
        private volatile Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void advanceBy(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("This clock is UTC and the code under test never asks for another zone");
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
