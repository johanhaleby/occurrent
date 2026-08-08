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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a MongoDB lease is up, and what refreshing one does. None of this is in the competing-consumer contract: the
 * conformance suite asserts that a holder which stops coordinating loses the lock before long, and says nothing about
 * how long or about leases at all, because a strategy backed by a leader election or a broker owes the same property
 * and has no lease to speak of. What the lease itself does is this class's subject, and it belongs here rather than in
 * either strategy module because both reach exactly this code through a different MongoDB API.
 * <p>
 * <strong>No sleep waits out a lease.</strong> {@code expiresAt} is judged against the database's own clock, so
 * nothing in the test process can move it. A test that needs a lease to look expired, or close to expiring, writes
 * {@code expiresAt} on the lock document directly instead, and a real refresh then overwrites it from the database's
 * actual current time. The support class's {@link ScheduledRefresh} still decides when a refresh runs, and holding it
 * rather than scheduling it is what leaves each {@link Consumer#refresh()} call in charge of that, instead of a
 * background thread on its own timer. The alternative is a short lease and a sleep, which is slower and weaker: it
 * cannot tell "the lease was up" from "the machine was busy".
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease")
// Nothing here waits, so this only has to catch a MongoDB call that never comes back. Without it such a call hangs the
// shard for its full 20 minutes, and this shard has no rerun to fall back on.
@Timeout(30)
class MongoLeaseTimingTest {

    private static final String DATABASE = "mongoleasetiming";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";
    /**
     * How far from the real current instant a seeded {@code expiresAt} sits, in either direction. Long enough that
     * the round trip to seed it and the round trip the test makes afterwards cannot cross it on a loaded CI runner,
     * short enough next to {@link #LEASE} that "close to expiring" and "just expired" still test the boundary rather
     * than a lease that is, for the test's purposes, simply fresh or simply gone.
     */
    private static final Duration MARGIN = Duration.ofSeconds(2);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

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
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    /**
     * Writes {@code expiresAt} on the subscription's lock document directly, standing in for what a real lease
     * would look like at that instant. The production code judges expiry against the database's own clock, so
     * seeding a value relative to the real {@link Instant#now()} is what makes a lease look expired, or close to
     * expiring, without moving anything.
     */
    private void seedExpiresAt(Instant expiresAt) {
        locks.updateOne(eq("_id", SUBSCRIPTION), set("expiresAt", expiresAt));
    }

    private Instant readExpiresAt() {
        BsonDocument document = locks.find(eq("_id", SUBSCRIPTION)).first();
        return Instant.ofEpochMilli(document.getDateTime("expiresAt").getValue());
    }

    @Test
    void is_still_held_a_moment_before_it_is_up() {
        Consumer holder = new Consumer("the-holder");
        assertThat(holder.register()).isTrue();

        seedExpiresAt(Instant.now().plus(MARGIN));

        assertThat(new Consumer("the-rival").register())
                .as("a rival taking a lease over early is the one thing a lease is there to prevent, since both would "
                        + "then consume the same subscription")
                .isFalse();
    }

    @Test
    void is_up_for_grabs_once_it_is_up() {
        Consumer holder = new Consumer("the-holder");
        assertThat(holder.register()).isTrue();

        seedExpiresAt(Instant.now().minus(MARGIN));

        assertThat(new Consumer("the-rival").register())
                .as("a lease nobody renewed has to become available, or an instance that died holds its subscription "
                        + "for good")
                .isTrue();
    }

    @Test
    void runs_from_the_refresh_rather_than_from_when_it_was_taken() {
        Consumer holder = new Consumer("the-holder");
        holder.register();

        // As if the lease taken above were about to run out, rather than fresh.
        seedExpiresAt(Instant.now().plus(MARGIN));
        holder.refresh();

        assertThat(readExpiresAt())
                .as("a refresh computed from the moment it ran, not extended from whatever expiresAt held before it, "
                        + "or a holder that is alive and refreshing on schedule loses its subscription every lease "
                        + "anyway")
                .isAfter(Instant.now().plus(LEASE.dividedBy(2)));
        assertThat(new Consumer("the-rival").register()).isFalse();
        assertThat(holder.hasLock()).isTrue();
    }

    @Test
    void goes_to_a_rival_that_keeps_asking_once_the_holder_stops_refreshing() {
        Consumer holder = new Consumer("the-holder");
        Consumer rival = new Consumer("the-rival");
        holder.register();
        assertThat(rival.register()).isFalse();

        // As if the holder never refreshed again, which is what it looks like from MongoDB when its process is gone.
        seedExpiresAt(Instant.now().minus(MARGIN));
        rival.refresh();

        assertThat(rival.hasLock()).isTrue();
        assertThat(rival.changes)
                .as("the rival is not registering again here, so being told is the only way a consumer driven by "
                        + "callbacks finds out it may start")
                .containsExactly("granted");
    }

    @Test
    void is_reported_lost_to_the_holder_on_its_next_refresh() {
        Consumer holder = new Consumer("the-holder");
        Consumer rival = new Consumer("the-rival");
        holder.register();
        rival.register();
        seedExpiresAt(Instant.now().minus(MARGIN));
        rival.refresh();

        // The holder comes back and refreshes, late.
        holder.refresh();

        assertThat(holder.hasLock())
                .as("the lease is the rival's now, and a holder still consuming on the strength of a lease it lost is "
                        + "two consumers on one subscription")
                .isFalse();
        assertThat(holder.changes).containsExactly("granted", "prohibited");
        assertThat(rival.hasLock()).isTrue();
    }

    @Test
    void is_given_up_the_moment_the_holder_releases_it() {
        Consumer holder = new Consumer("the-holder");
        holder.register();

        holder.release();

        assertThat(holder.hasLock())
                .as("the holder does not have the lease any more, and a consumer that asks rather than being told has "
                        + "nothing else to go on")
                .isFalse();
        assertThat(holder.changes)
                .as("and it is told once that the lease is gone, by the release itself")
                .containsExactly("granted", "prohibited");
    }

    @Test
    void is_not_taken_back_until_the_round_after_a_release() {
        Consumer holder = new Consumer("the-holder");
        holder.register();
        holder.release();

        holder.refresh();

        assertThat(holder.hasLock())
                .as("the round a consumer released in is the round it stands down for, so a rival gets a look at the "
                        + "lease before the consumer that gave it up competes for it again")
                .isFalse();
        assertThat(holder.changes)
                .as("and nothing changed in that round, so the loss is not reported a second time")
                .containsExactly("granted", "prohibited");

        holder.refresh();

        assertThat(holder.hasLock())
                .as("from the next round it is an ordinary candidate again, and with nobody else asking it wins")
                .isTrue();
        assertThat(holder.changes).containsExactly("granted", "prohibited", "granted");
    }

    @Test
    void goes_to_a_rival_even_when_the_holder_that_released_it_refreshes_first() {
        Consumer holder = new Consumer("the-holder");
        Consumer rival = new Consumer("the-rival");
        holder.register();
        rival.register();
        holder.release();

        // The holder refreshes before the rival does, which is the case the round it stands down for is there for. A
        // consumer that competed again straight away would take back the lease it had just given up, and the rival
        // asking a moment later would find it gone.
        holder.refresh();
        rival.refresh();

        assertThat(rival.hasLock())
                .as("standing down for a round is what makes releasing hand the lease over rather than hand it "
                        + "straight back to whoever gave it up")
                .isTrue();
        holder.refresh();
        assertThat(holder.hasLock())
                .as("and the rival's lease is not up, so the holder competing again does not take it")
                .isFalse();
    }

    /**
     * One competing consumer with a support of its own, standing in for one application instance. Its refresh is held
     * rather than scheduled, so {@link #refresh()} is the only thing that runs it.
     */
    private class Consumer {
        private final String subscriberId;
        private final MongoLeaseCompetingConsumerStrategySupport support;
        private final AtomicReference<Runnable> scheduledRefresh = new AtomicReference<>();
        private final List<String> changes = new ArrayList<>();

        private Consumer(String subscriberId) {
            this.subscriberId = subscriberId;
            ScheduledRefresh held = new ScheduledRefresh((lease, scheduler) -> scheduledRefresh.set(scheduler.refresh()));
            this.support = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), held)
                    .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
            this.support.addListener(new CompetingConsumerListener() {
                @Override
                public void onConsumeGranted(String subscriptionId, String subscriberId) {
                    changes.add("granted");
                }

                @Override
                public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                    changes.add("prohibited");
                }
            });
        }

        private boolean register() {
            return support.registerCompetingConsumer(locks, SUBSCRIPTION, subscriberId);
        }

        private void release() {
            support.releaseCompetingConsumer(locks, SUBSCRIPTION, subscriberId);
        }

        private boolean hasLock() {
            return support.hasLock(SUBSCRIPTION, subscriberId);
        }

        private void refresh() {
            scheduledRefresh.get().run();
        }
    }

}
