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
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What happens next when a listener throws while a register, release or unregister tells the listeners on its own
 * thread. Every round here is run by the test, on the test thread.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy after an inline tell met a listener that threw")
@Timeout(30)
class MongoLeaseInlineTellFollowUpTest {

    private static final String DATABASE = "mongoleaseinlinetellfollowup";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);
    private static final String A = "subscription-a";
    private static final String NODE = "the-node";
    private static final String RIVAL = "the-rival";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;

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
    void startWithNoLocks() {
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    @Test
    void a_grant_a_register_told_to_a_listener_that_threw_an_error_is_told_again_in_the_next_round() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        RecordingListener listener = RecordingListener.throwingAnErrorOnTheFirst("granted");
        node.addListener(listener);
        try {
            Throwable thrown = catchThrowable(() -> node.registerCompetingConsumer(locks, A, NODE));
            assertThat(thrown).as("what the register threw").isInstanceOf(AssertionError.class);
            assertThat(node.hasLock(A, NODE)).as("the register still took the lease").isTrue();

            round.get().run();

            // The listener may not have acted on the grant it threw on, and the lease is still held, so this round tells it once more
            assertThat(listener.calls).containsExactly("granted " + A, "granted " + A);
            assertThat(node.hasLock(A, NODE)).isTrue();

            round.get().run();

            assertThat(listener.calls).as("a grant that did not throw is told once").containsExactly("granted " + A, "granted " + A);
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_prohibition_a_release_told_to_a_listener_that_threw_an_error_is_told_again_in_the_next_round() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        RecordingListener listener = RecordingListener.throwingAnErrorOnTheFirst("prohibited");
        node.addListener(listener);
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();

            Throwable thrown = catchThrowable(() -> node.releaseCompetingConsumer(locks, A, NODE));
            assertThat(thrown).as("what the release threw").isInstanceOf(AssertionError.class);

            // The round after a release stands the consumer down, so nothing takes the lease back in it
            round.get().run();

            assertThat(listener.calls).containsExactly("granted " + A, "prohibited " + A, "prohibited " + A);
            assertThat(node.hasLock(A, NODE)).isFalse();
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_listener_that_shuts_the_strategy_down_on_a_prohibition_keeps_the_grant_that_follows_it_from_every_listener() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        // Wins the lease back from inside the prohibition, so the grant is due as soon as the prohibition has been told
        RecordingListener quitter = RecordingListener.runningOnTheLoss(() -> {
            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            round.get().run();
            node.shutdown();
        });
        RecordingListener bystander = RecordingListener.runningOnTheLoss(() -> {
        });
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();
            node.addListener(quitter);
            node.addListener(bystander);
            takeOverLease(rival);

            round.get().run();

            assertThat(node.hasLock(A, NODE)).as("the nested round won " + A + " back before the shutdown").isTrue();
            assertThat(quitter.calls).as("what the listener that shut down heard").containsExactly("prohibited " + A);
            assertThat(bystander.calls).as("what the other listener heard").containsExactly("prohibited " + A);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void an_unregister_whose_prohibition_met_a_listener_that_threw_forgets_the_consumer_right_away() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        RecordingListener listener = RecordingListener.throwingARuntimeExceptionOnTheFirst("prohibited");
        node.addListener(listener);
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();

            Throwable thrown = catchThrowable(() -> node.unregisterCompetingConsumer(locks, A, NODE));
            assertThat(thrown).as("what the unregister threw").isInstanceOf(IllegalStateException.class);
            assertThat(listener.calls).containsExactly("granted " + A, "prohibited " + A);

            // A lease for the same subscriber, which a round removes for a consumer that is still being unregistered
            locks.updateOne(eq("_id", A), set("subscriberId", NODE));
            round.get().run();

            assertThat(locks.countDocuments(and(eq("_id", A), eq("subscriberId", NODE))))
                    .as("the round left the lease alone, since the consumer had been forgotten")
                    .isEqualTo(1);
        } finally {
            node.shutdown();
        }
    }

    private MongoLeaseCompetingConsumerStrategySupport nodeWhoseRoundsTheTestRuns(AtomicReference<Runnable> round) {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh())))
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
    }

    private static MongoLeaseCompetingConsumerStrategySupport rivalThatNeverRefreshes() {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
    }

    private void takeOverLease(MongoLeaseCompetingConsumerStrategySupport rival) {
        locks.updateOne(eq("_id", A), set("expiresAt", Instant.now().minus(LONG_ENOUGH)));
        assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).as("the rival took over " + A).isTrue();
    }

    /**
     * Records each call it is told of as {@code granted} or {@code prohibited} and the subscription id. Throws on the
     * first call of the kind it was made for, or runs an action on every prohibition.
     */
    private static final class RecordingListener implements CompetingConsumerListener {
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final String kindToFailOn;
        private final Runnable failure;
        private final Runnable onTheLoss;
        private final AtomicBoolean failed = new AtomicBoolean();

        private RecordingListener(String kindToFailOn, Runnable failure, Runnable onTheLoss) {
            this.kindToFailOn = kindToFailOn;
            this.failure = failure;
            this.onTheLoss = onTheLoss;
        }

        static RecordingListener throwingAnErrorOnTheFirst(String kind) {
            return new RecordingListener(kind, () -> {
                throw new AssertionError("failed on the first " + kind);
            }, () -> {
            });
        }

        static RecordingListener throwingARuntimeExceptionOnTheFirst(String kind) {
            return new RecordingListener(kind, () -> {
                throw new IllegalStateException("failed on the first " + kind);
            }, () -> {
            });
        }

        static RecordingListener runningOnTheLoss(Runnable onTheLoss) {
            return new RecordingListener("", () -> {
            }, onTheLoss);
        }

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            record("granted", subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            record("prohibited", subscriptionId);
            onTheLoss.run();
        }

        private void record(String kind, String subscriptionId) {
            calls.add(kind + " " + subscriptionId);
            if (kind.equals(kindToFailOn) && failed.compareAndSet(false, true)) {
                failure.run();
            }
        }
    }
}
