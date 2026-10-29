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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What a tell writes in the record of what the listeners were told, and which change it works out next, when a listener
 * throws an {@link Error} or the lease changes under the tell. Every round here is run by the test, on the test thread,
 * so the tell a round hands over runs inside that round.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy whose tell met an Error or a lease that changed")
@Timeout(30)
class MongoLeaseTellRecordAfterErrorTest {

    private static final String DATABASE = "mongoleasetellrecordaftererror";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String A = "subscription-a";
    private static final String NODE = "the-node";
    private static final String RIVAL = "the-rival";
    private static final int CALLS_BEFORE_THE_LISTENER_GIVES_UP = 5;

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
    void a_grant_a_round_told_to_a_listener_that_threw_an_error_is_told_again_in_the_next_round_when_the_clock_threw_before_the_tell_ended() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        ClockThatThrowsOnceWhenArmed clock = new ClockThatThrowsOnceWhenArmed();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round, clock);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = RecordingListener.throwingAnErrorOnTheFirstGrantAfter(clock::arm);
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            round.get().run();

            assertThat(clock.thrown).as("the clock threw when the tell read it after the listener").isTrue();
            assertThat(listener.calls).as("what the listener heard in the round that took the lease").containsExactly("granted " + A);

            round.get().run();

            // The listener may not have acted on the grant it threw on, and the lease is still held
            assertThat(listener.calls).as("what the listener heard in the next round").containsExactly("granted " + A, "granted " + A);
            assertThat(node.hasLock(A, NODE)).isTrue();

            round.get().run();

            assertThat(listener.calls).as("a grant that did not throw is told once").containsExactly("granted " + A, "granted " + A);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_grant_a_register_told_to_a_listener_that_threw_an_error_is_told_again_in_the_next_round_when_the_clock_threw_before_the_tell_ended() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        ClockThatThrowsOnceWhenArmed clock = new ClockThatThrowsOnceWhenArmed();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round, clock);
        RecordingListener listener = RecordingListener.throwingAnErrorOnTheFirstGrantAfter(clock::arm);
        node.addListener(listener);
        try {
            Throwable thrown = catchThrowable(() -> node.registerCompetingConsumer(locks, A, NODE));

            assertThat(thrown).as("what the register threw").isInstanceOf(LinkageError.class);
            assertThat(listener.calls).as("what the listener heard in the register").containsExactly("granted " + A);
            assertThat(node.hasLock(A, NODE)).as("the register still took the lease").isTrue();

            round.get().run();

            assertThat(listener.calls).as("what the listener heard in the next round").containsExactly("granted " + A, "granted " + A);

            round.get().run();

            assertThat(listener.calls).as("a grant that did not throw is told once").containsExactly("granted " + A, "granted " + A);
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_prohibition_comes_before_the_grant_of_a_new_lease_when_the_lease_changes_as_the_tell_reads_its_status() throws ReflectiveOperationException {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round, System::nanoTime);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        // The lease is lost and another one taken as the first status read of a tell begins
        ConsumersThatRunOnceInATell consumers = new ConsumersThatRunOnceInATell(() -> {
            node.releaseCompetingConsumer(locks, A, NODE);
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).as("the node took the lease again").isTrue();
        });
        replaceTheConsumersOf(node, consumers);
        RecordingListener listener = RecordingListener.recordingEverything();
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            round.get().run();

            assertThat(consumers.fired).as("the lease changed during the tell").isTrue();
            // The lease the round took was lost before anyone was told it was granted, and a caller may have learned of it from hasLock
            assertThat(listener.calls).as("what the listener heard").containsExactly("prohibited " + A, "granted " + A);
            assertThat(node.hasLock(A, NODE)).isTrue();
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_that_registers_again_and_throws_an_error_on_a_grant_is_told_once_in_each_round() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round, System::nanoTime);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = RecordingListener.registeringAgainAndThrowingAnError(
                () -> node.registerCompetingConsumer(locks, A, NODE), CALLS_BEFORE_THE_LISTENER_GIVES_UP);
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            round.get().run();

            assertThat(listener.calls).as("what the listener heard in the round that took the lease").containsExactly("granted " + A);

            round.get().run();

            assertThat(listener.calls).as("what the listener heard in the next round").containsExactly("granted " + A, "granted " + A);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_grant_is_told_with_the_interrupt_that_reached_the_tell_after_it_cleared_its_flag_and_before_it_checked_that_the_strategy_runs() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        ClockThatInterruptsTheFirstTell clock = new ClockThatInterruptsTheFirstTell();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round, clock);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        List<Boolean> interruptedOnAGrant = new CopyOnWriteArrayList<>();
        RecordingListener listener = new RecordingListener(() -> interruptedOnAGrant.add(Thread.currentThread().isInterrupted()));
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            round.get().run();

            assertThat(clock.interrupted).as("the clock interrupted the tell as it read it").isTrue();
            assertThat(listener.calls).as("what the listener heard").containsExactly("granted " + A);
            // Only close() sends an interrupt to a tell, and the listener has to see it
            assertThat(interruptedOnAGrant).as("whether the thread was interrupted when the listener heard of the grant").containsExactly(true);
        } finally {
            Thread.interrupted();
            node.shutdown();
            rival.shutdown();
        }
    }

    private MongoLeaseCompetingConsumerStrategySupport nodeWhoseRoundsTheTestRuns(AtomicReference<Runnable> round, LongSupplier clock) {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh())), clock)
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
    }

    private static MongoLeaseCompetingConsumerStrategySupport rivalThatNeverRefreshes() {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
    }

    private static void replaceTheConsumersOf(MongoLeaseCompetingConsumerStrategySupport node, ConsumersThatRunOnceInATell consumers) throws ReflectiveOperationException {
        Field field = MongoLeaseCompetingConsumerStrategySupport.class.getDeclaredField("competingConsumers");
        field.setAccessible(true);
        field.set(node, consumers);
    }

    /**
     * Reads {@code System.nanoTime()}, and throws a {@link LinkageError} on the first read after it was armed.
     */
    private static final class ClockThatThrowsOnceWhenArmed implements LongSupplier {
        private final AtomicBoolean armed = new AtomicBoolean();
        private volatile boolean thrown;

        void arm() {
            armed.set(true);
        }

        @Override
        public long getAsLong() {
            if (armed.compareAndSet(true, false)) {
                thrown = true;
                // Thrown deliberately. Nothing here runs out of memory.
                throw new LinkageError("reading the clock fails");
            }
            return System.nanoTime();
        }
    }

    /**
     * Reads {@code System.nanoTime()}, and interrupts the thread that makes the first read from inside a tell. That is
     * an interrupt from {@code close()} that arrives after the tell has cleared its flag.
     */
    private static final class ClockThatInterruptsTheFirstTell implements LongSupplier {
        private final AtomicBoolean claimed = new AtomicBoolean();
        private volatile boolean interrupted;

        @Override
        public long getAsLong() {
            if (!claimed.get() && isReadByATell() && claimed.compareAndSet(false, true)) {
                Thread.currentThread().interrupt();
                interrupted = true;
            }
            return System.nanoTime();
        }
    }

    private static boolean isReadByATell() {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                frame.getClassName().equals(MongoLeaseCompetingConsumerStrategySupport.class.getName())
                        && frame.getMethodName().equals("tellUntilNothingIsDue")));
    }

    /**
     * The consumers of a strategy, which runs {@code action} once, on the first read of a consumer made from inside
     * {@code tellUntilNothingIsDue}, before that read answers.
     */
    private static final class ConsumersThatRunOnceInATell extends ConcurrentHashMap<Object, Object> {
        private final Runnable action;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private volatile boolean fired;

        private ConsumersThatRunOnceInATell(Runnable action) {
            this.action = action;
        }

        @Override
        public Object get(Object key) {
            if (!claimed.get() && isReadByATell() && claimed.compareAndSet(false, true)) {
                action.run();
                fired = true;
            }
            return super.get(key);
        }
    }

    /**
     * Records each call it is told of as {@code granted} or {@code prohibited} and the subscription id. A grant can
     * make it throw an {@link Error}, after it has run an action.
     */
    private static final class RecordingListener implements CompetingConsumerListener {
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final Runnable onAGrant;

        private RecordingListener(Runnable onAGrant) {
            this.onAGrant = onAGrant;
        }

        static RecordingListener recordingEverything() {
            return new RecordingListener(() -> {
            });
        }

        static RecordingListener throwingAnErrorOnTheFirstGrantAfter(Runnable beforeThrowing) {
            AtomicBoolean thrown = new AtomicBoolean();
            return new RecordingListener(() -> {
                if (thrown.compareAndSet(false, true)) {
                    beforeThrowing.run();
                    throw new AssertionError("failed on the first grant");
                }
            });
        }

        static RecordingListener registeringAgainAndThrowingAnError(Runnable register, int callsToDoItFor) {
            AtomicReference<RecordingListener> self = new AtomicReference<>();
            RecordingListener listener = new RecordingListener(() -> {
                if (self.get().calls.size() <= callsToDoItFor) {
                    register.run();
                    throw new AssertionError("failed on grant " + self.get().calls.size());
                }
            });
            self.set(listener);
            return listener;
        }

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            calls.add("granted " + subscriptionId);
            onAGrant.run();
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            calls.add("prohibited " + subscriptionId);
        }
    }
}
