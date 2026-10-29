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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
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
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A listener callback that is slow for one subscription, the way resuming a subscription whose change stream is still
 * opening is slow, must not delay what the listeners hear about another subscription. A subscription whose loss is
 * still being told hears of a later grant after the loss. A listener that throws an {@link Error} is told the current
 * answer again in the next round.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a MongoDB lease strategy telling its listeners about more than one subscription")
@Timeout(30)
class MongoLeaseNotificationPerSubscriptionTest {

    private static final String DATABASE = "mongoleasenotificationpersubscription";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final Duration REFRESH_PERIOD = Duration.ofMillis(100);
    /**
     * Long enough that a lock seeded this far in the past reads as expired against the database's own clock.
     */
    private static final Duration LONG_ENOUGH = Duration.ofSeconds(2);
    private static final Duration A_FEW_SECONDS = Duration.ofSeconds(5);
    private static final String A = "subscription-a";
    private static final String B = "subscription-b";
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
    void a_listener_blocked_on_the_grant_of_one_subscription_does_not_hold_up_the_loss_of_another() throws InterruptedException {
        MongoLeaseCompetingConsumerStrategySupport node = nodeRefreshingOnItsOwn();
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = new RecordingListener();
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            assertThat(node.registerCompetingConsumer(locks, B, NODE)).isTrue();
            listener.blockOn("granted " + A);

            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            assertThat(listener.busy.await(10, TimeUnit.SECONDS)).as("the listener is busy with the grant of " + A).isTrue();
            takeOverLease(rival, B);
            assertThat(eventually(() -> !node.hasLock(B, NODE))).as("a refresh round found " + B + " lost").isTrue();

            assertThat(listener.awaitCall("prohibited " + B))
                    .as("the loss of " + B + " reaches the listener while it is still busy with the grant of " + A + ", calls so far " + listener.calls)
                    .isTrue();
            assertThat(listener.blockedCallReturned).as("the grant of " + A + " is still blocked").isFalse();
        } finally {
            listener.unblock.countDown();
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_blocked_on_the_loss_of_one_subscription_does_not_hold_up_the_grant_of_another() throws InterruptedException {
        MongoLeaseCompetingConsumerStrategySupport node = nodeRefreshingOnItsOwn();
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = new RecordingListener();
        node.addListener(listener);
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();
            assertThat(rival.registerCompetingConsumer(locks, B, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, B, NODE)).isFalse();
            listener.blockOn("prohibited " + A);

            takeOverLease(rival, A);
            assertThat(listener.busy.await(10, TimeUnit.SECONDS)).as("the listener is busy with the loss of " + A).isTrue();
            rival.unregisterCompetingConsumer(locks, B, RIVAL);
            assertThat(eventually(() -> node.hasLock(B, NODE))).as("a refresh round granted " + B).isTrue();

            assertThat(listener.awaitCall("granted " + B))
                    .as("the grant of " + B + " reaches the listener while it is still busy with the loss of " + A + ", calls so far " + listener.calls)
                    .isTrue();
            assertThat(listener.blockedCallReturned).as("the loss of " + A + " is still blocked").isFalse();
        } finally {
            listener.unblock.countDown();
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_subscription_whose_loss_is_still_being_told_hears_of_a_later_grant_after_the_loss() throws InterruptedException {
        MongoLeaseCompetingConsumerStrategySupport node = nodeRefreshingOnItsOwn();
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = new RecordingListener();
        node.addListener(listener);
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();
            listener.blockOn("prohibited " + A);

            takeOverLease(rival, A);
            assertThat(listener.busy.await(10, TimeUnit.SECONDS)).as("the listener is busy with the loss of " + A).isTrue();
            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            assertThat(eventually(() -> node.hasLock(A, NODE))).as("the node won " + A + " back while the loss waited").isTrue();
            listener.unblock.countDown();

            listener.awaitCalls(2);
            assertThat(listener.calls).containsExactly("prohibited " + A, "granted " + A);
        } finally {
            listener.unblock.countDown();
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_on_a_grant_does_not_keep_the_other_listeners_from_hearing_of_it() {
        MongoLeaseCompetingConsumerStrategySupport node = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
        // Each one records and then throws, so whichever the set hands the grant to first keeps it from the other
        List<String> told = new CopyOnWriteArrayList<>();
        node.addListener(new ThrowingListener("first", told));
        node.addListener(new ThrowingListener("second", told));
        try {
            catchThrowable(() -> node.registerCompetingConsumer(locks, A, NODE));

            assertThat(told).containsExactlyInAnyOrder("first granted " + A, "second granted " + A);
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_an_error_on_a_grant_does_not_keep_the_other_listeners_from_hearing_of_it() {
        MongoLeaseCompetingConsumerStrategySupport node = rivalThatNeverRefreshes();
        List<String> told = new CopyOnWriteArrayList<>();
        node.addListener(ThrowingListener.throwingAnError("first", told));
        node.addListener(ThrowingListener.throwingAnError("second", told));
        try {
            Throwable thrown = catchThrowable(() -> node.registerCompetingConsumer(locks, A, NODE));

            assertThat(told).containsExactlyInAnyOrder("first granted " + A, "second granted " + A);
            assertThat(thrown).as("what the register threw").isInstanceOf(Error.class);
            assertThat(thrown.getSuppressed()).as("what the register threw, suppressed").hasSize(1);
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_checked_exception_a_listener_throws_on_a_grant_without_declaring_it_is_thrown_as_it_is() {
        MongoLeaseCompetingConsumerStrategySupport node = rivalThatNeverRefreshes();
        List<String> told = new CopyOnWriteArrayList<>();
        node.addListener(new CheckedExceptionListener(told));
        try {
            Throwable thrown = catchThrowable(() -> node.registerCompetingConsumer(locks, A, NODE));

            assertThat(told).containsExactly("granted " + A);
            assertThat(thrown).as("what the register threw").isInstanceOf(IOException.class);
        } finally {
            node.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_an_error_in_the_background_does_not_keep_the_other_listeners_from_hearing_of_it() throws InterruptedException {
        MongoLeaseCompetingConsumerStrategySupport node = nodeRefreshingOnItsOwn();
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        List<String> told = new CopyOnWriteArrayList<>();
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            node.addListener(ThrowingListener.throwingAnError("first", told));
            node.addListener(ThrowingListener.throwingAnError("second", told));

            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            assertThat(eventually(() -> told.contains("first granted " + A) && told.contains("second granted " + A)))
                    .as("both listeners heard of the grant of " + A + ", told so far " + told).isTrue();
            // Both throw an Error, so each round tells them the grant again until the lease is lost
            assertThat(told).containsOnly("first granted " + A, "second granted " + A);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_an_error_which_cannot_be_logged_does_not_keep_the_other_listeners_from_hearing_of_it() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh())))
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        List<String> told = new CopyOnWriteArrayList<>();
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            node.addListener(ThrowingListener.throwingAnErrorWhichCannotBeLogged("first", told));
            node.addListener(ThrowingListener.throwingAnErrorWhichCannotBeLogged("second", told));
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            // The refresh runs here and so does the grant it decides, since this ScheduledRefresh has no notifier
            Throwable thrown = catchThrowable(() -> round.get().run());

            assertThat(told).containsExactlyInAnyOrder("first granted " + A, "second granted " + A);
            assertThat(thrown).as("what the refresh threw").isNull();
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void an_error_a_listener_throws_in_the_background_is_logged_once_at_error_level() throws InterruptedException {
        AtomicReference<Runnable> round = new AtomicReference<>();
        NotifierThatThrowsWhenTold notifier = new NotifierThatThrowsWhenTold();
        MongoLeaseCompetingConsumerStrategySupport node = new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh()), notifier))
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        ErrorOnTheGrantListener listener = new ErrorOnTheGrantListener();
        Logged logged = new Logged();
        Logger supportLogger = (Logger) LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategySupport.class);
        Logger scheduledRefreshLogger = (Logger) LoggerFactory.getLogger(ScheduledRefresh.class);
        logged.start();
        supportLogger.addAppender(logged);
        scheduledRefreshLogger.addAppender(logged);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            node.addListener(listener);

            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            // Rounds are run here, since a round between the failing grant and the loss would tell the grant again and log it again
            round.get().run();
            notifier.awaitEveryTask();
            assertThat(listener.granted.await(10, TimeUnit.SECONDS)).as("the listener was told of the grant of " + A).isTrue();
            takeOverLease(rival, A);
            round.get().run();
            notifier.awaitEveryTask();
            assertThat(listener.prohibited.await(10, TimeUnit.SECONDS)).as("the listener was told of the loss of " + A).isTrue();

            assertThat(logged.failures()).singleElement().satisfies(failure -> {
                assertThat(failure.getLevel()).isEqualTo(Level.ERROR);
                assertThat(failure.getLoggerName()).isEqualTo(MongoLeaseCompetingConsumerStrategySupport.class.getName());
                assertThat(failure.getThrowableProxy().getClassName()).isEqualTo(AssertionError.class.getName());
            });
        } finally {
            supportLogger.detachAppender(logged);
            scheduledRefreshLogger.detachAppender(logged);
            logged.stop();
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_in_the_background_does_not_stop_later_callbacks_for_other_subscriptions() throws InterruptedException {
        MongoLeaseCompetingConsumerStrategySupport node = nodeRefreshingOnItsOwn();
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        RecordingListener listener = new RecordingListener();
        node.addListener(listener);
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(rival.registerCompetingConsumer(locks, B, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            assertThat(node.registerCompetingConsumer(locks, B, NODE)).isFalse();
            listener.throwOn("granted " + A);

            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            assertThat(listener.awaitCall("granted " + A)).as("the listener threw on the grant of " + A).isTrue();
            rival.unregisterCompetingConsumer(locks, B, RIVAL);

            assertThat(listener.awaitCall("granted " + B))
                    .as("the grant of " + B + " reaches the listener after it threw on " + A + ", calls so far " + listener.calls)
                    .isTrue();
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_listener_that_throws_an_error_on_a_grant_is_told_of_it_again_in_the_next_round() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        ListenerThatThrowsAnErrorOnce listener = ListenerThatThrowsAnErrorOnce.onTheFirst("granted");
        try {
            assertThat(rival.registerCompetingConsumer(locks, A, RIVAL)).isTrue();
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isFalse();
            node.addListener(listener);
            rival.unregisterCompetingConsumer(locks, A, RIVAL);

            round.get().run();
            OptionalLong tokenAfterTheFirstGrant = node.fencingToken(A);
            round.get().run();

            // The listener may not have acted on a grant it threw an Error on, and no other round decides the grant
            // again, so the next round tells it once more under the same lease
            assertThat(listener.calls).containsExactly("granted " + A, "granted " + A);
            assertThat(node.hasLock(A, NODE)).isTrue();
            assertThat(node.fencingToken(A)).isPresent().isEqualTo(tokenAfterTheFirstGrant);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    @Test
    void a_prohibition_an_error_cut_short_is_told_again_before_the_grant_of_a_new_lease() {
        AtomicReference<Runnable> round = new AtomicReference<>();
        MongoLeaseCompetingConsumerStrategySupport node = nodeWhoseRoundsTheTestRuns(round);
        MongoLeaseCompetingConsumerStrategySupport rival = rivalThatNeverRefreshes();
        ListenerThatThrowsAnErrorOnce interrupted = ListenerThatThrowsAnErrorOnce.onTheFirst("prohibited");
        ListenerThatThrowsAnErrorOnce other = ListenerThatThrowsAnErrorOnce.onTheFirst("");
        node.addListener(interrupted);
        node.addListener(other);
        try {
            assertThat(node.registerCompetingConsumer(locks, A, NODE)).isTrue();
            takeOverLease(rival, A);

            // No round runs between these two, so the second prohibition can only come from the new lease being compared with the first
            round.get().run();
            assertThat(other.calls).as("what the other listener heard of the loss").containsExactly("granted " + A, "prohibited " + A);
            rival.unregisterCompetingConsumer(locks, A, RIVAL);
            round.get().run();

            // The prohibition of the first lease is told again before the grant of the new one, since a listener that
            // threw an Error on it may still be running under the lease that was lost
            assertThat(interrupted.calls).containsExactly("granted " + A, "prohibited " + A, "prohibited " + A, "granted " + A);
        } finally {
            node.shutdown();
            rival.shutdown();
        }
    }

    private MongoLeaseCompetingConsumerStrategySupport nodeWhoseRoundsTheTestRuns(AtomicReference<Runnable> round) {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> round.set(scheduler.refresh())))
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
    }

    private MongoLeaseCompetingConsumerStrategySupport nodeRefreshingOnItsOwn() {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(), ScheduledRefresh.every(REFRESH_PERIOD))
                .scheduleRefresh(refreshOrAcquire -> () -> refreshOrAcquire.accept(locks));
    }

    private static MongoLeaseCompetingConsumerStrategySupport rivalThatNeverRefreshes() {
        return new MongoLeaseCompetingConsumerStrategySupport(LEASE, RetryStrategy.none(),
                new ScheduledRefresh((lease, scheduler) -> {
                }));
    }

    /**
     * Expires the node's lease and lets the rival take it. The node refreshes on its own schedule and can renew the
     * expired lease before the rival gets to it, so this tries again until the rival wins.
     */
    private void takeOverLease(MongoLeaseCompetingConsumerStrategySupport rival, String subscriptionId) {
        for (int attempt = 0; attempt < 50; attempt++) {
            locks.updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minus(LONG_ENOUGH)));
            if (rival.registerCompetingConsumer(locks, subscriptionId, RIVAL)) {
                return;
            }
        }
        throw new AssertionError("the rival never took over the lease for " + subscriptionId);
    }

    private static boolean eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    /**
     * Records every call from the first {@link #blockOn} or {@link #throwOn} on. Blocks on the call given to
     * {@link #blockOn} until {@link #unblock} is counted down, after counting down {@link #busy}, and throws on the
     * call given to {@link #throwOn}.
     */
    private static final class RecordingListener implements CompetingConsumerListener {
        private final AtomicBoolean recording = new AtomicBoolean();
        private final CountDownLatch busy = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private volatile String blockOn = "";
        private volatile String throwOn = "";
        private volatile boolean blockedCallReturned;

        void blockOn(String call) {
            blockOn = call;
            recording.set(true);
        }

        void throwOn(String call) {
            throwOn = call;
            recording.set(true);
        }

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            record("granted " + subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            record("prohibited " + subscriptionId);
        }

        private void record(String call) {
            if (!recording.get()) {
                return;
            }
            calls.add(call);
            if (call.equals(throwOn)) {
                throw new IllegalStateException("listener failed on " + call);
            }
            if (call.equals(blockOn)) {
                busy.countDown();
                try {
                    unblock.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    blockedCallReturned = true;
                }
            }
        }

        boolean awaitCall(String call) throws InterruptedException {
            long deadline = System.nanoTime() + A_FEW_SECONDS.toNanos();
            while (!calls.contains(call)) {
                if (System.nanoTime() >= deadline) {
                    return false;
                }
                Thread.sleep(10);
            }
            return true;
        }

        /**
         * Waits for {@code count} calls, and 200 milliseconds longer so that a call beyond them shows up as well. Gives
         * up after ten seconds, and the assertion after it then shows which calls are missing.
         */
        void awaitCalls(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (calls.size() < count && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(200);
        }
    }

    // The two lines that log a failing listener, the one for a listener the support class tells and the one for a tell that fails as a whole
    private static final class Logged extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            String message = event.getMessage();
            if (message.startsWith("Listener {} failed on") || message.startsWith("Telling the listeners of subscription {} failed")) {
                events.add(event);
            }
        }

        List<ILoggingEvent> failures() {
            return List.copyOf(events);
        }
    }

    /**
     * Throws an {@link AssertionError} on the grant, after counting down {@link #granted}, and counts down
     * {@link #prohibited} on the loss.
     */
    private static final class ErrorOnTheGrantListener implements CompetingConsumerListener {
        private final CountDownLatch granted = new CountDownLatch(1);
        private final CountDownLatch prohibited = new CountDownLatch(1);

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            granted.countDown();
            throw new AssertionError("failed on the grant of " + subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            prohibited.countDown();
        }
    }

    /**
     * Records each call it is told of, as {@code granted} or {@code prohibited} and the subscription id, and throws an
     * {@link AssertionError} on the first call of the kind given to {@link #onTheFirst}, none for an empty kind.
     */
    private static final class ListenerThatThrowsAnErrorOnce implements CompetingConsumerListener {
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final String kind;
        private final AtomicBoolean thrown = new AtomicBoolean();

        private ListenerThatThrowsAnErrorOnce(String kind) {
            this.kind = kind;
        }

        static ListenerThatThrowsAnErrorOnce onTheFirst(String kind) {
            return new ListenerThatThrowsAnErrorOnce(kind);
        }

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            record("granted", subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            record("prohibited", subscriptionId);
        }

        private void record(String call, String subscriptionId) {
            calls.add(call + " " + subscriptionId);
            if (call.equals(kind) && thrown.compareAndSet(false, true)) {
                throw new AssertionError("failed on the first " + call + " of " + subscriptionId);
            }
        }
    }

    /**
     * Records each call it is told of and then throws, an {@link IllegalStateException} or, made by
     * {@link #throwingAnError}, an {@link AssertionError}, or, made by {@link #throwingAnErrorWhichCannotBeLogged}, an
     * {@link ErrorThatThrowsWhenLogged}.
     */
    private record ThrowingListener(String name, List<String> told, Function<String, Throwable> failure) implements CompetingConsumerListener {

        private ThrowingListener(String name, List<String> told) {
            this(name, told, IllegalStateException::new);
        }

        private static ThrowingListener throwingAnError(String name, List<String> told) {
            return new ThrowingListener(name, told, message -> new AssertionError(message));
        }

        private static ThrowingListener throwingAnErrorWhichCannotBeLogged(String name, List<String> told) {
            return new ThrowingListener(name, told, message -> new ErrorThatThrowsWhenLogged());
        }

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            told.add(name + " granted " + subscriptionId);
            fail(name + " failed on the grant of " + subscriptionId);
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            told.add(name + " prohibited " + subscriptionId);
            fail(name + " failed on the loss of " + subscriptionId);
        }

        private void fail(String message) {
            Throwable thrown = failure.apply(message);
            if (thrown instanceof Error error) {
                throw error;
            }
            throw (RuntimeException) thrown;
        }
    }

    /**
     * Records each call it is told of and then throws an {@link IOException}, which neither callback declares, the
     * way code compiled from another JVM language can.
     */
    private record CheckedExceptionListener(List<String> told) implements CompetingConsumerListener {

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            told.add("granted " + subscriptionId);
            CheckedExceptionListener.<RuntimeException>throwUndeclared(new IOException("failed on the grant of " + subscriptionId));
        }

        @Override
        public void onConsumeProhibited(String subscriptionId, String subscriberId) {
            told.add("prohibited " + subscriptionId);
            CheckedExceptionListener.<RuntimeException>throwUndeclared(new IOException("failed on the loss of " + subscriptionId));
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void throwUndeclared(Throwable thrown) throws T {
            throw (T) thrown;
        }
    }
}
