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

package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RedeliveringSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerLosesNoHeldEventTest.FenceStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A prohibition left to a try for a subscription whose event waits for the lease over a wrapped model that redelivers.
 * Node A holds e1 while it doesn't hold the lease, and the event stays held until a pause, cancel or stop of the
 * subscription in the wrapped model has returned, which withdraws it. Node B, or A once it has the lease back,
 * delivers it once from the checkpoint A left.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class CompetingConsumerOwedProhibitionOverMongoTest {

    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration EVENTUALLY = Duration.ofSeconds(10);
    private static final String TRY_THREAD = "occurrent-competing-consumer-reconcile-";

    private final FenceStrategy strategy = new FenceStrategy();
    private final Leases leases = new Leases(strategy);
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> receivedOnB = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private SpringMongoEventStore eventStore;
    private NativeMongoCheckpointStorage checkpoints;
    private Hooked hooked;
    private CompetingConsumerSubscriptionModel a;
    private CompetingConsumerSubscriptionModel b;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        MongoTemplate template = new MongoTemplate(client, database);
        template.getDb().drop();
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        hooked = new Hooked(new DurableSubscriptionModel(nativeModel(), checkpoints));
        a = new CompetingConsumerSubscriptionModel(hooked, leases);
        b = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(nativeModel(), checkpoints), strategy);
    }

    @AfterEach
    void shutdown() {
        leases.triesFail = false;
        b.shutdown();
        a.shutdown();
        client.close();
    }

    @Test
    void a_held_event_stays_held_while_the_pause_for_the_loss_of_its_lease_fails_and_is_withdrawn_once_a_pause_returns() {
        // Given
        hooked.pausesToFail.set(2);
        e1WaitsOnA();

        // When
        strategy.transfer("X", "node", "other-node");
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> hooked.pausesReturned.get() == 1);
        bTakesTheLeaseOver();

        // Then
        assertThat(hooked.pausesReturnedAtEachWithdrawal).as("[pauses of X on A that had returned as A withdrew e1, after two that threw]").isNotEmpty().allMatch(pauses -> pauses >= 1);
        assertThat(received).as("[events delivered on A, which held e1 until a pause of X returned]").containsExactly("e0");
        assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
    }

    @Test
    void a_held_event_is_withdrawn_by_a_new_try_when_the_thread_of_the_try_that_pauses_it_is_interrupted() {
        // Given
        hooked.interruptsTheFirstPause.set(true);
        e1WaitsOnA();

        // When
        strategy.transfer("X", "node", "other-node");
        waitAtMostEventuallyUntil(() -> hooked.pausesReturned.get() > 0);

        // Then
        assertThat(hooked.pausesReturned.get()).as("pauses of X on A that returned after the first one was interrupted").isEqualTo(1);
        bTakesTheLeaseOver();
        assertThat(received).as("[events delivered on A, whose first pause of X was interrupted]").containsExactly("e0");
        assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
    }

    // The strategy tells the loss on the test thread
    @Test
    void a_prohibition_for_a_subscription_whose_event_waits_for_the_lease_is_acted_on_by_a_try_and_not_by_the_thread_that_tells_it() {
        // Given
        e1WaitsOnA();

        // When
        strategy.transfer("X", "node", "other-node");
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> hooked.pausesReturned.get() == 1);
        bTakesTheLeaseOver();

        // Then
        assertThat(hooked.pausingThreads).as("[threads that paused X on A]").isNotEmpty().allMatch(thread -> thread.startsWith(TRY_THREAD));
        assertThat(received).as("[events delivered on A, which paused X while e1 waited there]").containsExactly("e0");
        assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
    }

    @Test
    void a_pause_by_the_user_withdraws_a_held_event_whose_prohibition_a_failing_try_owes_and_the_resume_delivers_it_once() {
        // Given
        aTryOwesTheProhibitionWhileE1WaitsOnA();

        // When
        a.pauseSubscription("X");
        theLeaseIsBackOnA();
        a.resumeSubscription("X");
        write("e2", 3);

        // Then
        waitAtMostEventuallyUntil(() -> received.contains("e2"));
        assertThat(received).as("[events delivered on A, paused by the user while e1 waited there and resumed with the lease back]").containsExactly("e0", "e1", "e2");
        assertThat(hooked.pausesReturnedAtEachWithdrawal).as("[withdrawals of e1 on A]").hasSize(1);
    }

    @Test
    void a_stop_withdraws_a_held_event_whose_prohibition_a_failing_try_owes_and_the_start_delivers_it_once() {
        // Given
        aTryOwesTheProhibitionWhileE1WaitsOnA();

        // When
        a.stop();
        theLeaseIsBackOnA();
        a.start(true);
        write("e2", 3);

        // Then
        waitAtMostEventuallyUntil(() -> received.contains("e2"));
        assertThat(received).as("[events delivered on A, stopped while e1 waited there and started with the lease back]").containsExactly("e0", "e1", "e2");
        assertThat(hooked.pausesReturnedAtEachWithdrawal).as("[withdrawals of e1 on A]").hasSize(1);
    }

    // The cancel deletes the checkpoint, so the new subscription starts after e1
    @Test
    void a_cancel_withdraws_a_held_event_whose_prohibition_a_failing_try_owes_and_a_new_subscription_delivers_what_follows_once() {
        // Given
        aTryOwesTheProhibitionWhileE1WaitsOnA();

        // When
        a.cancelSubscription("X");
        theLeaseIsBackOnA();
        a.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();
        write("e2", 3);

        // Then
        waitAtMostEventuallyUntil(() -> received.contains("e2"));
        assertThat(received).as("[events delivered on A, which cancelled X while e1 waited there and subscribed again with the lease back]").containsExactly("e0", "e2");
        assertThat(hooked.pausesReturnedAtEachWithdrawal).as("[withdrawals of e1 on A]").hasSize(1);
    }

    // A grant whose look at the lease throws leaves X to a try. The strategy tells that try, on its own thread, that A
    // lost the lease, and B delivers e1, before the lease comes back to A without anyone being told and the try finds
    // it held.
    @Test
    void a_held_event_is_withdrawn_when_its_prohibition_comes_on_the_thread_of_a_try_that_then_finds_the_lease_back() throws Exception {
        // Given
        e1WaitsOnA();
        String checkpointAtE0 = String.valueOf(checkpoints.read("X"));
        CountDownLatch toldOnTheTry = new CountDownLatch(1);
        CountDownLatch leaseBack = new CountDownLatch(1);
        leases.onATry.set(() -> {
            strategy.transfer("X", "node", "other-node");
            toldOnTheTry.countDown();
            try {
                leaseBack.await(30, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        leases.throwOn = Thread.currentThread();
        a.onConsumeGranted("X", "node");
        assertThat(toldOnTheTry.await(10, SECONDS)).as("a try of A is told on its own thread that A lost the lease of X").isTrue();
        strategy.fenced = false;
        b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> receivedOnB.contains("e1") && !checkpointAtE0.equals(String.valueOf(checkpoints.read("X"))));

        // When
        strategy.releaseCompetingConsumer("X", "other-node");
        strategy.registerCompetingConsumer("X", "node");
        leaseBack.countDown();
        write("e2", 3);

        // Then
        waitAtMostEventuallyUntil(() -> received.contains("e2"));
        assertThat(received).as("[events delivered on A, whose try found the lease back after the loss it was told of]").containsExactly("e0", "e2");
        assertThat(receivedOnB).as("[events delivered on B, which held the lease while e1 waited on A]").containsExactly("e1");
    }

    // A holds the lease of X and has delivered e0, which stores a checkpoint, and e1 waits on A while the lease closes
    // without anyone being told
    private void e1WaitsOnA() {
        a.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> {
            strategy.deliveringThread = Thread.currentThread();
            received.add(e.getId());
        }).waitUntilStarted();
        write("e0", 1);
        await().atMost(EVENTUALLY).until(() -> received.contains("e0"));
        strategy.fenced = true;
        write("e1", 2);
        try {
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        // The look that found the lease closed comes before e1 counts as waiting, which the timed wait after it follows
        Thread delivering = requireNonNull(strategy.deliveringThread);
        await().atMost(EVENTUALLY).until(() -> delivering.getState() == Thread.State.TIMED_WAITING);
    }

    // The lease of X moves to B while e1 waits on A, and each try of A throws as it looks at the lease
    private void aTryOwesTheProhibitionWhileE1WaitsOnA() {
        leases.triesFail = true;
        e1WaitsOnA();
        strategy.transfer("X", "node", "other-node");
        await().atMost(EVENTUALLY).until(() -> leases.failedTries.get() > 0);
    }

    private void theLeaseIsBackOnA() {
        strategy.fenced = false;
        strategy.transfer("X", "other-node", "node");
    }

    private void bTakesTheLeaseOver() {
        strategy.fenced = false;
        b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
        write("e2", 3);
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> receivedOnB.contains("e2"));
    }

    // Returns once the condition holds or EVENTUALLY has passed, without failing, so the assertion after it fails instead
    private static void waitAtMostEventuallyUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + EVENTUALLY.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private MongoDatabase database() {
        return client.getDatabase(requireNonNull(new ConnectionString(mongo.getReplicaSetUrl() + ".events").getDatabase()));
    }

    private NativeMongoSubscriptionModel nativeModel() {
        return new NativeMongoSubscriptionModel(database(), "events", TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
    }

    private void write(String eventId, int second) {
        NameDefined event = new NameDefined(eventId, LocalDateTime.of(2026, 1, 1, 0, 0, second), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(eventId).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
    }

    // Passes every call on to the fence. hasLock throws once on the thread throwOn names, throws on each try of this
    // model while triesFail is set, and runs onATry on the first try that asks once it is set.
    private static final class Leases implements CompetingConsumerStrategy {
        private final FenceStrategy fence;
        private final AtomicInteger failedTries = new AtomicInteger();
        private final AtomicReference<@Nullable Runnable> onATry = new AtomicReference<>();
        private volatile @Nullable Thread throwOn;
        private volatile boolean triesFail;

        private Leases(FenceStrategy fence) {
            this.fence = fence;
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            Thread thread = Thread.currentThread();
            if (thread == throwOn) {
                throwOn = null;
                throw new IllegalStateException("hasLock failed");
            }
            if (thread.getName().startsWith(TRY_THREAD)) {
                if (triesFail) {
                    failedTries.incrementAndGet();
                    throw new IllegalStateException("hasLock failed on a try");
                }
                @Nullable Runnable once = onATry.getAndSet(null);
                if (once != null) {
                    once.run();
                }
            }
            return fence.hasLock(subscriptionId, subscriberId);
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return fence.registerCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            fence.unregisterCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            fence.releaseCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            fence.addListener(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            fence.removeListener(listenerConsumer);
        }
    }

    // Passes every call on to the durable model and declares that it redelivers, as that model does. Records the
    // pauses that returned at each event the action withdrew, and the thread of each pause. A pause throws without
    // passing on while pausesToFail is positive, and the first one throws on a thread it interrupts when
    // interruptsTheFirstPause is set.
    private static final class Hooked implements SubscriptionModel, SubscriptionModelWrapper, RedeliveringSubscriptions {
        private final SubscriptionModel wrapped;
        private final AtomicInteger pausesToFail = new AtomicInteger();
        private final AtomicBoolean interruptsTheFirstPause = new AtomicBoolean();
        private final AtomicInteger pausesReturned = new AtomicInteger();
        private final List<Integer> pausesReturnedAtEachWithdrawal = new CopyOnWriteArrayList<>();
        private final List<String> pausingThreads = new CopyOnWriteArrayList<>();

        private Hooked(SubscriptionModel wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribe(subscriptionId, filter, startAt, recordingWithdrawals(action));
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribePaused(subscriptionId, filter, startAt, recordingWithdrawals(action));
        }

        private Consumer<CloudEvent> recordingWithdrawals(Consumer<CloudEvent> action) {
            return event -> {
                try {
                    action.accept(event);
                } catch (AttemptNotMade withdrawn) {
                    pausesReturnedAtEachWithdrawal.add(pausesReturned.get());
                    throw withdrawn;
                }
            };
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pausingThreads.add(Thread.currentThread().getName());
            if (interruptsTheFirstPause.compareAndSet(true, false)) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("pause interrupted");
            }
            if (pausesToFail.getAndDecrement() > 0) {
                throw new IllegalStateException("pause failed");
            }
            wrapped.pauseSubscription(subscriptionId);
            pausesReturned.incrementAndGet();
        }

        @Override
        public boolean redeliversAnAttemptNotMade() {
            return true;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return wrapped;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            wrapped.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            wrapped.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            wrapped.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return wrapped.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return wrapped.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return wrapped.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return wrapped.resumeSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            wrapped.shutdown();
        }
    }
}
