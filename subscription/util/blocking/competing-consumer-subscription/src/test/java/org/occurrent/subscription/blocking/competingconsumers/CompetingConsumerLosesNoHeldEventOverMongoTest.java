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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerLosesNoHeldEventTest.FenceStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * An event that waits for the lease over the MongoDB subscription models is delivered once the lease is back, or once
 * this model pauses, stops or starts the subscription, and the subscription goes on delivering after it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerLosesNoHeldEventOverMongoTest {

    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration EVENTUALLY = Duration.ofSeconds(10);
    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration HELD_FOR = Duration.ofMillis(500);

    private final FenceStrategy strategy = new FenceStrategy();
    private final CountDownLatch inFirst = new CountDownLatch(1);
    private final CountDownLatch releaseFirst = new CountDownLatch(1);
    // Holds the catch-up in h3 when set
    private volatile @Nullable CountDownLatch releaseH3;
    private final List<String> received = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private @Nullable CompetingConsumerSubscriptionModel model;
    private @Nullable SubscriptionModel catchUp;
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    @BeforeEach
    void connect() {
        logged.start();
        modelLog.addAppender(logged);
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(client, database);
        template.getDb().drop();
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
    }

    @AfterEach
    void shutdown() {
        releaseFirst.countDown();
        @Nullable CountDownLatch h3 = releaseH3;
        if (h3 != null) {
            h3.countDown();
        }
        if (model != null) {
            model.shutdown();
        }
        client.close();
        modelLog.detachAppender(logged);
        modelLog.setLevel(null);
    }

    @Test
    void the_catch_up_of_a_subscription_whose_lease_moves_to_another_node_and_back_while_an_event_waits_delivers_every_event() throws Exception {
        Subscription subscription = catchUpWhileAnEventWaitsForTheLease(this::asTheStarterMakesIt);

        theLeaseMovesToAnotherNodeAndBack();

        theCatchUpAndWhatFollowsIsDelivered(subscription);
    }

    // The grant comes while the catch-up still runs, which the wrapped model cannot resume then
    @Test
    void the_catch_up_of_a_subscription_whose_lease_moves_to_another_node_and_back_while_an_event_waits_is_paused_once_the_lease_moves_away_again() throws Exception {
        Subscription subscription = catchUpWhileAnEventWaitsForTheLease(this::asTheStarterMakesIt);
        theLeaseMovesToAnotherNodeAndBack();
        theCatchUpAndWhatFollowsIsDelivered(subscription);

        strategy.transfer("X", "node", "other-node");

        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(requireNonNull(catchUp).isPaused("X")).as("[X paused in the wrapped model once this node lost its lease again]").isTrue());
    }

    // A stream catch-up applies a pause asked for during its replay once the replay has ended, which h3 holds off
    @Test
    void the_stream_catch_up_of_a_subscription_whose_lease_moves_to_another_node_and_back_while_an_event_waits_delivers_every_event() throws Exception {
        CountDownLatch h3 = new CountDownLatch(1);
        releaseH3 = h3;
        Subscription subscription = catchUpWhileAnEventWaitsForTheLease(durable -> new StreamCatchupSubscriptionModel(durable, eventStore, new CatchupSubscriptionModelConfig(100)));

        theLeaseMovesToAnotherNodeAndBack();
        await().pollDelay(HELD_FOR).atMost(HELD_FOR.multipliedBy(2)).until(() -> true);
        h3.countDown();

        theCatchUpAndWhatFollowsIsDelivered(subscription);
    }

    @Test
    void the_catch_up_of_a_subscription_that_this_model_stops_and_starts_while_an_event_waits_delivers_every_event() throws Exception {
        Subscription subscription = catchUpWhileAnEventWaitsForTheLease(this::asTheStarterMakesIt);

        requireNonNull(model).stop();
        strategy.fenced = false;
        model.start(true);

        theCatchUpAndWhatFollowsIsDelivered(subscription);
    }

    @Test
    void a_change_stream_that_does_not_retry_goes_on_delivering_after_an_event_waited_while_another_thread_waited_for_a_lock_its_thread_holds() throws Exception {
        Object applicationLock = new Object();
        SpringMongoSubscriptionModel spring = new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING).retryStrategy(RetryStrategy.none()));
        model = new CompetingConsumerSubscriptionModel(new OneEventAtATime(spring, applicationLock), strategy);
        model.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> {
            strategy.deliveringThread = Thread.currentThread();
            received.add(e.getId());
        }).waitUntilStarted();
        write("e0", 1);
        await().atMost(EVENTUALLY).until(() -> received.contains("e0"));

        strategy.fenced = true;
        write("e1", 2);
        assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease").isTrue();
        Thread waiter = Thread.ofPlatform().daemon().start(() -> {
            synchronized (applicationLock) {
                applicationLock.notifyAll();
            }
        });
        await().atMost(EVENTUALLY).until(() -> waiter.getState() == Thread.State.BLOCKED || !waiter.isAlive());
        await().during(HELD_FOR).atMost(HELD_FOR.multipliedBy(2)).untilAsserted(() ->
                assertThat(received).as("events received without the lease").containsExactly("e0"));
        strategy.fenced = false;
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events received once the lease was back]").contains("e0", "e1"));
        write("e2", 3);

        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events received after the event that waited]").contains("e0", "e1", "e2"));
    }

    // Node A holds e1 while it doesn't hold the lease, and B takes the lease once A has paused the subscription
    @Test
    void a_held_event_is_withdrawn_and_not_delivered_when_the_subscription_is_paused() throws Exception {
        // Given
        NativeMongoCheckpointStorage checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        List<String> receivedOnB = new CopyOnWriteArrayList<>();
        CompetingConsumerSubscriptionModel a = overTheNativeModel(strategy, checkpoints);
        CompetingConsumerSubscriptionModel b = overTheNativeModel(strategy, checkpoints);
        try {
            subscribeOnA(a);
            strategy.fenced = true;
            write("e1", 2);
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();

            // When
            a.pauseSubscription("X");
            strategy.transfer("X", "node", "other-node");
            strategy.fenced = false;
            b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
            write("e2", 3);

            // Then
            await().atMost(EVENTUALLY).until(() -> receivedOnB.contains("e2"));
            assertThat(received).as("[events delivered on A, which paused the subscription while e1 waited there for the lease]").containsExactly("e0");
            assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
        } finally {
            b.shutdown();
            a.shutdown();
        }
    }

    // A late grant holds the lock of X on A while A loses the lease, so the prohibition is left to a try, and the lease
    // is back on A before that try runs
    @Test
    void a_held_event_is_delivered_once_when_the_lease_comes_back_before_the_try_for_its_prohibition() throws Exception {
        // Given
        NativeMongoCheckpointStorage checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        List<String> receivedOnB = new CopyOnWriteArrayList<>();
        LookThatWaits lookThatWaits = new LookThatWaits(strategy);
        CompetingConsumerSubscriptionModel a = overTheNativeModel(lookThatWaits, checkpoints);
        CompetingConsumerSubscriptionModel b = overTheNativeModel(strategy, checkpoints);
        try {
            subscribeOnA(a);
            Thread lateGrant = lookThatWaits.waitsIn(() -> a.onConsumeGranted("X", "node"));
            assertThat(lookThatWaits.entered.await(10, SECONDS)).as("the late grant holds the lock of X on A while it looks at the lease").isTrue();
            strategy.fenced = true;
            write("e1", 2);
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();
            strategy.transfer("X", "node", "other-node");
            strategy.fenced = false;
            b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
            await().atMost(EVENTUALLY).until(() -> receivedOnB.contains("e1"));

            // When
            strategy.transfer("X", "other-node", "node");
            lookThatWaits.open.countDown();
            lateGrant.join(EVENTUALLY.toMillis());
            write("e2", 3);

            // Then
            await().atMost(EVENTUALLY).until(() -> received.contains("e2"));
            assertThat(receivedOnB).as("[events delivered on B, which held the lease while e1 waited on A]").containsExactly("e1");
            assertThat(received).as("[events delivered on A, which resumed from the checkpoint B left once the lease was back]").containsExactly("e0", "e2");
        } finally {
            lookThatWaits.open.countDown();
            b.shutdown();
            a.shutdown();
        }
    }

    // Before shutdown() has begun, nothing on A acts on e1 once its wait ends, so A can only deliver it
    @Test
    void an_interrupted_wait_lets_the_event_through_and_warns() throws Exception {
        // Given
        NativeMongoCheckpointStorage checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        CompetingConsumerSubscriptionModel a = overTheNativeModel(strategy, checkpoints);
        try {
            subscribeOnA(a);
            strategy.fenced = true;
            write("e1", 2);
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();

            // When
            requireNonNull(strategy.deliveringThread).interrupt();

            // Then, with the interrupt flag set again once the action has run, which fails the checkpoint write after it
            await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events delivered on A, whose thread was interrupted while e1 waited]").containsExactly("e0", "e1"));
            assertThat(messages(Level.WARN, modelLog)).as("[warnings that A delivered e1 without the lease]")
                    .filteredOn(warning -> warning.contains("Delivering an event without the lease")).singleElement().asString()
                    .contains("since the thread was interrupted", "subscriberId=node", "subscriptionId=X");
        } finally {
            a.shutdown();
        }
    }

    // A shuts down while e1 waits there for the lease, and B takes the lease over from the checkpoint A left
    @Test
    void a_held_event_is_withdrawn_and_not_delivered_when_this_model_shuts_down() throws Exception {
        // Given
        NativeMongoCheckpointStorage checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        List<String> receivedOnB = new CopyOnWriteArrayList<>();
        CompetingConsumerSubscriptionModel a = overTheNativeModel(strategy, checkpoints);
        CompetingConsumerSubscriptionModel b = overTheNativeModel(strategy, checkpoints);
        try {
            subscribeOnA(a);
            strategy.fenced = true;
            write("e1", 2);
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();

            // When
            a.shutdown();
            strategy.transfer("X", "node", "other-node");
            strategy.fenced = false;
            b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
            write("e2", 3);

            // Then
            await().atMost(EVENTUALLY).until(() -> receivedOnB.contains("e2"));
            assertThat(received).as("[events delivered on A, which shut down while e1 waited there for the lease]").containsExactly("e0");
            assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
        } finally {
            b.shutdown();
            a.shutdown();
        }
    }

    // shutdown() of A has begun and waits in the lease strategy, before A shuts the wrapped model down, as the thread e1
    // waits on is interrupted
    @Test
    void a_held_event_whose_wait_is_interrupted_once_shutdown_has_begun_is_withdrawn() throws Exception {
        // Given
        modelLog.setLevel(Level.DEBUG);
        NativeMongoCheckpointStorage checkpoints = new NativeMongoCheckpointStorage(database(), "checkpoints-" + UUID.randomUUID());
        List<String> receivedOnB = new CopyOnWriteArrayList<>();
        ShutdownThatWaits shutdownThatWaits = new ShutdownThatWaits(strategy);
        CompetingConsumerSubscriptionModel a = overTheNativeModel(shutdownThatWaits, checkpoints);
        CompetingConsumerSubscriptionModel b = overTheNativeModel(strategy, checkpoints);
        try {
            subscribeOnA(a);
            strategy.fenced = true;
            write("e1", 2);
            assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("e1 waits for the lease on A").isTrue();
            Thread shutdownOfA = Thread.ofPlatform().daemon().start(a::shutdown);
            assertThat(shutdownThatWaits.entered.await(10, SECONDS)).as("the shutdown of A waits in the lease strategy").isTrue();

            // When
            requireNonNull(strategy.deliveringThread).interrupt();
            await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> received.contains("e1") || messages(Level.DEBUG, modelLog).stream().anyMatch(debug -> debug.contains("Withdrawing an event that waited for the lease")));
            shutdownThatWaits.open.countDown();
            shutdownOfA.join(EVENTUALLY.toMillis());
            strategy.transfer("X", "node", "other-node");
            strategy.fenced = false;
            b.subscribe("other-node", "X", null, StartAt.subscriptionModelDefault(), e -> receivedOnB.add(e.getId())).waitUntilStarted();
            write("e2", 3);

            // Then
            await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> receivedOnB.contains("e2"));
            assertThat(received).as("[events delivered on A, whose thread was interrupted while e1 waited and A shut down]").containsExactly("e0");
            assertThat(receivedOnB).as("[events delivered on B, which took the lease over from the checkpoint A left]").containsExactly("e1", "e2");
        } finally {
            shutdownThatWaits.open.countDown();
            b.shutdown();
            a.shutdown();
        }
    }

    private List<String> messages(Level level, Logger from) {
        synchronized (logged) {
            return new ArrayList<>(logged.list).stream().filter(e -> e.getLevel() == level && e.getLoggerName().equals(from.getName())).map(ILoggingEvent::getFormattedMessage).toList();
        }
    }

    private MongoDatabase database() {
        return client.getDatabase(template.getDb().getName());
    }

    // A node of its own, with its own change streams, that shares the lease strategy and the checkpoints
    private CompetingConsumerSubscriptionModel overTheNativeModel(CompetingConsumerStrategy leaseStrategy, NativeMongoCheckpointStorage checkpoints) {
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database(), "events", TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
        return new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(nativeModel, checkpoints), leaseStrategy);
    }

    // A holds the lease of X and has delivered e0, which stores a checkpoint
    private void subscribeOnA(CompetingConsumerSubscriptionModel a) {
        a.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> {
            strategy.deliveringThread = Thread.currentThread();
            received.add(e.getId());
        }).waitUntilStarted();
        write("e0", 1);
        await().atMost(EVENTUALLY).until(() -> received.contains("e0"));
    }

    private SubscriptionModel asTheStarterMakesIt(DurableSubscriptionModel durable) {
        return new CatchupSubscriptionModel(durable, eventStore);
    }

    private void theLeaseMovesToAnotherNodeAndBack() {
        strategy.transfer("X", "node", "other-node");
        strategy.fenced = false;
        strategy.transfer("X", "other-node", "node");
    }

    // The composition the Spring Boot starter makes, with the catch-up model made from the durable one. h1 runs while
    // the lease closes without anyone being told, as the MongoDB lease strategies close it, so h2 waits for it during
    // the catch-up.
    private Subscription catchUpWhileAnEventWaitsForTheLease(Function<DurableSubscriptionModel, SubscriptionModel> catchUpOf) throws InterruptedException {
        for (int i = 1; i <= 5; i++) {
            write("h" + i, i);
        }
        SpringMongoSubscriptionModel spring = new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING));
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        catchUp = catchUpOf.apply(new DurableSubscriptionModel(spring, storage));
        model = new CompetingConsumerSubscriptionModel(catchUp, strategy);
        Subscription subscription = model.subscribe("node", "X", null, StartAtTime.beginningOfTime(), e -> {
            if (e.getId().equals("h1")) {
                strategy.deliveringThread = Thread.currentThread();
                inFirst.countDown();
                try {
                    releaseFirst.await();
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }
            @Nullable CountDownLatch h3 = releaseH3;
            if (e.getId().equals("h3") && h3 != null) {
                try {
                    h3.await();
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(e.getId());
        });
        assertThat(inFirst.await(10, SECONDS)).as("h1 runs").isTrue();
        strategy.fenced = true;
        releaseFirst.countDown();
        assertThat(strategy.askedWithoutTheLease.await(10, SECONDS)).as("h2 waits for the lease").isTrue();
        return subscription;
    }

    private void theCatchUpAndWhatFollowsIsDelivered(Subscription subscription) {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events the catch-up delivered]").contains("h1", "h2", "h3", "h4", "h5"));
        write("live1", 10);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events delivered once the catch-up was done]").contains("h1", "h2", "h3", "h4", "h5", "live1"));
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(5))).as("[the subscription started]").isTrue();
    }

    private void write(String eventId, int second) {
        NameDefined event = new NameDefined(eventId, LocalDateTime.of(2026, 1, 1, 0, 0, second), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(eventId).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
    }

    // Holds the first look at the lease made on the thread that waitsIn starts until open is counted down, and passes
    // every call on to the fence
    private static final class LookThatWaits implements CompetingConsumerStrategy {
        private final FenceStrategy fence;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);
        private volatile @Nullable Thread waiting;

        private LookThatWaits(FenceStrategy fence) {
            this.fence = fence;
        }

        private Thread waitsIn(Runnable call) {
            Thread thread = Thread.ofPlatform().daemon().name("late-grant").unstarted(call);
            waiting = thread;
            thread.start();
            return thread;
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (Thread.currentThread() == waiting) {
                waiting = null;
                entered.countDown();
                try {
                    open.await(30, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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

    // Holds shutdown() until open is counted down, and passes every call on to the fence
    private static final class ShutdownThatWaits implements CompetingConsumerStrategy {
        private final FenceStrategy fence;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);

        private ShutdownThatWaits(FenceStrategy fence) {
            this.fence = fence;
        }

        @Override
        public void shutdown() {
            entered.countDown();
            try {
                open.await(30, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
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

    // Runs every action holding one lock, as an application that handles one event at a time can
    private record OneEventAtATime(SubscriptionModel wrapped, Object lock) implements SubscriptionModel {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribe(subscriptionId, filter, startAt, oneAtATime(action));
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribePaused(subscriptionId, filter, startAt, oneAtATime(action));
        }

        private Consumer<CloudEvent> oneAtATime(Consumer<CloudEvent> action) {
            return event -> {
                synchronized (lock) {
                    action.accept(event);
                }
            };
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
        public void pauseSubscription(String subscriptionId) {
            wrapped.pauseSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            wrapped.shutdown();
        }
    }
}
