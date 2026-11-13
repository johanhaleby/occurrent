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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A pause of a {@link NativeMongoSubscriptionModel} subscription waits for a running action for one second at most, so
 * an action that is stuck outlives the pause. A resumed subscription must not call its action while that call still
 * runs, however many times it was paused and resumed meanwhile, and a shutdown must end the wait of a resumed run.
 */
@Testcontainers
@Timeout(30)
@DisplayName("NativeMongoSubscriptionModelStuckAction")
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelStuckActionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CountDownLatch releaseFirstCallOfE1 = new CountDownLatch(1);
    private final AtomicBoolean firstCallOfE1 = new AtomicBoolean(true);
    private final AtomicReference<Thread> stuckThread = new AtomicReference<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger mostInFlight = new AtomicInteger();
    private final CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Thread> executorThreads = new CopyOnWriteArrayList<>();
    private final NameDefined e1 = nameDefined("name1");
    private final NameDefined e2 = nameDefined("name2");
    private MongoClient mongoClient;
    private MongoCollection<Document> eventCollection;
    private MongoDatabase database;
    private MongoEventStore mongoEventStore;
    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stuckAction");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        subscriptionExecutor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            executorThreads.add(thread);
            return thread;
        });
        subscriptionModel = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor, NativeMongoSubscriptionModelConfig.withConfig());
    }

    @AfterEach
    void shutdown() {
        // The stuck call ignores interrupts, so the model's shutdown would otherwise wait for it
        releaseFirstCallOfE1.countDown();
        subscriptionModel.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_resumed_subscription_calls_its_action_only_once_the_call_that_outlived_the_pause_has_returned() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));

        // When: paused, which returns while that call still runs, then resumed, and a second event is written
        subscriptionModel.pauseSubscription(subscriptionId);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(Duration.ofSeconds(3), () -> calls.size() > 1);

        // Then: nothing else was called while the first call was stuck
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> calls.contains("E2 returned"));
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned", "E1 started", "E1 returned", "E2 started", "E2 returned");
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_second_pause_and_resume_while_the_resumed_run_waits_still_holds_back_the_action_until_the_stuck_call_has_returned() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

        // When: paused and resumed again while the first resumed run waits for the stuck call, and a second event is written
        subscriptionModel.pauseSubscription(subscriptionId);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(Duration.ofSeconds(3), () -> calls.size() > 1);

        // Then: nothing else was called while the first call was stuck
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> calls.contains("E2 returned"));
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned", "E1 started", "E1 returned", "E2 started", "E2 returned");
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_shutdown_while_a_resumed_run_waits_for_the_stuck_call_ends_the_wait_of_that_run() {
        // Given: the first call for E1 is stuck, and a resumed run waits for it
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        List<Thread> waiting = executorThreads.stream().filter(thread -> thread != stuckThread.get() && thread.isAlive()).toList();
        assertThat(waiting).as("executor threads other than the one running the stuck call").isNotEmpty();

        // When
        long startedShuttingDown = System.nanoTime();
        subscriptionModel.shutdown();
        Duration shuttingDown = Duration.ofNanos(System.nanoTime() - startedShuttingDown);

        // Then: the model gave up waiting for the stuck call and interrupted the run, which stopped waiting
        assertThat(shuttingDown).as("time to shut down while a call is stuck").isLessThan(Duration.ofSeconds(8));
        waitAtMostFor(FIVE_SECONDS, () -> waiting.stream().noneMatch(Thread::isAlive));
        assertThat(waiting).as("threads still waiting after the shutdown").noneMatch(Thread::isAlive);
        assertThat(stuckThread.get().isAlive()).as("thread of the stuck call, which ignores interrupts").isTrue();
    }

    private void action(CloudEvent cloudEvent) {
        String name = cloudEvent.getId().equals(e1.eventId()) ? "E1" : "E2";
        mostInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        calls.add(name + " started");
        try {
            if (name.equals("E1") && firstCallOfE1.getAndSet(false)) {
                stuckThread.set(Thread.currentThread());
                awaitUninterruptibly(releaseFirstCallOfE1);
            }
        } finally {
            calls.add(name + " returned");
            inFlight.decrementAndGet();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitAtMostFor(Duration timeout, Callable<Boolean> condition) {
        try {
            await().atMost(timeout).until(condition);
        } catch (ConditionTimeoutException ignored) {
            // The assertions that follow fail with what the model did instead
        }
    }

    private static NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
