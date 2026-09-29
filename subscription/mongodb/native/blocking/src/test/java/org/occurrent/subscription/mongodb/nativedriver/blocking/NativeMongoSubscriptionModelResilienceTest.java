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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.*;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.retry.RetryStrategy.exponentialBackoff;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Tests that {@link NativeMongoSubscriptionModel} survives the same class of MongoDB operational failures
 * (leader elections/failovers, transient network errors, change stream history lost) that
 * {@code SpringMongoSubscriptionModel} has been hardened against in production, and that it recovers gap-free.
 */
@Testcontainers
@Timeout(20)
public class NativeMongoSubscriptionModelResilienceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoEventStore mongoEventStore;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;
    private MongoDatabase database;
    private MongoCollection<Document> realEventCollection;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".resilience");
        this.mongoClient = MongoClients.create(connectionString);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        realEventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        subscriptionExecutor = Executors.newCachedThreadPool();
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    /**
     * Wraps {@code realEventCollection} so that the very first {@code watch(...)} call throws {@code exception},
     * simulating a change-stream disruption (a failover, a transient network error, history lost, ...), while every
     * subsequent call behaves exactly like the real collection. Mirrors how {@code SpringMongoSubscriptionModelTest}
     * injects the same class of failure.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsOnce(RuntimeException exception) {
        MongoCollection<Document> throwingCollection = mock(MongoCollection.class);
        when(throwingCollection.watch(anyList(), eq(Document.class)))
                .thenThrow(exception)
                .thenAnswer(invocation -> realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class));
        return throwingCollection;
    }

    /**
     * Wraps {@code realEventCollection} so that {@code watch(...)} succeeds (registering the subscription normally),
     * but the cursor throws {@code exception} as soon as it is iterated, simulating a change-stream disruption that
     * happens mid-subscription rather than at cursor-open time.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsDuringIteration(RuntimeException exception) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> throwingCursor = mock(MongoChangeStreamCursor.class);
        doThrow(exception).when(throwingCursor).forEachRemaining(any());
        ChangeStreamIterable<Document> throwingIterable = iterableOf(throwingCursor);
        MongoCollection<Document> throwingCollection = mock(MongoCollection.class);
        when(throwingCollection.watch(anyList(), eq(Document.class))).thenReturn(throwingIterable);
        return throwingCollection;
    }

    // Returns itself from the calls that set where the change stream opens, as the driver's iterable does, so the
    // model reaches the cursor rather than failing on a null iterable first
    @SuppressWarnings("unchecked")
    private static ChangeStreamIterable<Document> iterableOf(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
        ChangeStreamIterable<Document> iterable = mock(ChangeStreamIterable.class, RETURNS_SELF);
        when(iterable.cursor()).thenReturn(cursor);
        return iterable;
    }

    /**
     * Wraps {@code realEventCollection} so that the first change stream fails with a failover-like error as soon as
     * it is iterated, while every later {@code watch(...)} call behaves like the real collection and is counted in
     * {@code reopened}. {@code failed} counts down when the model closes the first one's cursor, which it does after it
     * has decided to restart and before the backoff starts. A test that acts right after that, with a backoff much
     * longer than the time it takes to act, acts during the backoff.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsDuringIterationOnce(CountDownLatch failed, AtomicInteger reopened) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> throwingCursor = mock(MongoChangeStreamCursor.class);
        doThrow(failoverLikeException()).when(throwingCursor).forEachRemaining(any());
        doAnswer(invocation -> {
            failed.countDown();
            return null;
        }).when(throwingCursor).close();
        ChangeStreamIterable<Document> throwingIterable = iterableOf(throwingCursor);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.watch(anyList(), eq(Document.class)))
                .thenReturn(throwingIterable)
                .thenAnswer(invocation -> {
                    reopened.incrementAndGet();
                    return realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class);
                });
        return collection;
    }

    /**
     * Wraps {@code realEventCollection} so that every {@code watch(...)} call throws what {@code failure} supplies
     * while {@code failing} is set, counting each one in {@code refused}, and behaves like the real collection
     * otherwise.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsWhile(AtomicBoolean failing, AtomicInteger refused, Supplier<RuntimeException> failure) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.watch(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            if (failing.get()) {
                refused.incrementAndGet();
                throw failure.get();
            }
            return realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class);
        });
        return collection;
    }

    /**
     * Wraps {@code realEventCollection} so that a change stream asked to open before the operation time in
     * {@code oldestKept} fails with lost history, the way MongoDB answers once its oplog no longer reaches back that
     * far. Every other change stream behaves like the real collection's.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionWhoseHistoryStartsAt(AtomicReference<BsonTimestamp> oldestKept) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.watch(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            ChangeStreamIterable<Document> iterable = spy(realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class));
            doAnswer(startAt -> {
                BsonTimestamp operationTime = startAt.getArgument(0);
                BsonTimestamp oldest = oldestKept.get();
                if (oldest != null && operationTime.compareTo(oldest) < 0) {
                    throw changeStreamHistoryLostException();
                }
                return startAt.callRealMethod();
            }).when(iterable).startAtOperationTime(any(BsonTimestamp.class));
            return iterable;
        });
        return collection;
    }

    /**
     * Wraps {@code database} so that {@code runCommand(..)} fails with a {@code MongoTimeoutException}, counted in
     * {@code refused}, while {@code unreachable} is set, and behaves like the real database otherwise, counting each
     * reply in {@code answered}.
     */
    private MongoDatabase databaseThatCannotBeReachedWhile(AtomicBoolean unreachable, AtomicInteger refused, AtomicInteger answered) {
        MongoDatabase unreachableDatabase = spy(database);
        doAnswer(invocation -> {
            if (unreachable.get()) {
                refused.incrementAndGet();
                throw new MongoTimeoutException("MongoDB cannot be reached");
            }
            Object reply = invocation.callRealMethod();
            answered.incrementAndGet();
            return reply;
        }).when(unreachableDatabase).runCommand(any(Bson.class));
        return unreachableDatabase;
    }

    /**
     * Waits until {@code condition} holds or {@code timeout} passes, whichever comes first. The assertions that follow
     * say whether the model did what was waited for.
     */
    private static void waitAtMostFor(Duration timeout, Callable<Boolean> condition) {
        try {
            await().atMost(timeout).until(condition);
        } catch (ConditionTimeoutException ignored) {
            // The assertions that follow fail with what the model did instead
        }
    }

    /**
     * Holds back MongoDB's reply to the first {@code runCommand(..)} on the database {@link #wrap(MongoDatabase)}
     * returns until {@link #release()}, so a test can act between MongoDB answering and the model getting the answer.
     * An interrupt ends the wait with a {@code MongoInterruptedException}, like the driver's own waits. Every later
     * call gets its reply straight away.
     */
    private static final class FirstReplyHeld {
        private final AtomicBoolean first = new AtomicBoolean(true);
        private final CountDownLatch answered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicInteger waiting = new AtomicInteger();

        MongoDatabase wrap(MongoDatabase database) {
            MongoDatabase held = spy(database);
            doAnswer(invocation -> {
                Object reply = invocation.callRealMethod();
                if (first.getAndSet(false)) {
                    waiting.incrementAndGet();
                    answered.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new MongoInterruptedException("Interrupted while the reply was held back", e);
                    } finally {
                        waiting.decrementAndGet();
                    }
                }
                return reply;
            }).when(held).runCommand(any(Bson.class));
            return held;
        }

        void awaitAnswered() {
            await().atMost(FIVE_SECONDS).until(() -> answered.getCount() == 0);
        }

        void release() {
            released.countDown();
        }

        int waiting() {
            return waiting.get();
        }
    }

    private static MongoCommandException changeStreamHistoryLostException() {
        return new MongoCommandException(changeStreamHistoryLostResponse(), new ServerAddress());
    }

    private static BsonDocument changeStreamHistoryLostResponse() {
        List<BsonElement> elements = new ArrayList<>();
        elements.add(new BsonElement("code", new BsonInt32(286)));
        elements.add(new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));
        return new BsonDocument(elements);
    }

    /**
     * Wraps a cursor whose change stream fails with lost history as soon as it is iterated. The model reads the
     * error code only after it has checked that the subscription wasn't closed, so {@code checkingErrorCode} counts
     * down once the model has decided the failure is its own, and the error code isn't returned until the model
     * closes the cursor.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatLosesHistoryUntilItsCursorIsClosed(CountDownLatch checkingErrorCode) {
        CountDownLatch closed = new CountDownLatch(1);
        MongoCommandException historyLost = new MongoCommandException(changeStreamHistoryLostResponse(), new ServerAddress()) {
            @Override
            public int getErrorCode() {
                checkingErrorCode.countDown();
                try {
                    closed.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.getErrorCode();
            }
        };
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock(MongoChangeStreamCursor.class);
        doThrow(historyLost).when(cursor).forEachRemaining(any());
        doAnswer(invocation -> {
            closed.countDown();
            return null;
        }).when(cursor).close();
        ChangeStreamIterable<Document> iterable = iterableOf(cursor);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.watch(anyList(), eq(Document.class))).thenReturn(iterable);
        return collection;
    }

    /**
     * Wraps {@code realEventCollection} so that the first change stream hands {@code batch} to the model one document
     * at a time and ignores being closed, as the driver's own cursor does for the documents it has already fetched.
     * Every later {@code watch(...)} call behaves like the real collection.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatHasFetched(List<ChangeStreamDocument<Document>> batch) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock(MongoChangeStreamCursor.class);
        doAnswer(invocation -> {
            java.util.function.Consumer<ChangeStreamDocument<Document>> consumer = invocation.getArgument(0);
            batch.forEach(consumer);
            return null;
        }).when(cursor).forEachRemaining(any());
        ChangeStreamIterable<Document> iterable = iterableOf(cursor);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.watch(anyList(), eq(Document.class)))
                .thenReturn(iterable)
                .thenAnswer(invocation -> realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class));
        return collection;
    }

    // Real change-stream documents for events written to the real collection, with real resume tokens
    private List<ChangeStreamDocument<Document>> changeStreamDocumentsFor(List<CloudEvent> events) {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = realEventCollection.watch(Document.class).cursor()) {
            mongoEventStore.write("fetched", 0, events);
            List<ChangeStreamDocument<Document>> documents = new ArrayList<>();
            await().atMost(FIVE_SECONDS).until(() -> {
                ChangeStreamDocument<Document> document = cursor.tryNext();
                if (document != null) {
                    documents.add(document);
                }
                return documents.size() == events.size();
            });
            return documents;
        }
    }

    /**
     * Simulates the class of error a driver surfaces during a replica-set primary election/failover: the change
     * stream cursor becomes unusable and a socket-level read fails until a new primary is elected.
     */
    private static MongoSocketReadException failoverLikeException() {
        return new MongoSocketReadException("expected: simulated primary election/failover", new ServerAddress(), new java.io.IOException("Connection reset by peer"));
    }

    @Nested
    @DisplayName("ChangeStreamHistoryLost")
    class ChangeStreamHistoryLostTest {

        @Test
        void a_subscription_made_while_the_model_is_stopped_whose_history_is_lost_before_start_gets_the_history_lost_handling() {
            // Given a subscription made while the model is stopped
            AtomicReference<BsonTimestamp> oldestKept = new AtomicReference<>();
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            operationTimeAtSubscribe.release();
            subscriptionModel = new NativeMongoSubscriptionModel(operationTimeAtSubscribe.wrap(database), collectionWhoseHistoryStartsAt(oldestKept), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            subscriptionModel.stop();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });
            operationTimeAtSubscribe.awaitAnswered();
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));

            // When the oplog no longer holds anything written before start()
            oldestKept.set(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
            subscriptionModel.start();

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId));
        }

        @Test
        void restarts_subscription_from_now_when_configured_to_do_so() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true).retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }

        @Test
        void removes_the_subscription_entry_when_history_is_lost_mid_subscription_and_not_configured_to_restart() {
            // Given: unlike collectionThatFailsOnce, the failure happens once the cursor is already registered in
            // runningSubscriptions, proving the entry isn't leaked once the subscription gives up.
            MongoCollection<Document> throwingCollection = collectionThatFailsDuringIteration(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();

            // When
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(2));

            // Then
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
                assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
            });
            // The strongest proof the entry isn't leaked: the id is free to reuse. If it were still in either map,
            // this would throw IllegalArgumentException("Subscription ... is already defined.").
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(2));
        }

        @Test
        void does_not_restart_subscription_when_not_configured_to_do_so() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            boolean started = subscriptionModel.subscribe(subscriptionId, state::add).waitUntilStarted(Duration.ofSeconds(2));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            assertThat(started).isFalse();
            await().atMost(Duration.ofSeconds(1)).during(Duration.ofMillis(500)).untilAsserted(() -> assertThat(state).isEmpty());
            assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        }

        @Test
        void a_pause_does_not_wait_for_a_subscription_whose_history_was_lost_just_before_it() throws InterruptedException {
            // Given a subscription the model is about to forget because its history was lost
            CountDownLatch checkingErrorCode = new CountDownLatch(1);
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatLosesHistoryUntilItsCursorIsClosed(checkingErrorCode), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });
            assertThat(checkingErrorCode.await(10, SECONDS)).isTrue();

            // When
            long pauseStarted = System.nanoTime();
            subscriptionModel.pauseSubscription(subscriptionId);
            Duration pausing = Duration.ofNanos(System.nanoTime() - pauseStarted);

            // Then
            assertThat(pausing).isLessThan(Duration.ofMillis(500));
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
        }

        @Test
        void start_returns_when_a_subscription_it_resumes_loses_its_history_before_start_waits_for_it() {
            // Given a dispatcher that runs a subscription on the thread that starts it, so the model has forgotten the
            // subscription whose history was lost by the time the resume inside start() returns
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsOnce(changeStreamHistoryLostException()), TimeRepresentation.RFC_3339_STRING, new CallerRunsExecutorService(),
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            subscriptionModel.stop();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });

            // When
            Throwable thrown = catchThrowable(subscriptionModel::start);

            // Then
            assertThat(thrown).isNull();
            assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        }
    }

    // Runs each task on the calling thread
    private static class CallerRunsExecutorService extends AbstractExecutorService {
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }
    }

    @Nested
    @DisplayName("Pause while delivering")
    class PauseWhileDeliveringTest {

        @Test
        void events_the_change_stream_fetched_before_a_pause_are_delivered_after_the_resume_rather_than_the_pause() throws InterruptedException {
            // Given three events fetched in one batch, and a handler still busy with the first one when the pause comes
            List<CloudEvent> events = new ArrayList<>();
            for (String eventId : List.of("e1", "e2", "e3")) {
                events.addAll(serialize(new NameDefined(eventId, LocalDateTime.now(), "name", eventId)));
            }
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatHasFetched(changeStreamDocumentsFor(events)), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            CountDownLatch handlingFirst = new CountDownLatch(1);
            CountDownLatch finishFirst = new CountDownLatch(1);
            AtomicBoolean pauseReturned = new AtomicBoolean();
            AtomicBoolean resumed = new AtomicBoolean();
            CopyOnWriteArrayList<String> deliveredAfterPause = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<String> deliveredAfterResume = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, cloudEvent -> {
                if (resumed.get()) {
                    deliveredAfterResume.add(cloudEvent.getId());
                } else if (pauseReturned.get()) {
                    deliveredAfterPause.add(cloudEvent.getId());
                } else if (handlingFirst.getCount() == 1) {
                    handlingFirst.countDown();
                    try {
                        finishFirst.await(10, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            assertThat(handlingFirst.await(10, SECONDS)).isTrue();

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            pauseReturned.set(true);
            finishFirst.countDown();

            // Then
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(deliveredAfterPause).isEmpty());
            resumed.set(true);
            subscriptionModel.resumeSubscription(subscriptionId);
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(deliveredAfterResume).containsExactly("e2", "e3"));
        }
    }

    @Nested
    @DisplayName("Failover / transient errors")
    class FailoverTest {

        @Test
        void restarts_and_resumes_gap_free_after_a_failover_like_error() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(failoverLikeException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }
    }

    @Nested
    @DisplayName("Pause/resume")
    class PauseResumeTest {

        @Test
        void resume_continues_from_last_delivered_position_instead_of_replaying_from_the_original_start_at() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            // StartAt.now() is the scenario the Spring "supplier" fix targets: reusing a stale position on resume
            // must not replay (or skip) events relative to where the subscription actually left off.
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            mongoEventStore.write("1", 1, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2")));
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            // Then: the event written while paused is delivered exactly once after resume, and the first event is
            // not redelivered.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));
            assertThat(state).extracting(CloudEvent::getType).containsExactly(NameDefined.class.getName(), NameWasChanged.class.getName());
        }

        @Test
        void resuming_at_a_given_position_reopens_the_change_stream_there() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
            // Captured so the model can be asked to reopen from here, a position earlier than the one it will have
            // tracked itself by the time it is paused below.
            Checkpoint afterFirstEvent = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(state.get(0));

            mongoEventStore.write("2", 0, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            mongoEventStore.write("3", 0, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name3")));
            subscriptionModel.resumeSubscription(subscriptionId, StartAt.checkpoint(afterFirstEvent)).waitUntilStarted(Duration.ofSeconds(10));

            // Then: the second and third events both arrive again, because the change stream reopened at the
            // explicit position rather than at the position the subscription itself had tracked (which was already
            // past the second event and would have delivered only the third).
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(4));
            assertThat(state).extracting(CloudEvent::getType)
                    .containsExactly(NameDefined.class.getName(), NameWasChanged.class.getName(), NameWasChanged.class.getName(), NameWasChanged.class.getName());
        }
    }

    /**
     * Tests ADR 116, "A refused write throws, and it must never be retried": a delivery action throwing
     * {@link CheckpointWriteConditionNotFulfilledException} must not be retried on either retry loop, the
     * subscription it belongs to must stay known and pausable rather than forgotten, and no other subscription on
     * the same model is affected.
     */
    @Nested
    @DisplayName("Checkpoint write refusal (ADR 116)")
    class CheckpointWriteRefusalTest {

        private CheckpointWriteConditionNotFulfilledException refusal(String subscriptionId) {
            return new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(5), CheckpointWriteCondition.notOlderThan(3));
        }

        /**
         * ADR 116 has the refusal reach "an executor's uncaught handler" once it is logged, on the outer restart
         * loop's dispatcher thread. Awaitility catches uncaught exceptions from every thread by default and
         * rethrows them into the polling {@code await()} call, which would otherwise turn this test's own
         * provoked, expected escape into a spurious failure. A thread factory that recognizes exactly this
         * exception keeps the assertion on the model's behaviour instead of on Awaitility's global safety net.
         */
        private ExecutorService dispatcherTolerantOfTheExpectedRefusalEscape() {
            return Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setUncaughtExceptionHandler((t, throwable) -> {
                    if (!(throwable instanceof CheckpointWriteConditionNotFulfilledException)) {
                        throw new AssertionError("Unexpected uncaught exception on subscription dispatcher thread " + t, throwable);
                    }
                });
                return thread;
            });
        }

        @Test
        void delivery_action_throwing_the_refusal_is_invoked_exactly_once() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position is a
            // concrete resume token before the target event arrives. Without it, a broken exclusion's restart would
            // resolve "start from now" at restart time and never rediscover the already-past target event, letting
            // this test pass by accident instead of by proving the exclusion holds.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId())) {
                    targetInvocations.incrementAndGet();
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));

            // Then: exactly one invocation, and it stays that way well past what a retry backoff, or an unbounded
            // restart loop reopening the change stream from the same concrete position, would allow.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
            // during(), not pollDelay()+atMost(): the latter only needs one truthy poll and returns as soon as it
            // sees one, so a redelivery landing between polls would go unnoticed. during() re-checks continuously
            // and fails the moment the count moves off 1, which is what "stays that way" actually means.
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
        }

        @Test
        void subscription_stays_known_and_pausable_and_a_resume_redelivers_the_refused_event() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position
            // is a concrete resume token before the target event arrives. Without it, the position would still be
            // the unresolved "start from now" it was subscribed with, since a refusal aborts the action before that
            // position is advanced, and a resume would then start from "now" at resume time, after the target event
            // rather than before it.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId()) && targetInvocations.getAndIncrement() == 0) {
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));

            // Then: the subscription is still known (pause succeeds rather than throwing UnknownSubscriptionException
            // or SubscriptionNotRunningException), and a resume redelivers the event the refusal aborted.
            subscriptionModel.pauseSubscription(subscriptionId);
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).contains(targetEvent.eventId()));
            assertThat(targetInvocations.get()).isEqualTo(2);
        }

        @Test
        void a_refused_write_on_one_subscription_does_not_stop_delivery_on_another() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String refusedSubscriptionId = UUID.randomUUID().toString();
            String healthySubscriptionId = UUID.randomUUID().toString();
            AtomicInteger refusedInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> healthyState = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(refusedSubscriptionId, event -> {
                refusedInvocations.incrementAndGet();
                throw refusal(refusedSubscriptionId);
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.subscribe(healthySubscriptionId, healthyState::add).waitUntilStarted(Duration.ofSeconds(10));

            LocalDateTime now = LocalDateTime.now();

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
                assertThat(refusedInvocations.get()).isEqualTo(1);
                assertThat(healthyState).hasSize(1);
            });
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "name2", "name2")));

            // Then: the healthy subscription keeps delivering, and the refused one still hasn't retried.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(healthyState).hasSize(2));
            assertThat(refusedInvocations.get()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Pause, cancel and start while a change stream cannot open")
    class WhileAChangeStreamCannotOpenTest {

        private final CopyOnWriteArrayList<Throwable> uncaught = new CopyOnWriteArrayList<>();

        private ExecutorService dispatcherRecordingUncaughtExceptions() {
            return Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setUncaughtExceptionHandler((t, throwable) -> uncaught.add(throwable));
                return thread;
            });
        }

        @Test
        void a_subscription_paused_while_waiting_to_restart_stays_paused() throws InterruptedException {
            // Given a one second backoff to pause in
            CountDownLatch failed = new CountDownLatch(1);
            AtomicInteger reopened = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsDuringIterationOnce(failed, reopened), TimeRepresentation.RFC_3339_STRING, dispatcherRecordingUncaughtExceptions(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofSeconds(1))));
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, state::add);
            assertThat(failed.await(10, SECONDS)).isTrue();

            // When
            subscriptionModel.pauseSubscription(subscriptionId);

            // Then
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                assertThat(reopened).hasValue(0);
                assertThat(state).isEmpty();
            });
            assertThat(uncaught).describedAs("errors thrown on the dispatcher thread").isEmpty();
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
            assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        }

        @Test
        void a_subscription_cancelled_while_waiting_to_restart_stays_cancelled() throws InterruptedException {
            // Given a one second backoff to cancel in
            CountDownLatch failed = new CountDownLatch(1);
            AtomicInteger reopened = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsDuringIterationOnce(failed, reopened), TimeRepresentation.RFC_3339_STRING, dispatcherRecordingUncaughtExceptions(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofSeconds(1))));
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, state::add);
            assertThat(failed.await(10, SECONDS)).isTrue();

            // When
            subscriptionModel.cancelSubscription(subscriptionId);

            // Then
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                assertThat(reopened).hasValue(0);
                assertThat(state).isEmpty();
            });
            assertThat(uncaught).describedAs("errors thrown on the dispatcher thread").isEmpty();
            assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        }

        @Test
        void a_subscription_made_while_the_model_is_stopped_receives_the_events_written_once_mongodb_can_be_reached_again_before_start() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            AtomicInteger answered = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(databaseThatCannotBeReachedWhile(unreachable, refused, answered), realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            subscriptionModel.stop();
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), handled::add);
            await().atMost(FIVE_SECONDS).until(() -> refused.get() > 0);

            // When MongoDB can be reached again, and an event is written, before start()
            unreachable.set(false);
            waitAtMostFor(FIVE_SECONDS, () -> answered.get() > 0);
            NameDefined writtenBeforeStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBeforeStart));
            subscriptionModel.start();

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenBeforeStart.eventId()));
        }

        @Test
        void a_subscription_stopped_before_mongodb_answered_the_request_for_its_operation_time_receives_the_events_written_once_mongodb_can_be_reached_again_before_start() {
            // Given a subscription made on a running model while MongoDB can't be reached, and then stopped
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            AtomicInteger answered = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(databaseThatCannotBeReachedWhile(unreachable, refused, answered), realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), handled::add);
            await().atMost(FIVE_SECONDS).until(() -> refused.get() > 0);
            subscriptionModel.stop();

            // When MongoDB can be reached again, and an event is written, before start()
            unreachable.set(false);
            waitAtMostFor(FIVE_SECONDS, () -> answered.get() > 0);
            NameDefined writtenBeforeStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBeforeStart));
            subscriptionModel.start();

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenBeforeStart.eventId()));
        }

        @Test
        void a_subscription_made_while_the_model_is_stopped_whose_retry_strategy_gives_up_on_mongodb_does_not_start_at_a_later_position() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(databaseThatCannotBeReachedWhile(unreachable, refused, new AtomicInteger()), realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherRecordingUncaughtExceptions(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
            subscriptionModel.stop();
            String subscriptionId = UUID.randomUUID().toString();
            Subscription subscription = subscriptionModel.subscribe(subscriptionId, __ -> {
            });
            await().atMost(FIVE_SECONDS).until(() -> refused.get() >= 2);
            unreachable.set(false);

            // When
            CompletableFuture<Void> starting = CompletableFuture.runAsync(subscriptionModel::start);

            // Then
            assertThat(starting).succeedsWithin(Duration.ofSeconds(5));
            assertThat(subscription.waitUntilStarted(Duration.ZERO)).isFalse();
            assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(uncaught).singleElement().isInstanceOf(MongoTimeoutException.class));
        }

        @Test
        void a_subscription_whose_retry_strategy_gave_up_on_mongodb_asks_again_when_paused_and_resumed() {
            // Given a subscription that start() didn't open because its retry strategy gave up on MongoDB
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(databaseThatCannotBeReachedWhile(unreachable, refused, new AtomicInteger()), realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
            subscriptionModel.stop();
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, handled::add);
            await().atMost(FIVE_SECONDS).until(() -> refused.get() >= 2);
            unreachable.set(false);
            subscriptionModel.start();

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            boolean resumed = subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(5));
            NameDefined writtenAfterResume = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenAfterResume));

            // Then
            assertThat(resumed).isTrue();
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenAfterResume.eventId()));
        }

        @Test
        void a_subscription_is_known_and_can_be_paused_and_cancelled_before_its_change_stream_opens() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsWhile(unreachable, refused, () -> new MongoTimeoutException("MongoDB cannot be reached")), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            CopyOnWriteArrayList<CloudEvent> pausedState = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> cancelledState = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("paused", pausedState::add);
            subscriptionModel.subscribe("cancelled", cancelledState::add);
            await().atMost(FIVE_SECONDS).until(() -> refused.get() >= 2);
            boolean runningBeforeItOpened = subscriptionModel.isRunning("paused");

            // When
            Throwable pauseRefusal = catchThrowable(() -> subscriptionModel.pauseSubscription("paused"));
            subscriptionModel.cancelSubscription("cancelled");
            unreachable.set(false);

            // Then
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                assertThat(pausedState).isEmpty();
                assertThat(cancelledState).isEmpty();
            });
            assertThat(runningBeforeItOpened).isTrue();
            assertThat(pauseRefusal).isNull();
            assertThat(subscriptionModel.isPaused("paused")).isTrue();
            assertThat(subscriptionModel.subscriptionIds()).containsExactly("paused");
        }

        @Test
        void the_subscription_paused_before_its_change_stream_opened_answers_that_it_started_once_a_resume_opens_it() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsWhile(unreachable, refused, () -> new MongoTimeoutException("MongoDB cannot be reached")), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            Subscription subscription = subscriptionModel.subscribe("a", __ -> {
            });
            await().atMost(FIVE_SECONDS).until(() -> refused.get() >= 1);
            subscriptionModel.pauseSubscription("a");
            unreachable.set(false);

            // When
            boolean resumed = subscriptionModel.resumeSubscription("a").waitUntilStarted(Duration.ofSeconds(10));

            // Then
            assertThat(resumed).isTrue();
            assertThat(subscription.waitUntilStarted(Duration.ofSeconds(2))).isTrue();
        }

        @Test
        void start_does_not_hold_up_other_calls_while_a_change_stream_cannot_open() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean();
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsWhile(unreachable, refused, () -> new MongoTimeoutException("MongoDB cannot be reached")), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            subscriptionModel.subscribe("a", __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.subscribe("b", __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.stop();
            unreachable.set(true);
            CompletableFuture<Void> start = CompletableFuture.runAsync(() -> subscriptionModel.start());
            await().atMost(FIVE_SECONDS).until(() -> refused.get() >= 2);

            try {
                // When
                CompletableFuture<Void> cancel = CompletableFuture.runAsync(() -> subscriptionModel.cancelSubscription("b"));

                // Then
                assertThat(cancel).succeedsWithin(Duration.ofSeconds(2));
                assertThat(CompletableFuture.supplyAsync(subscriptionModel::subscriptionIds)).succeedsWithin(Duration.ofSeconds(2)).isEqualTo(Set.of("a"));
                assertThat(start).isNotDone();
            } finally {
                // Lets a start that holds the model's lock return when an assertion above fails, so shutdown() can run
                unreachable.set(false);
            }
            assertThat(start).succeedsWithin(Duration.ofSeconds(10));
        }

        @Test
        void start_returns_when_change_stream_history_is_lost_and_not_configured_to_restart() {
            // Given
            AtomicBoolean historyLost = new AtomicBoolean();
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsWhile(historyLost, refused, NativeMongoSubscriptionModelResilienceTest::changeStreamHistoryLostException), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            subscriptionModel.subscribe("a", __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.stop();
            historyLost.set(true);

            // When
            CompletableFuture<Void> start = CompletableFuture.runAsync(() -> subscriptionModel.start());

            // Then
            assertThat(start).succeedsWithin(Duration.ofSeconds(10));
            assertThat(subscriptionModel.subscriptionIds()).isEmpty();
        }

        @Test
        void start_returns_when_the_retry_strategy_gives_up_and_the_subscription_stays_running() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean();
            AtomicInteger refused = new AtomicInteger();
            subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatFailsWhile(unreachable, refused, () -> new MongoTimeoutException("MongoDB cannot be reached")), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
            subscriptionModel.subscribe("a", __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.stop();
            unreachable.set(true);

            // When
            CompletableFuture<Void> start = CompletableFuture.runAsync(() -> subscriptionModel.start());

            // Then
            assertThat(start).succeedsWithin(Duration.ofSeconds(10));
            assertThat(refused).hasValue(2);
            assertThat(subscriptionModel.isRunning("a")).isTrue();
        }
    }

    @Nested
    @DisplayName("Subscribing while the model is stopped")
    class SubscribeWhileStoppedTest {

        private NativeMongoSubscriptionModel stoppedModelOver(MongoDatabase database) {
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            model.stop();
            return model;
        }

        @Test
        void subscribe_returns_without_waiting_for_mongodb_to_answer_the_request_for_its_operation_time() {
            // Given
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = stoppedModelOver(operationTimeAtSubscribe.wrap(database));
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Throwable thrown = catchThrowable(() -> Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () -> subscriptionModel.subscribe(subscriptionId, __ -> {
            })));

            // Then
            assertThat(thrown).isNull();
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
        }

        @Test
        void start_opens_the_change_stream_at_the_operation_time_asked_for_at_subscribe_when_the_answer_arrives_after_start() {
            // Given a subscription whose request for MongoDB's operation time has been answered, but not yet returned
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = stoppedModelOver(operationTimeAtSubscribe.wrap(database));
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), handled::add);
            operationTimeAtSubscribe.awaitAnswered();
            NameDefined writtenBeforeStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBeforeStart));

            // When start() gets there before the answer
            CompletableFuture<Void> starting = CompletableFuture.runAsync(subscriptionModel::start);
            await().pollDelay(Duration.ofMillis(500)).atMost(FIVE_SECONDS).until(() -> true);
            operationTimeAtSubscribe.release();

            // Then
            assertThat(starting).succeedsWithin(Duration.ofSeconds(5));
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenBeforeStart.eventId()));
        }

        @Test
        void a_subscription_paused_while_its_change_stream_waits_for_the_operation_time_resumes_at_it() {
            // Given a subscription that start() resumed while its request for MongoDB's operation time is outstanding
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = stoppedModelOver(operationTimeAtSubscribe.wrap(database));
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, handled::add);
            operationTimeAtSubscribe.awaitAnswered();
            NameDefined writtenBeforeStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBeforeStart));
            CompletableFuture<Void> starting = CompletableFuture.runAsync(subscriptionModel::start);
            await().atMost(FIVE_SECONDS).until(() -> subscriptionModel.isRunning(subscriptionId));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            operationTimeAtSubscribe.release();
            subscriptionModel.resumeSubscription(subscriptionId);

            // Then
            assertThat(starting).succeedsWithin(Duration.ofSeconds(5));
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenBeforeStart.eventId()));
        }

        @Test
        void a_pause_frees_the_dispatcher_thread_of_a_change_stream_waiting_for_the_operation_time() {
            // Given a subscription that start() resumed while its request for MongoDB's operation time is outstanding
            ThreadPoolExecutor dispatcher = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, SECONDS, new SynchronousQueue<>());
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = new NativeMongoSubscriptionModel(operationTimeAtSubscribe.wrap(database), realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcher,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
            subscriptionModel.stop();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });
            operationTimeAtSubscribe.awaitAnswered();
            CompletableFuture.runAsync(subscriptionModel::start);
            await().atMost(FIVE_SECONDS).until(() -> subscriptionModel.isRunning(subscriptionId));

            // When paused, resumed and paused again before the answer arrives
            subscriptionModel.pauseSubscription(subscriptionId);
            subscriptionModel.resumeSubscription(subscriptionId);
            subscriptionModel.pauseSubscription(subscriptionId);

            // Then only the thread the request is outstanding on stays busy
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(dispatcher.getActiveCount()).isOne());
        }

        @Test
        void cancelling_a_subscription_stops_its_outstanding_request_for_the_operation_time() {
            // Given
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = stoppedModelOver(operationTimeAtSubscribe.wrap(database));
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });
            operationTimeAtSubscribe.awaitAnswered();

            // When
            subscriptionModel.cancelSubscription(subscriptionId);

            // Then
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(operationTimeAtSubscribe.waiting()).isZero());
        }

        @Test
        void shutdown_stops_an_outstanding_request_for_the_operation_time_instead_of_waiting_for_it() {
            // Given
            FirstReplyHeld operationTimeAtSubscribe = new FirstReplyHeld();
            subscriptionModel = stoppedModelOver(operationTimeAtSubscribe.wrap(database));
            subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> {
            });
            operationTimeAtSubscribe.awaitAnswered();

            // When
            long before = System.nanoTime();
            subscriptionModel.shutdown();
            Duration shutdownTook = Duration.ofNanos(System.nanoTime() - before);

            // Then
            assertThat(shutdownTook).isLessThan(Duration.ofSeconds(2));
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(operationTimeAtSubscribe.waiting()).isZero());
        }
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
