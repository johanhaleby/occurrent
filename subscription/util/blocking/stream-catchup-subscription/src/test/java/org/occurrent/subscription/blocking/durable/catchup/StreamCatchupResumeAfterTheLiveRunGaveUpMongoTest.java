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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A subscription whose live delegate gave up restarting it after it went live still runs on the catch-up model, so a
 * resume at a position is refused until the subscription is paused. The live delegate is the real
 * {@link NativeMongoSubscriptionModel}, whose event collection stops serving change streams while a flag is set.
 */
@Testcontainers
@Timeout(30)
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupResumeAfterTheLiveRunGaveUpMongoTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final AtomicBoolean changeStreamsFail = new AtomicBoolean();
    private final AtomicInteger refusedOpens = new AtomicInteger();
    private final List<Throwable> thrownOnTheDispatcher = new CopyOnWriteArrayList<>();

    private MongoClient mongoClient;
    private MongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private ExecutorService dispatcher;
    private NativeMongoSubscriptionModel liveDelegate;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        String collectionName = requireNonNull(connectionString.getCollection());
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new MongoEventStore(mongoClient, databaseName, collectionName, eventStoreConfig);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        // A give-up is thrown on the dispatcher thread, which is where the test sees it
        dispatcher = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, throwable) -> thrownOnTheDispatcher.add(throwable));
            return thread;
        });
        liveDelegate = new NativeMongoSubscriptionModel(database, collectionFailingChangeStreamsWhileFlagged(database.getCollection(collectionName)), TimeRepresentation.RFC_3339_STRING, dispatcher,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
        catchup = new StreamCatchupSubscriptionModel(liveDelegate, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shutdown() {
        if (catchup != null) {
            catchup.shutdown();
        }
        if (dispatcher != null) {
            dispatcher.shutdownNow();
        }
        mongoClient.close();
    }

    @Test
    void a_subscription_whose_live_run_gave_up_after_going_live_is_refused_a_resume_at_a_position_until_paused_and_then_delivers_from_that_position() {
        // Given a subscription that went live and delivered an event
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        catchup.subscribe(subscriptionId, StartAt.now(), event -> received.add(nameOf(event))).waitUntilStarted();
        append(nameDefined("beforeGiveUp", 0));
        await("the live run delivers what is written after it opened").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).containsExactly("beforeGiveUp"));

        // And its open change stream fails and every reopen is refused until the retry strategy gives up
        changeStreamsFail.set(true);
        await("the live delegate gives up restarting the subscription").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(thrownOnTheDispatcher).singleElement().isInstanceOf(MongoTimeoutException.class));
        assertThat(refusedOpens.get()).as("reopens refused before the give-up").isPositive();
        assertThat(catchup.isRunning(subscriptionId)).as("running on the catch-up model after the give-up").isTrue();
        changeStreamsFail.set(false);

        // And an event written before a position the live delegate restarts at, and one written after it
        append(nameDefined("beforePosition", 1));
        Checkpoint position = requireNonNull(liveDelegate.globalCheckpoint());
        append(nameDefined("afterPosition", 2));

        // When it is resumed at that position without a pause
        // Then the resume is refused, since the subscription still runs here
        assertThatThrownBy(() -> catchup.resumeSubscription(subscriptionId, StartAt.checkpoint(position)))
                .as("a resume at a position of the running subscription")
                .isInstanceOf(SubscriptionAlreadyRunningException.class);

        // When it is paused and then resumed at that position
        catchup.pauseSubscription(subscriptionId);
        assertThatCode(() -> catchup.resumeSubscription(subscriptionId, StartAt.checkpoint(position)))
                .as("a resume at a position of the paused subscription")
                .doesNotThrowAnyException();

        // Then it delivers what was written after the position and not what was written before it
        await("the resumed subscription delivers what was written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(received).contains("afterPosition"));
        assertThat(received).as("what the subscription received").containsExactly("beforeGiveUp", "afterPosition");
    }

    // Fails the open cursor on its next read and refuses every change stream opened while the flag is set, so the give-up
    // is the native model's own restart loop running out of attempts and not a double that imitates it
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionFailingChangeStreamsWhileFlagged(MongoCollection<Document> collection) {
        return proxy(MongoCollection.class, collection, (method, args) -> {
            if (method.getName().equals("watch") && changeStreamsFail.get()) {
                refusedOpens.incrementAndGet();
                throw new MongoTimeoutException("Change streams cannot be opened");
            }
            return null;
        });
    }

    // Wraps what the delegate returns, so the iterable and the cursor the native model reaches through it also fail
    // while the flag is set. The answer of onCall replaces the delegate's call when it isn't null
    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> type, T delegate, CallBeforeTheDelegate onCall) {
        InvocationHandler handler = (ignored, method, args) -> {
            Object answer = onCall.call(method, args);
            if (answer != null) {
                return answer;
            }
            if (method.getName().equals("tryNext") && delegate instanceof MongoChangeStreamCursor<?> && changeStreamsFail.get()) {
                throw new MongoTimeoutException("The change stream ended");
            }
            Object result;
            try {
                result = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (result instanceof ChangeStreamIterable<?> iterable) {
                return proxy(ChangeStreamIterable.class, iterable, (m, a) -> null);
            }
            if (result instanceof MongoChangeStreamCursor<?> cursor) {
                return proxy(MongoChangeStreamCursor.class, cursor, (m, a) -> null);
            }
            return result;
        };
        return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{type}, handler);
    }

    @FunctionalInterface
    private interface CallBeforeTheDelegate {
        Object call(Method method, Object[] args);
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private static NameDefined nameDefined(String name, int secondsAfterStart) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0, secondsAfterStart), "name", name);
    }

    private void append(DomainEvent event) {
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }
}
