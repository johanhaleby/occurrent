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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pause moves the subscription from the running subscriptions to the paused ones and closes its cursor in between.
 * A reader that asks while the cursor is being closed finds the subscription running or paused, never neither. The
 * close is held until the reader has asked, or for half a second when the reader has to wait for the pause to end.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelPauseWindowTest {

    @Container
    private static final ReplicaSetReadyMongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private final CountDownLatch closingTheCursor = new CountDownLatch(1);
    private final CountDownLatch readerHasAsked = new CountDownLatch(1);
    private final AtomicBoolean holdTheNextClose = new AtomicBoolean();
    private final ExecutorService subscriptionExecutor = Executors.newCachedThreadPool();
    private final ExecutorService threads = Executors.newCachedThreadPool();
    private MongoClient mongoClient;
    private SpringMongoSubscriptionModel model;

    @BeforeEach
    void create_the_model() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase())) {
            @Override
            public MongoDatabase getDb() {
                return databaseWhoseCursorsAreClosedSlowly(super.getDb());
            }
        };
        model = new SpringMongoSubscriptionModel(mongoTemplate, SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).executor(subscriptionExecutor));
    }

    @AfterEach
    void shut_down() {
        readerHasAsked.countDown();
        model.shutdown();
        threads.shutdownNow();
        subscriptionExecutor.shutdownNow();
        mongoClient.close();
    }

    @Test
    void a_subscription_is_running_or_paused_for_a_reader_that_asks_while_a_pause_closes_its_cursor() throws Exception {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        model.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        Future<Boolean> readerFoundItNeither = threads.submit(() -> {
            assertThat(closingTheCursor.await(10, TimeUnit.SECONDS)).as("cursor being closed").isTrue();
            boolean running = model.isRunning(subscriptionId);
            boolean paused = model.isPaused(subscriptionId);
            readerHasAsked.countDown();
            return !running && !paused;
        });
        holdTheNextClose.set(true);

        // When
        model.pauseSubscription(subscriptionId);

        // Then
        assertThat(readerFoundItNeither.get(10, TimeUnit.SECONDS)).as("reader found the subscription neither running nor paused").isFalse();
        assertThat(model.isPaused(subscriptionId)).as("paused once the pause returned").isTrue();
    }

    private MongoDatabase databaseWhoseCursorsAreClosedSlowly(MongoDatabase database) {
        return (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(), new Class<?>[]{MongoDatabase.class}, (proxy, method, args) -> {
            Object result = invoke(database, method, args);
            return method.getName().equals("getCollection") && result instanceof MongoCollection ? collectionWhoseCursorIsClosedSlowly((MongoCollection<?>) result) : result;
        });
    }

    private MongoCollection<?> collectionWhoseCursorIsClosedSlowly(MongoCollection<?> collection) {
        return (MongoCollection<?>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(), new Class<?>[]{MongoCollection.class}, (proxy, method, args) -> {
            Object result = invoke(collection, method, args);
            return method.getName().equals("watch") && result instanceof ChangeStreamIterable ? changeStreamWhoseCursorIsClosedSlowly((ChangeStreamIterable<?>) result) : result;
        });
    }

    private ChangeStreamIterable<?> changeStreamWhoseCursorIsClosedSlowly(ChangeStreamIterable<?> changeStream) {
        return (ChangeStreamIterable<?>) Proxy.newProxyInstance(ChangeStreamIterable.class.getClassLoader(), new Class<?>[]{ChangeStreamIterable.class}, (proxy, method, args) -> {
            Object result = invoke(changeStream, method, args);
            if (method.getName().equals("cursor") && result instanceof MongoChangeStreamCursor) {
                return cursorClosedSlowly((MongoChangeStreamCursor<?>) result);
            }
            return result instanceof ChangeStreamIterable ? changeStreamWhoseCursorIsClosedSlowly((ChangeStreamIterable<?>) result) : result;
        });
    }

    private MongoChangeStreamCursor<?> cursorClosedSlowly(MongoChangeStreamCursor<?> cursor) {
        return (MongoChangeStreamCursor<?>) Proxy.newProxyInstance(MongoChangeStreamCursor.class.getClassLoader(), new Class<?>[]{MongoChangeStreamCursor.class}, (proxy, method, args) -> {
            if (method.getName().equals("close") && holdTheNextClose.compareAndSet(true, false)) {
                closingTheCursor.countDown();
                readerHasAsked.await(500, TimeUnit.MILLISECONDS);
            }
            return invoke(cursor, method, args);
        });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
