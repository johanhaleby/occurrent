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

package org.occurrent.subscription.reactor.durable.catchup;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A straight-to-live subscribe on top of {@link ReactorMongoSubscriptionModel}, which calls the caller's dynamic
 * {@link StartAt} again on the thread that subscribed. Two subscriptions whose functions each cancel the other through
 * the catch-up model, without blocking on the cancel, must neither deadlock nor leave either one running.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupDynamicStartCancelMongoTest {

    private static final Duration STARTUP = Duration.ofSeconds(60);
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final StreamSubscriptionFilter FILTER = StreamSubscriptionFilter.filter(Filter.all());

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "dynamicstartcancel"));

    private MongoClient mongoClient;
    private ReactorStreamCatchupSubscriptionModel catchup;

    @AfterEach
    void shutdown() {
        if (catchup != null) {
            catchup.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void two_subscribes_return_when_the_dynamic_start_of_each_cancels_the_other_subscription_without_waiting_for_the_cancel() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("dynamicstartcancel"));
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactorMongoSubscriptionModel subscriptions = new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
        catchup = new ReactorStreamCatchupSubscriptionModel(subscriptions, new UnusedReader());
        loadClassesOfASubscribeAndACancel();
        CountDownLatch bothInsideTheMongoSubscribe = new CountDownLatch(2);
        Map<String, CompletableFuture<Void>> cancels = new ConcurrentHashMap<>();
        Map<String, String> evaluatedOn = new ConcurrentHashMap<>();
        CompletableFuture<Void> subscribeOfX = new CompletableFuture<>();
        CompletableFuture<Void> subscribeOfY = new CompletableFuture<>();
        Thread subscribingX = daemon("subscribe-x", subscribeOfX, () -> catchup.subscribe("x", FILTER,
                cancellingFromTheMongoModel("y", bothInsideTheMongoSubscribe, cancels, evaluatedOn, "x"), cloudEvent -> Mono.empty()));
        Thread subscribingY = daemon("subscribe-y", subscribeOfY, () -> catchup.subscribe("y", FILTER,
                cancellingFromTheMongoModel("x", bothInsideTheMongoSubscribe, cancels, evaluatedOn, "y"), cloudEvent -> Mono.empty()));
        subscribingX.start();
        subscribingY.start();
        awaitLatch(bothInsideTheMongoSubscribe, STARTUP, "The subscribes of x and y were not both inside the Mongo model");

        await(CompletableFuture.allOf(subscribeOfX, subscribeOfY), "The subscribes of x and y did not return. x returned: " + subscribeOfX.isDone()
                + ", " + describe(subscribingX) + ". y returned: " + subscribeOfY.isDone() + ", " + describe(subscribingY));
        await(CompletableFuture.allOf(cancels.get("x"), cancels.get("y")), "A cancel did not complete");

        assertThat(evaluatedOn).as("threads the Mongo model evaluated the start on").containsEntry("x", "subscribe-x").containsEntry("y", "subscribe-y");
        assertThat(subscriptions.isRunning("x") || subscriptions.isPaused("x")).as("whether the Mongo model holds x").isFalse();
        assertThat(subscriptions.isRunning("y") || subscriptions.isPaused("y")).as("whether the Mongo model holds y").isFalse();
    }

    // Keeps the time the threads of a test take to load these classes out of what the deadline measures
    private void loadClassesOfASubscribeAndACancel() {
        catchup.subscribe("warm-up", FILTER, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("warm-up").block(STARTUP);
    }

    // Answers null to the catch-up, which sends the subscribe straight to live, and cancels the other id the first time
    // the Mongo model asks, once both subscribes are inside it. Fires the cancel without blocking on it.
    private StartAt cancellingFromTheMongoModel(String other, CountDownLatch bothInside, Map<String, CompletableFuture<Void>> cancels,
                                                Map<String, String> evaluatedOn, String own) {
        AtomicBoolean cancelled = new AtomicBoolean();
        return StartAt.dynamic(context -> {
            if (!context.hasSubscriptionModelType(ReactorMongoSubscriptionModel.class)) {
                return null;
            }
            if (cancelled.compareAndSet(false, true)) {
                evaluatedOn.put(own, Thread.currentThread().getName());
                bothInside.countDown();
                awaitLatch(bothInside, STARTUP, "The other subscribe never reached the Mongo model");
                cancels.put(own, catchup.cancelSubscription(other).toFuture());
            }
            return StartAt.now();
        });
    }

    private static Thread daemon(String name, CompletableFuture<Void> returned, Runnable work) {
        Thread thread = new Thread(() -> {
            try {
                work.run();
                returned.complete(null);
            } catch (Throwable t) {
                returned.completeExceptionally(t);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void await(CompletableFuture<?> future, String failure) {
        try {
            future.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(failure + " within " + DEADLINE.toSeconds() + " seconds");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(failure + ", it failed with " + e.getCause(), e.getCause());
        }
    }

    private static void awaitLatch(CountDownLatch latch, Duration timeout, String failure) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError(failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static String describe(Thread thread) {
        return "thread " + thread.getName() + " is " + thread.getState() + " at " + Arrays.stream(thread.getStackTrace()).limit(6).toList();
    }

    // Nothing is replayed here, since the subscribes under test go straight to live
    private static final class UnusedReader implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("Nothing must be replayed"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(0L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }
}
