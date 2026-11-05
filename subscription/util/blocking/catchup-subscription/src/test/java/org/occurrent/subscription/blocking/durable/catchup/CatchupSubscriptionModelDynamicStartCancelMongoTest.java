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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The {@link CatchupSubscriptionModel} dispatcher cancels an id on behalf of its inner catch-up models, and a dynamic
 * start-position function can wait for such a cancel on another thread. The function is asked before the dispatcher
 * claims the id, so the cancel returns at once, wins over the subscribe, and the subscribe
 * subscribes nothing.
 */
@Testcontainers
@Timeout(120)
@DisplayName("CatchupSubscriptionModelDynamicStartCancel")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelDynamicStartCancelMongoTest {

    // Longer than LIFECYCLE_WAIT, so a claim that gives up while the function still waits shows as a failed cancel
    private static final Duration FUNCTION_WAITS = AbstractCatchupSubscriptionModel.LIFECYCLE_WAIT.plusSeconds(2);
    private static final Duration EVENT_DELIVERY = Duration.ofSeconds(10);
    private static final URI SOURCE = URI.create("urn:test");
    private static final String DATABASE = "catchupdynamicstartcancel";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, DATABASE));

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private CatchupSubscriptionModelFixture fixture;
    private SubscriptionModel model;
    private ExecutorService otherThreads;
    private final String subscriptionId = UUID.randomUUID().toString();

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
    void create_instances() {
        fixture = new CatchupSubscriptionModelFixture(mongoClient, database);
        model = fixture.subscriptionModel();
        otherThreads = Executors.newCachedThreadPool();
    }

    @AfterEach
    void shutdown() {
        otherThreads.shutdownNow();
        fixture.close();
    }

    @Test
    void a_cancel_from_another_thread_returns_while_the_function_waits_for_it() {
        // Given
        WaitsForCancel function = new WaitsForCancel();

        // When
        model.subscribe(subscriptionId, function.startAt(), event -> {
        });

        // Then
        function.assertTheCancelReturnedWhileTheFunctionWaited();
    }

    @Test
    void the_returned_subscription_never_starts_and_the_id_is_not_running_after_the_cancel() {
        // Given
        WaitsForCancel function = new WaitsForCancel();
        Subscription returned = model.subscribe(subscriptionId, function.startAt(), event -> {
        });
        function.assertTheCancelReturnedWhileTheFunctionWaited();

        // When
        boolean started = returned.waitUntilStarted(Duration.ofMillis(500));

        // Then
        assertAll(
                () -> assertThat(started).as("the returned subscription started").isFalse(),
                () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isFalse()
        );
    }

    @Test
    void no_event_reaches_the_action_of_the_cancelled_subscribe() {
        // Given
        WaitsForCancel function = new WaitsForCancel();
        List<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe(subscriptionId, function.startAt(), received::add);
        function.assertTheCancelReturnedWhileTheFunctionWaited();

        // When
        fixture.publish(List.of(anEvent()));

        // Then an event that does not arrive can only be shown by waiting, so the action is watched for a while
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(received::isEmpty);
    }

    @Test
    void the_id_can_be_subscribed_again_and_the_new_subscription_is_delivered_an_event() {
        // Given
        WaitsForCancel function = new WaitsForCancel();
        model.subscribe(subscriptionId, function.startAt(), event -> {
        });
        function.assertTheCancelReturnedWhileTheFunctionWaited();
        List<CloudEvent> again = new CopyOnWriteArrayList<>();

        // When
        boolean started = model.subscribe(subscriptionId, StartAt.now(), again::add).waitUntilStarted(EVENT_DELIVERY);
        fixture.publish(List.of(anEvent()));

        // Then
        await().atMost(EVENT_DELIVERY).until(() -> again.size() == 1);
        assertThat(started).isTrue();
    }

    private static CloudEvent anEvent() {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("test")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    /**
     * A start-position function that, the first time it is asked, cancels its own subscription id on another thread and
     * waits for that cancel to return, longer than {@code LIFECYCLE_WAIT}. Any later ask answers {@code now} at once.
     */
    private final class WaitsForCancel {
        private final AtomicBoolean asked = new AtomicBoolean();
        private final CompletableFuture<Void> cancelCompleted = new CompletableFuture<>();
        private final AtomicBoolean cancelReturnedWhileWaiting = new AtomicBoolean();

        StartAt startAt() {
            return StartAt.dynamic(context -> {
                if (asked.compareAndSet(false, true)) {
                    CompletableFuture.runAsync(() -> model.cancelSubscription(subscriptionId), otherThreads).whenComplete((__, error) -> {
                        if (error == null) {
                            cancelCompleted.complete(null);
                        } else {
                            cancelCompleted.completeExceptionally(error);
                        }
                    });
                    try {
                        cancelCompleted.get(FUNCTION_WAITS.toMillis(), TimeUnit.MILLISECONDS);
                        cancelReturnedWhileWaiting.set(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException | TimeoutException e) {
                        // assertTheCancelReturnedWhileTheFunctionWaited() decides whether the cancel returned, so the function only stops waiting
                    }
                }
                return StartAt.now();
            });
        }

        // The cancel has to return before the function stops waiting for it. A cancel that returns only once the
        // subscribe has returned, because it waited for the subscribe, is the deadlock this guards against
        void assertTheCancelReturnedWhileTheFunctionWaited() {
            assertThat(cancelCompleted).as("the cancel made while the function waited").succeedsWithin(EVENT_DELIVERY);
            assertThat(cancelReturnedWhileWaiting).as("the cancel returned before the function stopped waiting for it").isTrue();
        }
    }
}
