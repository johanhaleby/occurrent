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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * An application that is shut down while it starts, a Spring context closing during startup for instance, can shut the
 * model down while a competing subscribe is making a durable subscription in the wrapped model.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerShutdownDuringSubscribeTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private SpringMongoCheckpointStorage checkpointStorage;
    private String locks;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(client, database);
        template.getDb().drop();
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        checkpointStorage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        locks = "locks-" + UUID.randomUUID();
    }

    @AfterEach
    void close() {
        client.close();
    }

    @Test
    void a_shutdown_that_overtakes_a_subscribe_keeps_the_position_the_durable_subscription_stored() throws Exception {
        // The first run handles one event, so the position of X is stored
        CompetingConsumerSubscriptionModel firstRun = new CompetingConsumerSubscriptionModel(durable(), strategy());
        List<String> handledInTheFirstRun = new CopyOnWriteArrayList<>();
        firstRun.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> handledInTheFirstRun.add(e.getId())).waitUntilStarted(Duration.ofSeconds(10));
        String first = writeEvent();
        await().atMost(10, SECONDS).until(() -> handledInTheFirstRun.contains(first) && checkpointStorage.read("X") != null);
        firstRun.shutdown();

        // The second run is shut down while the wrapped model makes X, and shutdown() waits in the strategy's shutdown
        // until the subscribe has found it shut down
        CountDownLatch made = new CountDownLatch(1);
        CountDownLatch releaseTheSubscribe = new CountDownLatch(1);
        CountDownLatch strategyShutDown = new CountDownLatch(1);
        CountDownLatch releaseTheShutdown = new CountDownLatch(1);
        CompetingConsumerSubscriptionModel secondRun = new CompetingConsumerSubscriptionModel(new WaitsAfterMaking(durable(), made, releaseTheSubscribe),
                new WaitsAfterShuttingDown(strategy(), strategyShutDown, releaseTheShutdown));
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> secondRun.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {}));
        assertThat(made.await(10, SECONDS)).as("the wrapped model made X").isTrue();
        CompletableFuture<Void> shuttingDown = CompletableFuture.runAsync(secondRun::shutdown);
        assertThat(strategyShutDown.await(10, SECONDS)).as("shutdown() shut the strategy down").isTrue();
        releaseTheSubscribe.countDown();
        Throwable subscribeFailure = catchThrowable(() -> subscribing.get(10, SECONDS));
        releaseTheShutdown.countDown();
        shuttingDown.get(10, SECONDS);
        String positionAfterTheSecondRun = String.valueOf(checkpointStorage.read("X"));

        String writtenWhileDown = writeEvent();

        // The third run starts from the stored position
        CompetingConsumerSubscriptionModel thirdRun = new CompetingConsumerSubscriptionModel(durable(), strategy());
        List<String> handledInTheThirdRun = new CopyOnWriteArrayList<>();
        thirdRun.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), e -> handledInTheThirdRun.add(e.getId())).waitUntilStarted(Duration.ofSeconds(10));
        String afterTheRestart = writeEvent();
        try {
            await().atMost(10, SECONDS).until(() -> handledInTheThirdRun.contains(afterTheRestart));
        } finally {
            thirdRun.shutdown();
        }

        assertThat(handledInTheThirdRun).as("events handled after the restart, position of X after the second run=" + positionAfterTheSecondRun + ", subscribe failure=" + subscribeFailure)
                .contains(writtenWhileDown, afterTheRestart);
    }

    private DurableSubscriptionModel durable() {
        return new DurableSubscriptionModel(new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING)), checkpointStorage);
    }

    private SpringMongoLeaseCompetingConsumerStrategy strategy() {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
    }

    private String writeEvent() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            latch.await(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Shuts the strategy down, then waits until released
    private record WaitsAfterShuttingDown(CompetingConsumerStrategy strategy, CountDownLatch shutDown, CountDownLatch release) implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return strategy.registerCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            strategy.unregisterCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            strategy.releaseCompetingConsumer(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return strategy.hasLock(subscriptionId, subscriberId);
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
            strategy.addListener(listener);
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
            strategy.removeListener(listener);
        }

        @Override
        public void shutdown() {
            strategy.shutdown();
            shutDown.countDown();
            waitFor(release);
        }
    }

    // Makes a subscription in the wrapped model, then waits until released before returning it
    private record WaitsAfterMaking(SubscriptionModel model, CountDownLatch made, CountDownLatch release) implements SubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return waitAfter(model.subscribe(subscriptionId, filter, startAt, action));
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return waitAfter(model.subscribePaused(subscriptionId, filter, startAt, action));
        }

        private Subscription waitAfter(Subscription subscription) {
            made.countDown();
            waitFor(release);
            return subscription;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            model.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            model.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            model.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return model.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return model.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return model.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return model.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            model.pauseSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            model.shutdown();
        }
    }
}
