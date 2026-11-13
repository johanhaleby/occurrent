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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A pause of a {@link SpringMongoSubscriptionModel} subscription waits for a running action for one second at most, so
 * an action that is stuck outlives the pause. A resumed subscription must not call its action while that call still
 * runs.
 */
@Testcontainers
@Timeout(30)
@DisplayName("SpringMongoSubscriptionModelStuckAction")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelStuckActionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    private final NameDefined e1 = nameDefined("name1");
    private final NameDefined e2 = nameDefined("name2");
    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stuckAction");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        eventCollectionName = connectionString.getCollection();
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).eventStoreCapabilities(STREAM, DCB).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    private final CountDownLatch releaseFirstCallOfE1 = new CountDownLatch(1);
    private final AtomicBoolean firstCallOfE1 = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger mostInFlight = new AtomicInteger();
    private final CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();

    @AfterEach
    void shutdown() {
        // The stuck call ignores interrupts, so the model's shutdown would otherwise wait for it
        releaseFirstCallOfE1.countDown();
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_resumed_subscription_calls_its_action_only_once_the_call_that_outlived_the_pause_has_returned() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventCollectionName, timeRepresentation));
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

    private void action(CloudEvent cloudEvent) {
        String name = cloudEvent.getId().equals(e1.eventId()) ? "E1" : "E2";
        mostInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        calls.add(name + " started");
        try {
            if (name.equals("E1") && firstCallOfE1.getAndSet(false)) {
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
