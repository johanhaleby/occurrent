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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * What {@link SpringMongoSubscriptionModel} does for an action that keeps failing, for a pause or a cancel that comes
 * before the change stream has opened, and for the executor it makes itself.
 */
@Testcontainers
@Timeout(40)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelCursorLoopTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CountDownLatch openTheChangeStream = new CountDownLatch(1);
    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String eventCollectionName;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".cursorloop");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, connectionString.getDatabase())))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        openTheChangeStream.countDown();
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void an_event_whose_action_still_throws_after_its_retries_is_delivered_again_and_no_later_event_is_handled_before_it() {
        // Given an action that fails three times for one event, and a retry strategy that gives up after two
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventCollectionName, TimeRepresentation.RFC_3339_STRING)
                .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(20)).maxAttempts(2)));
        NameDefined first = nameDefined();
        NameDefined failing = nameDefined();
        NameDefined last = nameDefined();
        AtomicInteger failures = new AtomicInteger();
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), cloudEvent -> {
            if (cloudEvent.getId().equals(failing.eventId()) && failures.getAndIncrement() < 3) {
                throw new IllegalStateException("expected");
            }
            handled.add(cloudEvent.getId());
        }).waitUntilStarted(Duration.ofSeconds(10));
        // Handled first, so the position a restart continues from is the one of an event
        mongoEventStore.write("1", serialize(first));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).containsExactly(first.eventId()));

        // When
        mongoEventStore.write("2", serialize(failing));
        mongoEventStore.write("3", serialize(last));

        // Then
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(handled).as("the events the action completed for").contains(last.eventId()));
        assertThat(handled).as("the events the action completed for").containsExactly(first.eventId(), failing.eventId(), last.eventId());
    }

    @Test
    void a_subscription_paused_before_its_change_stream_has_opened_receives_nothing_until_it_is_resumed() throws InterruptedException {
        // Given
        CountDownLatch opening = new CountDownLatch(1);
        subscriptionModel = new SpringMongoSubscriptionModel(templateThatWaitsBeforeItOpensAChangeStream(opening), eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(subscriptionId, cloudEvent -> handled.add(cloudEvent.getId()));
        assertThat(opening.await(10, SECONDS)).isTrue();

        // When
        subscriptionModel.pauseSubscription(subscriptionId);
        openTheChangeStream.countDown();
        NameDefined writtenWhilePaused = nameDefined();
        mongoEventStore.write("1", serialize(writtenWhilePaused));

        // Then
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).untilAsserted(() -> assertThat(handled).as("handled while paused").isEmpty());
        assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined writtenAfterTheResume = nameDefined();
        mongoEventStore.write("2", serialize(writtenAfterTheResume));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).contains(writtenAfterTheResume.eventId()));
        assertThat(handled).doesNotHaveDuplicates();
    }

    @Test
    void a_subscription_cancelled_before_its_change_stream_has_opened_receives_nothing_and_its_id_can_be_subscribed_again() throws InterruptedException {
        // Given
        CountDownLatch opening = new CountDownLatch(1);
        subscriptionModel = new SpringMongoSubscriptionModel(templateThatWaitsBeforeItOpensAChangeStream(opening), eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> handledByTheCancelled = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(subscriptionId, cloudEvent -> handledByTheCancelled.add(cloudEvent.getId()));
        assertThat(opening.await(10, SECONDS)).isTrue();

        // When
        subscriptionModel.cancelSubscription(subscriptionId);
        openTheChangeStream.countDown();
        CopyOnWriteArrayList<String> handledByTheNext = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(subscriptionId, cloudEvent -> handledByTheNext.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined writtenAfterTheCancel = nameDefined();
        mongoEventStore.write("1", serialize(writtenAfterTheCancel));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByTheNext).containsExactly(writtenAfterTheCancel.eventId()));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handledByTheCancelled).as("handled by the cancelled subscription").isEmpty());
    }

    @Test
    void shutdown_ends_the_threads_of_the_executor_the_model_made() {
        // Given
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        AtomicReference<Thread> threadOfTheExecutor = new AtomicReference<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> threadOfTheExecutor.set(Thread.currentThread())).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", serialize(nameDefined()));
        await().atMost(10, SECONDS).until(() -> threadOfTheExecutor.get() != null);

        // When
        subscriptionModel.shutdown();

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(threadOfTheExecutor.get().isAlive()).as("the thread that handled the event is alive").isFalse());
    }

    // The first getDb() is the one that opens the first change stream, and it waits until the test lets it go on
    private MongoTemplate templateThatWaitsBeforeItOpensAChangeStream(CountDownLatch opening) {
        MongoTemplate waiting = spy(mongoTemplate);
        doAnswer(invocation -> {
            if (opening.getCount() > 0) {
                opening.countDown();
                openTheChangeStream.await();
            }
            return invocation.callRealMethod();
        }).when(waiting).getDb();
        return waiting;
    }

    private static NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name");
    }

    private List<CloudEvent> serialize(DomainEvent event) {
        return List.of(CloudEventBuilder.v1()
                .withId(event.eventId())
                .withSource(URI.create("http://name"))
                .withType(event.getClass().getName())
                .withTime(toLocalDateTime(event.timestamp()).atOffset(UTC))
                .withSubject(event.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(event))
                .build());
    }
}
