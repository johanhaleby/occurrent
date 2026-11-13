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
 * A {@link SpringMongoSubscriptionModel} subscribed at the present opens its change stream at the present when
 * MongoDB's reply to the operation time question has no {@code operationTime}, and has recorded no position then.
 * This test stubs that reply and checks that the commit time of the first event is recorded before its action runs.
 */
@Testcontainers
@Timeout(30)
@DisplayName("SpringMongoSubscriptionModelNoOperationTime")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelNoOperationTimeTest {

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
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".noOperationTime");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        eventCollectionName = connectionString.getCollection();
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).eventStoreCapabilities(STREAM, DCB).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_and_resume_after_the_action_of_the_first_event_threw_delivers_that_event_again_when_mongodb_sent_no_operation_time() {
        // Given: a backoff of 2 s between attempts, so the pause comes during the first backoff
        subscriptionModel = new SpringMongoSubscriptionModel(templateThatSendsNoOperationTime(),
                withConfig(eventCollectionName, timeRepresentation).retryStrategy(RetryStrategy.fixed(2000)));
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger failures = new AtomicInteger();
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
            if (failing.get()) {
                failures.incrementAndGet();
                throw new IllegalStateException("expected");
            }
            handled.add(cloudEvent.getId());
        }).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> failures.get() >= 1);

        // When: paused while the retry backs off, then resumed once the listener works again
        subscriptionModel.pauseSubscription(subscriptionId);
        failing.set(false);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e2.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId());
    }

    /**
     * Answers the operation time question with a reply that has no {@code operationTime}, as a server that doesn't send
     * one does. Every other command gets its real reply.
     */
    private MongoTemplate templateThatSendsNoOperationTime() {
        return new MongoTemplate(mongoClient, requireNonNull(mongoTemplate.getDb().getName())) {
            @Override
            public Document executeCommand(Document command) {
                if (MongoCommons.CURRENT_OPERATION_TIME_COMMAND.equals(command)) {
                    return new Document("ok", 1.0);
                }
                return super.executeCommand(command);
            }
        };
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
