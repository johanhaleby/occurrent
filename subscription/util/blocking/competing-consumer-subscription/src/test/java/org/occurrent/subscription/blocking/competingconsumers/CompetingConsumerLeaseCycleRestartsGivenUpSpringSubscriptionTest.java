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
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataAccessResourceFailureException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A Spring subscription whose restart loop gave up still counts as running, so losing and regaining the lease pauses
 * it and resumes it, and the resume opens a new change stream. The lease handover is driven through the two listener
 * calls a strategy's refresh would make, so nothing here waits for a lease to expire.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseCycleRestartsGivenUpSpringSubscriptionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final AtomicBoolean unreachable = new AtomicBoolean();
    private final AtomicInteger refusedChangeStreams = new AtomicInteger();

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private MongoTemplate unreachableWhileToldTo;
    private String eventCollection;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        eventCollection = requireNonNull(connectionString.getCollection());
        mongoTemplate = new MongoTemplate(mongoClient, database);
        // The change stream is opened from getDb(), so failing only that call keeps the operation time request
        // and the event store working
        unreachableWhileToldTo = new MongoTemplate(mongoClient, database) {
            @Override
            public MongoDatabase getDb() {
                if (unreachable.get()) {
                    refusedChangeStreams.incrementAndGet();
                    throw new DataAccessResourceFailureException("MongoDB cannot be reached", new MongoTimeoutException("timed out"));
                }
                return super.getDb();
            }
        };
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, database));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        if (competingConsumerSubscriptionModel != null) {
            competingConsumerSubscriptionModel.shutdown();
        }
    }

    @Timeout(value = 30, unit = SECONDS)
    @Test
    void losing_and_regaining_the_lease_restarts_a_subscription_whose_restarts_gave_up() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        SpringMongoSubscriptionModel springModel = new SpringMongoSubscriptionModel(unreachableWhileToldTo, new SpringMongoSubscriptionModelConfig(eventCollection, TimeRepresentation.RFC_3339_STRING)
                .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
        competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(springModel, new AlwaysGrantedCompetingConsumerStrategy());
        unreachable.set(true);
        competingConsumerSubscriptionModel.subscribe("A", subscriptionId, null, StartAt.subscriptionModelDefault(), delivered::add);
        awaitRestartsGivingUp();
        unreachable.set(false);

        // When
        assertThatCode(() -> competingConsumerSubscriptionModel.onConsumeProhibited(subscriptionId, "A")).doesNotThrowAnyException();
        competingConsumerSubscriptionModel.onConsumeGranted(subscriptionId, "A");

        // Then
        NameDefined writtenAfterTheLeaseCycle = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name");
        eventStore.write(UUID.randomUUID().toString(), serialize(writtenAfterTheLeaseCycle));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).contains(writtenAfterTheLeaseCycle.eventId()));
    }

    // The restart loop has given up once the refusals stop growing, with the retry's 100 ms backoff well inside
    // the poll interval
    private void awaitRestartsGivingUp() {
        AtomicInteger lastSeen = new AtomicInteger(-1);
        await("the restart loop gives up").atMost(10, SECONDS).pollInterval(Duration.ofMillis(750)).until(() -> {
            int refused = refusedChangeStreams.get();
            return refused >= 2 && lastSeen.getAndSet(refused) == refused;
        });
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(OBJECT_MAPPER::writeValueAsBytes).apply(e))
                .build());
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final class AlwaysGrantedCompetingConsumerStrategy implements CompetingConsumerStrategy {

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
        }
    }
}
