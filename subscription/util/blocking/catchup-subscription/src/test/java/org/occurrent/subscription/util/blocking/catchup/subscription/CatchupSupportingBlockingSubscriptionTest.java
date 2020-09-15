/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.util.blocking.catchup.subscription;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionForMongoDB;
import org.occurrent.subscription.mongodb.nativedriver.blocking.BlockingSubscriptionPositionStorageForMongoDB;
import org.occurrent.subscription.mongodb.nativedriver.blocking.RetryStrategy;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
@Timeout(15000)
public class CatchupSupportingBlockingSubscriptionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8");

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private MongoEventStore mongoEventStore;
    private CatchupSupportingBlockingSubscription subscription;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        TimeRepresentation timeRepresentation = TimeRepresentation.DATE;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        MongoCollection<Document> eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        subscriptionExecutor = Executors.newFixedThreadPool(1);
        BlockingSubscriptionForMongoDB blockingSubscriptionForMongoDB = new BlockingSubscriptionForMongoDB(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none());
        BlockingSubscriptionPositionStorage storage = new BlockingSubscriptionPositionStorageForMongoDB(database, "storage");
        subscription = new CatchupSupportingBlockingSubscription(blockingSubscriptionForMongoDB, mongoEventStore, storage);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        subscription.shutdown();
        subscriptionExecutor.shutdown();
        subscriptionExecutor.awaitTermination(10, SECONDS);
        mongoClient.close();
    }

    @Test
    void blocking_native_mongodb_subscription_calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        // When
        subscription.subscribe(UUID.randomUUID().toString(), StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}