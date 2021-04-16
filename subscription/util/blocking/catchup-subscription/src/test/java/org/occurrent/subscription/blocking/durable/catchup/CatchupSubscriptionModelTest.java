/*
 * Copyright 2021 Johan Haleby
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
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionPositionStorage;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.occurrent.filter.Filter.TIME;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;
import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
@Timeout(15000)
public class CatchupSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private MongoEventStore mongoEventStore;
    private CatchupSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;
    private MongoDatabase database;
    private MongoCollection<Document> eventCollection;
    private NativeMongoSubscriptionPositionStorage storage;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        TimeRepresentation timeRepresentation = TimeRepresentation.DATE;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        storage = new NativeMongoSubscriptionPositionStorage(database, "storage");
        subscription = newCatchupSubscription(database, eventCollection, timeRepresentation, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));
        objectMapper = new ObjectMapper();
    }


    @AfterEach
    void shutdown() {
        subscription.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 5, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void catchup_subscription_reads_historic_events() {
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

    @Test
    void catchup_subscription_reads_historic_events_with_filter() {
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
        subscription.subscribe(UUID.randomUUID().toString(), filter(type(NameDefined.class.getName())), StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(2);
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getName());
        });
    }

    @Test
    void catchup_subscription_reads_historic_events_with_filter_using_custom_sort_by_during_catchup_phase() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();
        String eventId3 = UUID.randomUUID().toString();
        NameDefined nameDefined1 = new NameDefined(eventId1, now, "name1");
        NameDefined nameDefined2 = new NameDefined(eventId2, now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(eventId3, now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        CatchupSubscriptionModelConfig cfg = new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1))
                .catchupPhaseSortBy(SortBy.descending(TIME));
        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, cfg);

        // When
        subscription.subscribe(UUID.randomUUID().toString(), StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).containsExactly(eventId3, eventId2, eventId1));
    }

    @Test
    void catchup_subscription_reads_historic_events_and_then_switches_to_new_events() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameDefined nameDefined3 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(6), "name5");
        NameDefined nameDefined4 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(7), "name6");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        CountDownLatch writeFirstEvent = new CountDownLatch(1);
        CountDownLatch writeSecondEvent = new CountDownLatch(1);

        Thread thread = new Thread(() -> {
            awaitLatch(writeFirstEvent);
            mongoEventStore.write("3", 0, serialize(nameDefined3));

            awaitLatch(writeSecondEvent);
            mongoEventStore.write("4", 0, serialize(nameDefined4));
        });
        thread.start();

        // When
        subscription.subscribe(UUID.randomUUID().toString(), StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), e -> {
            state.add(e);
            switch (state.size()) {
                case 2:
                    writeFirstEvent.countDown();
                    break;
                case 3:
                    writeSecondEvent.countDown();
                    break;
            }
        }).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(state).hasSize(5).extracting(this::deserialize).containsExactly(nameDefined1, nameDefined2, nameWasChanged1, nameDefined3, nameDefined4));

        thread.join();
    }

    @Test
    void catchup_subscription_continues_where_it_left_off_when_not_using_filter() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameDefined nameDefined3 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(6), "name5");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));
        mongoEventStore.write("3", 0, serialize(nameDefined3));

        CountDownLatch cancelSubscriberLatch = new CountDownLatch(1);
        CountDownLatch waitUntilCancelled = new CountDownLatch(1);
        CountDownLatch waitUntilSecondEventProcessed = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
        new Thread(() -> {
            awaitLatch(cancelSubscriberLatch);
            subscription.shutdown();
            waitUntilCancelled.countDown();
        }).start();

        // When

        subscription.subscribe(subscriptionId, StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), e -> {
            if (state.size() < 2) {
                state.add(e);
            }

            if (state.size() == 2) {
                cancelSubscriberLatch.countDown();
                awaitLatch(waitUntilCancelled);
                waitUntilSecondEventProcessed.countDown();
            }
        }).waitUntilStarted();

        awaitLatch(waitUntilSecondEventProcessed);
        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));
        subscription.subscribe(subscriptionId, state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(state).hasSize(4).extracting(this::deserialize).containsExactly(nameDefined1, nameDefined2, nameDefined3, nameWasChanged1));
    }

    @Test
    void catchup_subscription_continues_where_it_left_off_when_using_filter() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameDefined nameDefined3 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(6), "name5");
        NameDefined nameDefined4 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("4", 0, serialize(nameDefined4));
        mongoEventStore.write("3", 0, serialize(nameDefined3));

        CountDownLatch cancelSubscriberLatch = new CountDownLatch(1);
        CountDownLatch waitUntilCancelled = new CountDownLatch(1);
        CountDownLatch waitUntilSecondEventProcessed = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
        new Thread(() -> {
            awaitLatch(cancelSubscriberLatch);
            subscription.shutdown();
            waitUntilCancelled.countDown();
        }).start();

        // When

        subscription.subscribe(subscriptionId, filter(type(NameDefined.class.getName())), e -> {
            if (state.size() < 2) {
                state.add(e);
            }

            if (state.size() == 2) {
                cancelSubscriberLatch.countDown();
                awaitLatch(waitUntilCancelled);
                waitUntilSecondEventProcessed.countDown();
            }
        }).waitUntilStarted();

        awaitLatch(waitUntilSecondEventProcessed);
        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));
        subscription.subscribe(subscriptionId, filter(type(NameDefined.class.getName())), state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(state).hasSize(4).extracting(this::deserialize).containsExactly(nameDefined1, nameDefined2, nameDefined3, nameDefined4));
    }

    @Test
    void catchup_subscription_continues_where_it_left_off_after_all_historic_events_have_been_consumed() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameDefined nameDefined3 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(6), "name5");
        NameDefined nameDefined4 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));

        CountDownLatch cancelSubscriberLatch = new CountDownLatch(1);
        CountDownLatch waitUntilCancelled = new CountDownLatch(1);
        CountDownLatch waitUntilSecondEventProcessed = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
        new Thread(() -> {
            awaitLatch(cancelSubscriberLatch);
            subscription.shutdown();
            waitUntilCancelled.countDown();
        }).start();

        Supplier<CatchupSubscriptionModel> catchupSupportingBlockingSubscription = () -> {
            subscriptionExecutor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel blockingSubscriptionForMongoDB = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.DATE, subscriptionExecutor, RetryStrategy.none());
            DurableSubscriptionModel blockingSubscriptionWithAutomaticPositionPersistence = new DurableSubscriptionModel(blockingSubscriptionForMongoDB, storage);
            return new CatchupSubscriptionModel(blockingSubscriptionWithAutomaticPositionPersistence, mongoEventStore, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage)));
        };

        subscription = catchupSupportingBlockingSubscription.get();

        // When
        subscription.subscribe(subscriptionId, filter(type(NameDefined.class.getName())), e -> {
            if (state.size() < 2) {
                state.add(e);
            }

            if (state.size() == 2) {
                cancelSubscriberLatch.countDown();
                awaitLatch(waitUntilCancelled);
                waitUntilSecondEventProcessed.countDown();
            }
        }).waitUntilStarted();

        awaitLatch(waitUntilSecondEventProcessed);
        subscription = catchupSupportingBlockingSubscription.get();
        subscription.subscribe(subscriptionId, filter(type(NameDefined.class.getName())), state::add).waitUntilStarted();

        mongoEventStore.write("4", 0, serialize(nameDefined4));
        mongoEventStore.write("3", 0, serialize(nameDefined3));

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(state).hasSize(4).extracting(this::deserialize).containsExactly(nameDefined1, nameDefined2, nameDefined4, nameDefined3));
    }

    @Test
    void catchup_subscription_restarts_from_beginning_of_time_when_position_is_not_persisted_during_catch_up_and_catch_up_fails() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameDefined nameDefined3 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(6), "name5");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));
        mongoEventStore.write("3", 0, serialize(nameDefined3));

        CountDownLatch cancelSubscriberLatch = new CountDownLatch(1);
        CountDownLatch waitUntilCancelled = new CountDownLatch(1);
        CountDownLatch waitUntilSecondEventProcessed = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        String subscriptionId = UUID.randomUUID().toString();
        new Thread(() -> {
            awaitLatch(cancelSubscriberLatch);
            subscription.shutdown();
            waitUntilCancelled.countDown();
        }).start();

        // When
        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, new CatchupSubscriptionModelConfig(100));
        subscription.subscribe(subscriptionId, StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), e -> {
            if (state.size() < 2) {
                state.add(e);
            }

            if (state.size() == 2) {
                cancelSubscriberLatch.countDown();
                awaitLatch(waitUntilCancelled);
                waitUntilSecondEventProcessed.countDown();
            }
        }).waitUntilStarted();

        awaitLatch(waitUntilSecondEventProcessed);
        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));
        subscription.subscribe(subscriptionId, state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                // Note that it's correct behavior that we expect 6 events here since the first subscription is not configured to store subscription position during catch-up => duplicates
                assertThat(state).hasSize(6).extracting(this::deserialize).containsExactly(nameDefined1, nameDefined2, nameDefined1, nameDefined2, nameDefined3, nameWasChanged1));
    }

    @Test
    void catchup_subscription_can_be_configured_to_only_store_the_position_of_every_tenth_event_when_persisting_catchup_subscription() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        for (int i = 0; i < 100; i++) {
            mongoEventStore.write(String.valueOf(i), 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusMinutes(i), "name" + i)));
        }

        AtomicInteger numberOfSavedPositions = new AtomicInteger();
        storage = new NativeMongoSubscriptionPositionStorage(database, "storage") {
            @Override
            public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
                numberOfSavedPositions.incrementAndGet();
                return super.save(subscriptionId, subscriptionPosition);
            }
        };

        subscription = newCatchupSubscription(database, eventCollection, TimeRepresentation.DATE, new CatchupSubscriptionModelConfig(100, useSubscriptionPositionStorage(this.storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(10)));

        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

        // When
        String subscriptionId = UUID.randomUUID().toString();
        subscription.subscribe(subscriptionId, StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime()), state::add).waitUntilStarted();

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(100));
        assertThat(numberOfSavedPositions).hasValue(11); // Store every 10th position equals 10 for 100 events + 1 additional save for global subscription position before switching to continuous mode
        assertThat(storage.read(subscriptionId)).isNotNull();
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("ConstantConditions")
    private DomainEvent deserialize(CloudEvent e) {
        try {
            return (DomainEvent) objectMapper.readValue(e.getData().toBytes(), Class.forName(e.getType()));
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
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

    private CatchupSubscriptionModel newCatchupSubscription(MongoDatabase database, MongoCollection<Document> eventCollection, TimeRepresentation timeRepresentation, CatchupSubscriptionModelConfig config) {
        subscriptionExecutor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel blockingSubscriptionForMongoDB = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none());
        return new CatchupSubscriptionModel(blockingSubscriptionForMongoDB, mongoEventStore, config);
    }
}