package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelTest {
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModelTest.class);

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private EventStore eventStore;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel1;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel2;
    private DurableSubscriptionModel springSubscriptionModel1;
    private DurableSubscriptionModel springSubscriptionModel2;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        log.info("Connecting to MongoDB at {}", connectionString);
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        SpringMongoSubscriptionPositionStorage positionStorage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, "positions");
        springSubscriptionModel1 = new DurableSubscriptionModel(new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), timeRepresentation), positionStorage);
        springSubscriptionModel2 = new DurableSubscriptionModel(new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), timeRepresentation), positionStorage);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        competingConsumerSubscriptionModel1.shutdown();
        competingConsumerSubscriptionModel2.shutdown();
        springSubscriptionModel1.shutdown();
        springSubscriptionModel2.shutdown();
    }

    @Test
    void only_one_consumer_receives_event_when_starting() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", mongoTemplate));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", mongoTemplate));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        // Then
        Thread.sleep(1000);
        assertThat(cloudEvents).hasSize(1);
    }

    @Test
    void another_consumer_takes_over_when_first_subscription_is_paused() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(2));
    }

    @Test
    void can_pause_and_resume_subscription_that_is_in_waiting_state() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted(); // Subscription in SM2 is now in waiting state

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name1");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name2");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        // SM2 subscription is in waiting state, but we still want to allow pause!
        competingConsumerSubscriptionModel2.pauseSubscription(subscriptionId);
        competingConsumerSubscriptionModel1.stop();

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel2.resumeSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged2));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).extracting(CloudEvent::getId).containsExactly("1", "2", "3"));
    }

    @Test
    void another_consumer_takes_over_when_first_subscription_is_canceled() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        SpringMongoLeaseCompetingConsumerStrategy springMongoLeaseCompetingConsumerStrategy2 = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build();
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", springMongoLeaseCompetingConsumerStrategy2));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe("subscriber1", subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe("subscriber2", subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name1");
        NameWasChanged nameWasChanged2 = new NameWasChanged("eventId3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "my name2");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.cancelSubscription(subscriptionId);

        // Cancelling a subscription also removes the subscription position from storage, thus this event will be lost!
        eventStore.write("streamId", serialize(nameWasChanged1));

        await().atMost(2, SECONDS).untilAsserted(() -> assertThat(springMongoLeaseCompetingConsumerStrategy2.hasLock(subscriptionId, "subscriber2")).isTrue());

        eventStore.write("streamId", serialize(nameWasChanged2));

        // Then
        await("waiting for third event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).extracting(CloudEvent::getId).containsExactly("eventId1", "eventId3"));
    }

    @Test
    void another_consumer_takes_over_when_first_subscription_model_is_stopped() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.stop();

        eventStore.write("streamId", serialize(nameWasChanged));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(2));
    }

    @Test
    void stopping_and_starting_both_competing_subscription_models_when_sm_2_is_started_before_sm1() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "my name3");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        competingConsumerSubscriptionModel1.stop();
        competingConsumerSubscriptionModel2.stop();

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel2.start();
        competingConsumerSubscriptionModel1.start();

        eventStore.write("streamId", serialize(nameWasChanged2));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).extracting(CloudEvent::getId).containsExactly("1", "2", "3"));
    }

    @Test
    void stopping_and_starting_both_competing_subscription_models_when_sm_1_is_started_before_sm2() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "my name3");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        competingConsumerSubscriptionModel1.stop();
        competingConsumerSubscriptionModel2.stop();

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel1.start();
        competingConsumerSubscriptionModel2.start();

        eventStore.write("streamId", serialize(nameWasChanged2));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).extracting(CloudEvent::getId).containsExactly("1", "2", "3"));
    }

    @RepeatedIfExceptionsTest(repeats = 3, suspend = 400)
    @Timeout(10)
    void stopping_and_starting_both_competing_subscription_models_several_times() {

        // Given
        CopyOnWriteArrayList<Tuple> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, e -> {
            cloudEvents.add(tuple("1", e));
        }).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, e -> {
            cloudEvents.add(tuple("2", e));
        }).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "my name3");
        NameWasChanged nameWasChanged3 = new NameWasChanged("4", LocalDateTime.of(2021, 2, 26, 14, 15, 19), "my name4");
        NameWasChanged nameWasChanged4 = new NameWasChanged("5", LocalDateTime.of(2021, 2, 26, 14, 15, 20), "my name5");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        competingConsumerSubscriptionModel1.stop();

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel1.start();

        eventStore.write("streamId", serialize(nameWasChanged2));

        competingConsumerSubscriptionModel2.stop();
        competingConsumerSubscriptionModel1.stop();

        eventStore.write("streamId", serialize(nameWasChanged3));

        competingConsumerSubscriptionModel2.start();
        competingConsumerSubscriptionModel1.start();

        eventStore.write("streamId", serialize(nameWasChanged4));

        competingConsumerSubscriptionModel2.stop();
        competingConsumerSubscriptionModel1.stop();

        competingConsumerSubscriptionModel1.start();
        competingConsumerSubscriptionModel2.start();

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents.stream().map(t -> ((CloudEvent) t.toArray()[1]).getId())).containsExactly("1", "2", "3", "4", "5"));
    }

    // Note that pausing a subscription is async when using the SpringMongoSubscriptionModel.
    // This means that if we resume a subscription fast enough (i.e. right after we've called pause), there's a chance that the
    // "action" has not yet saved the position of the cloud event to the subscription position storage. This means that "resume" may
    // start from an earlier event (the one previously written to the subscription position storage) and there may be duplicate events.
    // This is why we're using awaitility so much in this test (to wait for the action to have written the cloud event position to storage)
    @Timeout(20)
    @RepeatedIfExceptionsTest(repeats = 3, suspend = 500)
    void pausing_and_resuming_both_competing_subscription_models_several_times() {

        // Given
        CopyOnWriteArrayList<Tuple> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, e -> {
            cloudEvents.add(tuple("1", e));
        }).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, e -> {
            cloudEvents.add(tuple("2", e));
        }).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "my name3");
        NameWasChanged nameWasChanged3 = new NameWasChanged("4", LocalDateTime.of(2021, 2, 26, 14, 15, 19), "my name4");
        NameWasChanged nameWasChanged4 = new NameWasChanged("5", LocalDateTime.of(2021, 2, 26, 14, 15, 20), "my name5");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId).waitUntilStarted();

        eventStore.write("streamId", serialize(nameWasChanged2));
        await("waiting for third event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(3));

        competingConsumerSubscriptionModel2.pauseSubscription(subscriptionId);
        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged3));

        competingConsumerSubscriptionModel2.resumeSubscription(subscriptionId).waitUntilStarted();
        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId).waitUntilStarted();
        await("waiting for fourth event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(4));

        eventStore.write("streamId", serialize(nameWasChanged4));

        competingConsumerSubscriptionModel2.pauseSubscription(subscriptionId);

        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId).waitUntilStarted();
        competingConsumerSubscriptionModel2.resumeSubscription(subscriptionId).waitUntilStarted();

        // Then
        await("waiting for all events").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents.stream().map(t -> ((CloudEvent) t.toArray()[1]).getId())).containsExactly("1", "2", "3", "4", "5"));
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

    private static SpringMongoLeaseCompetingConsumerStrategy loggingStrategy(String name, MongoTemplate mongoTemplate) {
        return loggingStrategy(name, SpringMongoLeaseCompetingConsumerStrategy.withDefaults(mongoTemplate));
    }

    private static SpringMongoLeaseCompetingConsumerStrategy loggingStrategy(String name, SpringMongoLeaseCompetingConsumerStrategy strategy) {
        strategy.addListener(new CompetingConsumerStrategy.CompetingConsumerListener() {
            @Override
            public void onConsumeGranted(String subscriptionId, String subscriberId) {
                log.info("[{}] Consuming granted for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                log.info("[{}] Consuming prohibited for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }
        });
        return strategy;
    }
}