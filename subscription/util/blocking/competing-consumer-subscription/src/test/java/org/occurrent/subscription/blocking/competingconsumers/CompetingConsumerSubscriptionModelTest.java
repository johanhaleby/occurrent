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
        if (competingConsumerSubscriptionModel1 != null) {
            competingConsumerSubscriptionModel1.shutdown();
        }
        if (competingConsumerSubscriptionModel2 != null) {
            competingConsumerSubscriptionModel2.shutdown();
        }
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

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        // Then
        Thread.sleep(1000);
        assertThat(cloudEvents).hasSize(1);
    }

    @Test
    void another_consumer_takes_over_when_first_subscription_is_explicitly_paused_by_user() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(2));
    }

    @Test
    void same_consumer_can_resume_when_subscription_is_paused_by_system_because_consumption_prohibited() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        SpringMongoLeaseCompetingConsumerStrategy springMongoLeaseCompetingConsumerStrategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build();
        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", springMongoLeaseCompetingConsumerStrategy));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", springMongoLeaseCompetingConsumerStrategy));

        String subscriberId1 = "Subscriber1";
        String subscriberId2 = "Subscriber2";
        String subscriptionId = "MySubscription";
        competingConsumerSubscriptionModel1.subscribe(subscriberId1, subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriberId2, subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        // Here we simulate that consumption has been prohibited, i.e. the subscriber has lost the lock to the subscription.
        // It should regain it again automatically after one second though (because "leaseTime" is set to 1 second so the
        // competing consumer will try to regain lock after this amount of time if it's lost).
        springMongoLeaseCompetingConsumerStrategy.releaseCompetingConsumer(subscriptionId, subscriberId1);
        competingConsumerSubscriptionModel1.onConsumeProhibited(subscriptionId, subscriberId1);

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

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name1");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");

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
    void can_pause_and_resume_same_subscription() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name1");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged1));

        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId);

        eventStore.write("streamId", serialize(nameWasChanged2));

        // Then
        await("waiting for second event").atMost(5, SECONDS).untilAsserted(() -> assertThat(cloudEvents).extracting(CloudEvent::getId).containsExactly("1", "2", "3"));
    }

    @RepeatedIfExceptionsTest(repeats = 3, suspend = 400)
    void another_consumer_takes_over_when_first_subscription_is_canceled() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        SpringMongoLeaseCompetingConsumerStrategy springMongoLeaseCompetingConsumerStrategy2 = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build();
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", springMongoLeaseCompetingConsumerStrategy2));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe("subscriber1", subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe("subscriber2", subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name1");
        NameWasChanged nameWasChanged2 = new NameWasChanged("eventId3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "name", "my name2");

        // When
        eventStore.write("streamId", serialize(nameDefined));
        await("waiting for first event").atMost(2, SECONDS).untilAsserted(() -> assertThat(cloudEvents).hasSize(1));

        competingConsumerSubscriptionModel1.cancelSubscription(subscriptionId);

        // Cancelling a subscription also removes the subscription position from storage, thus this event will be lost!
        // However, cancelSubscription is async because of Spring, this is why we have the @RepeatedIfExceptionsTest annotation here.
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

        NameDefined nameDefined = new NameDefined("eventId1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged = new NameWasChanged("eventId2", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

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

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "name", "my name3");

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

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "name", "my name3");

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

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "name", "my name3");
        NameWasChanged nameWasChanged3 = new NameWasChanged("4", LocalDateTime.of(2021, 2, 26, 14, 15, 19), "name", "my name4");
        NameWasChanged nameWasChanged4 = new NameWasChanged("5", LocalDateTime.of(2021, 2, 26, 14, 15, 20), "name", "my name5");

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
        competingConsumerSubscriptionModel1.subscribe("1", subscriptionId, null, StartAt.subscriptionModelDefault(), e -> cloudEvents.add(tuple("1", e))).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe("2", subscriptionId, null, StartAt.subscriptionModelDefault(), e -> cloudEvents.add(tuple("2", e))).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("1", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        NameWasChanged nameWasChanged1 = new NameWasChanged("2", LocalDateTime.of(2021, 2, 26, 14, 15, 17), "name", "my name2");
        NameWasChanged nameWasChanged2 = new NameWasChanged("3", LocalDateTime.of(2021, 2, 26, 14, 15, 18), "name", "my name3");
        NameWasChanged nameWasChanged3 = new NameWasChanged("4", LocalDateTime.of(2021, 2, 26, 14, 15, 19), "name", "my name4");
        NameWasChanged nameWasChanged4 = new NameWasChanged("5", LocalDateTime.of(2021, 2, 26, 14, 15, 20), "name", "my name5");


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

    @Test
    void when_CompetingConsumerSubscriptionModel_is_blocked_by_StartAt_then_it_will_delegate_to_parent() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", mongoTemplate));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", mongoTemplate));

        String subscriptionId = UUID.randomUUID().toString();
        // Here we tell that the CompetingConsumerSubscriptionModel should not be used
        StartAt dynamic = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, dynamic, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, dynamic, cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        eventStore.write("streamId", serialize(nameDefined));

        // Then
        await().untilAsserted(() -> assertThat(cloudEvents).hasSize(2));
    }

    @Test
    void it_is_possible_to_stop_and_start_a_CompetingConsumerSubscriptionModel_when_some_subscriptions_are_blocked() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEventsSubscription1 = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> cloudEventsSubscription2 = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", mongoTemplate));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", mongoTemplate));

        String subscriptionId1 = UUID.randomUUID().toString();
        String subscriptionId2 = UUID.randomUUID().toString();
        // Here we tell that the CompetingConsumerSubscriptionModel should not be used
        StartAt dynamic = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        competingConsumerSubscriptionModel1.subscribe(subscriptionId1, dynamic, cloudEventsSubscription1::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId1, dynamic, cloudEventsSubscription1::add).waitUntilStarted();

        competingConsumerSubscriptionModel1.subscribe(subscriptionId2, cloudEventsSubscription2::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId2, cloudEventsSubscription2::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        competingConsumerSubscriptionModel1.stop();
        competingConsumerSubscriptionModel2.stop();

        competingConsumerSubscriptionModel1.start();
        competingConsumerSubscriptionModel2.start();

        eventStore.write("streamId", serialize(nameDefined));

        // Then
        await().untilAsserted(() -> assertThat(cloudEventsSubscription1).hasSize(2));
        await().untilAsserted(() -> assertThat(cloudEventsSubscription2).hasSize(1));
    }

    @Test
    void it_is_possible_to_pause_and_resume_a_CompetingConsumerSubscriptionModel_when_some_subscriptions_are_blocked() {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEventsSubscription1 = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> cloudEventsSubscription2 = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", mongoTemplate));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", mongoTemplate));

        String subscriptionId1 = UUID.randomUUID().toString();
        String subscriptionId2 = UUID.randomUUID().toString();
        // Here we tell that the CompetingConsumerSubscriptionModel should not be used
        StartAt dynamic = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        competingConsumerSubscriptionModel1.subscribe(subscriptionId1, dynamic, cloudEventsSubscription1::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId1, dynamic, cloudEventsSubscription1::add).waitUntilStarted();

        competingConsumerSubscriptionModel1.subscribe(subscriptionId2, cloudEventsSubscription2::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId2, cloudEventsSubscription2::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");

        // When
        competingConsumerSubscriptionModel1.pauseSubscription(subscriptionId1);
        competingConsumerSubscriptionModel2.pauseSubscription(subscriptionId2);

        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId1);
        competingConsumerSubscriptionModel2.resumeSubscription(subscriptionId2);

        eventStore.write("streamId", serialize(nameDefined));

        // Then
        await().untilAsserted(() -> assertThat(cloudEventsSubscription1).hasSize(2));
        await().untilAsserted(() -> assertThat(cloudEventsSubscription2).hasSize(1));
    }

    @Test
    void only_one_consumer_receives_event_when_starting543543() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", mongoTemplate));

        String subscriptionId = "MySubscription";
        String subsciberId = "subsciberId";
        competingConsumerSubscriptionModel1.subscribe(subsciberId, subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvents::add).waitUntilStarted();

        NameDefined nameDefined = new NameDefined("eventId", LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
        CompetingConsumerSubscriptionModel.SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = new CompetingConsumerSubscriptionModel.SubscriptionIdAndSubscriberId(subscriptionId, subsciberId);
        CompetingConsumerSubscriptionModel.CompetingConsumer competingConsumer = new CompetingConsumerSubscriptionModel.CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerSubscriptionModel.CompetingConsumerState.Running());

        // When
        competingConsumerSubscriptionModel1.onConsumeProhibited(subscriptionId, subsciberId);
//        competingConsumerSubscriptionModel1.pauseConsumer(competingConsumer, false);
        competingConsumerSubscriptionModel1.resumeSubscription(subscriptionId);
        eventStore.write("streamId", serialize(nameDefined));

        // Then
        Thread.sleep(1000);
        assertThat(cloudEvents).hasSize(1);
    }


    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
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
                log.info("[{}] Consumption granted for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                log.info("[{}] Consumption prohibited for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }
        });
        return strategy;
    }
}