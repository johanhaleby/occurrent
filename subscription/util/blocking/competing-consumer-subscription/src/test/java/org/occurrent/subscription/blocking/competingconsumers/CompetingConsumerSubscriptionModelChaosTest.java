package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@DisplayNameGeneration(ReplaceUnderscores.class)
@Disabled("Only for local testing and tweaking")
class CompetingConsumerSubscriptionModelChaosTest {
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModelChaosTest.class);

    private EventStore eventStore;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel1;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel2;
    private DurableSubscriptionModel springSubscriptionModel1;
    private DurableSubscriptionModel springSubscriptionModel2;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017/occurrent-test.events");
        log.info("Connecting to MongoDB at {}", connectionString);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder.maintenanceFrequency(4, SECONDS))
                .build();

        MongoClient mongoClient = MongoClients.create(settings);
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
    void chaos_competing_consumers() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<CloudEvent> cloudEvents = new CopyOnWriteArrayList<>();

        competingConsumerSubscriptionModel1 = new CompetingConsumerSubscriptionModel(springSubscriptionModel1, loggingStrategy("1", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));
        competingConsumerSubscriptionModel2 = new CompetingConsumerSubscriptionModel(springSubscriptionModel2, loggingStrategy("2", new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(Duration.ofSeconds(1)).build()));

        String subscriptionId = UUID.randomUUID().toString();
        competingConsumerSubscriptionModel1.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();
        competingConsumerSubscriptionModel2.subscribe(subscriptionId, cloudEvents::add).waitUntilStarted();

        Thread.sleep(1000);

        Retry retry = RetryStrategy
                .fixed(1000)
                .retryIf(not(DuplicateCloudEventException.class::isInstance))
                .onError(((info, throwable) -> log.warn("Detected error (retryCount={}). Error={} - {}. Backoff before retry={}",
                        info.getRetryCount(), throwable.getClass().getSimpleName(), throwable.getMessage(), info.getBackoffBeforeNextRetryAttempt().orElse(Duration.ZERO))));

        Thread eventPublishingThread = new Thread(() -> {
            // When
            for (int i = 0; i < 1000; i++) {
                String streamId = UUID.randomUUID().toString();

                NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name");
                NameWasChanged nameWasChanged = new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.of(2021, 2, 26, 14, 15, 16), "name", "my name changed");

                try {
                    retry.execute(() -> eventStore.write(streamId, serialize(nameDefined)));
                } catch (DuplicateCloudEventException ignored) {

                }
                try {
                    retry.execute(() -> eventStore.write(streamId, serialize(nameWasChanged)));
                } catch (DuplicateCloudEventException ignored) {
                }
            }
        });
        eventPublishingThread.start();

        final AtomicBoolean running = new AtomicBoolean(true);
        Thread chaosThread = new Thread(() -> {
            while (running.get()) {
                int millis = ThreadLocalRandom.current().nextInt(1500, 5000);
                sleep(millis);
                if (millis % 2 == 0) {
                    if (competingConsumerSubscriptionModel1.isRunning(subscriptionId)) {
                        pause(subscriptionId, competingConsumerSubscriptionModel1);
                    }
                    if (competingConsumerSubscriptionModel2.isPaused(subscriptionId)) {
                        resume(subscriptionId, competingConsumerSubscriptionModel2);
                    }
                } else {
                    if (competingConsumerSubscriptionModel2.isRunning(subscriptionId)) {
                        pause(subscriptionId, competingConsumerSubscriptionModel2);
                    }
                    if (competingConsumerSubscriptionModel1.isPaused(subscriptionId)) {
                        resume(subscriptionId, competingConsumerSubscriptionModel1);
                    }
                }
            }
        });
        chaosThread.start();


        // Then
        eventPublishingThread.join();
        await("waiting for second event").atMost(10, SECONDS).untilAsserted(() -> assertThat(cloudEvents.size()).isEqualTo(2000));
        running.set(false);
        chaosThread.join();
    }

    private void pause(String subscriptionId, CompetingConsumerSubscriptionModel subscriptionModel) {
        try {
            subscriptionModel.pauseSubscription(subscriptionId);
        } catch (Exception e) {
            log.error("pause error for subscriptionId {}", subscriptionId, e);
        }
    }

    private void resume(String subscriptionId, CompetingConsumerSubscriptionModel subscriptionModel) {
        try {
            subscriptionModel.resumeSubscription(subscriptionId);
        } catch (Exception e) {
            log.error("resume error for subscriptionId {}", subscriptionId, e);
        }
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
                log.info("[{}] Consuming granted for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                log.info("[{}] Consuming prohibited for subscription {} (subscriber={})", name, subscriptionId, subscriberId);
            }
        });
        return strategy;
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}