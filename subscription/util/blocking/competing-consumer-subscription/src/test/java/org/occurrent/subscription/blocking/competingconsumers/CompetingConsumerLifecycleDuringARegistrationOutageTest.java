package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class CompetingConsumerLifecycleDuringARegistrationOutageTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient strategyClient;
    private MongoClient modelClient;
    private SpringMongoLeaseCompetingConsumerStrategy strategy;
    private CompetingConsumerSubscriptionModel model;
    private boolean paused;

    @AfterEach
    void unpauseAndShutdown() {
        if (paused) {
            mongo.getDockerClient().unpauseContainerCmd(mongo.getContainerId()).exec();
        }
        try {
            model.shutdown();
        } catch (RuntimeException ignored) {
            // Some tests have shut it down already
        }
        strategyClient.close();
        modelClient.close();
    }

    @Test
    void shutdown_returns_while_another_thread_subscribes_and_its_lease_registration_retries_through_an_outage() {
        // Given
        newModel();
        pauseTheDatabase();
        CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> model.subscribe("node", "subscription", null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).until(() -> !subscribe.isDone());

        // When
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown);

        // Then
        assertThat(shutdown).as("shutdown() while a subscribe retries its registration").succeedsWithin(Duration.ofSeconds(20));
        assertThat(subscribe).as("the subscribe that shutdown() overtook").failsWithin(Duration.ofSeconds(20));
    }

    @Test
    void stop_returns_while_another_thread_subscribes_and_the_subscription_waits_for_start_once_the_database_is_back() {
        // Given
        newModel();
        pauseTheDatabase();
        CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> model.subscribe("node", "subscription", null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).until(() -> !subscribe.isDone());

        // When
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop);

        // Then
        assertThat(stop).as("stop() while a subscribe retries its registration").succeedsWithin(Duration.ofSeconds(20));
        unpauseTheDatabase();
        assertThat(subscribe).as("the subscribe once the database is back").succeedsWithin(Duration.ofSeconds(60));
        assertThat(model.isRunning("subscription")).as("the subscription runs on the node stop() stopped").isFalse();
        assertThat(strategy.hasLock("subscription", "node")).as("the node stop() stopped holds the lease").isFalse();

        model.start(false);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(model.isRunning("subscription")).as("the subscription runs once the node is started").isTrue());
    }

    // The strategy's client gives up on an unreachable database within half a second, so registering retries many
    // times while the database is paused. The wrapped model keeps the default timeouts, since a read timeout shorter
    // than a change stream's wait for new events would keep its cursor busy for good.
    private void newModel() {
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        String database = requireNonNull(connectionString.getDatabase());
        strategyClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applyToClusterSettings(b -> b.serverSelectionTimeout(500, MILLISECONDS))
                .applyToSocketSettings(b -> b.connectTimeout(500, MILLISECONDS).readTimeout(500, MILLISECONDS))
                .build());
        modelClient = MongoClients.create(connectionString);
        strategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(new MongoTemplate(strategyClient, database)).leaseTime(Duration.ofSeconds(2)).collectionName("locks-" + UUID.randomUUID()).build();
        model = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(new MongoTemplate(modelClient, database), "events", TimeRepresentation.RFC_3339_STRING), strategy);
    }

    private void pauseTheDatabase() {
        mongo.getDockerClient().pauseContainerCmd(mongo.getContainerId()).exec();
        paused = true;
    }

    private void unpauseTheDatabase() {
        mongo.getDockerClient().unpauseContainerCmd(mongo.getContainerId()).exec();
        paused = false;
    }
}
