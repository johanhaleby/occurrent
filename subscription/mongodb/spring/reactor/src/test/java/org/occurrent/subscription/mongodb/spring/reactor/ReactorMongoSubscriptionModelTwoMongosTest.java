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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscription;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A subscription on a sharded cluster that the client reaches through two {@code mongos}. A cursor lives on the
 * {@code mongos} that opened it, so every {@code getMore} of a change stream has to go back to that one, and a client that
 * is free to pick either gets a cursor that doesn't exist from the other. The cluster is a config server, one shard and
 * two {@code mongos} in one container, which takes a while to start.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelTwoMongosTest {

    private static final int FIRST_MONGOS_PORT = 27017;
    private static final int SECOND_MONGOS_PORT = 27020;
    private static final SubscriptionFilter NAME_DEFINED_ONLY = AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));
    // Long enough for a client that picks a mongos for every getMore to have picked the wrong one
    private static final Duration OBSERVATION = Duration.ofSeconds(10);

    @Container
    private static final GenericContainer<?> shardedCluster = new GenericContainer<>("mongo:8.0")
            .withCopyFileToContainer(MountableFile.forClasspathResource("sharded-two-mongos/start.sh"), "/start.sh")
            .withCreateContainerCmdModifier(command -> command.withEntrypoint("bash", "/start.sh"))
            .withExposedPorts(FIRST_MONGOS_PORT, SECOND_MONGOS_PORT)
            .waitingFor(Wait.forLogMessage(".*SHARDED CLUSTER READY.*", 1).withStartupTimeout(Duration.ofMinutes(5)));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MongoClient writerClient;
    private MongoClient subscriberClient;
    private CommandLog commands;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createSubscriptionModel() {
        String databaseName = "twomongos" + UUID.randomUUID().toString().replace("-", "");
        // Either mongos can be picked for any command the client sends, since both are within the local threshold
        ConnectionString connectionString = new ConnectionString("mongodb://%1$s:%2$d,%1$s:%3$d/?localThresholdMS=1000".formatted(shardedCluster.getHost(), shardedCluster.getMappedPort(FIRST_MONGOS_PORT), shardedCluster.getMappedPort(SECOND_MONGOS_PORT)));
        writerClient = MongoClients.create(connectionString);
        commands = new CommandLog();
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(commands).build());
        ReactiveTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(writerClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        ReactiveMongoTemplate writerTemplate = new ReactiveMongoTemplate(writerClient, databaseName);
        writerTemplate.createCollection("events").block();
        mongoEventStore = new ReactorMongoEventStore(writerTemplate, eventStoreConfig);
        subscriptionModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, databaseName), "events", TimeRepresentation.RFC_3339_STRING);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        subscriberClient.close();
        writerClient.close();
    }

    @Test
    void a_subscription_sends_no_getMore_that_fails() {
        // Given
        subscriptionModel.subscribe("sharded", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty());

        // When
        write(nameDefined());

        // Then
        await().during(OBSERVATION).atMost(OBSERVATION.plusSeconds(10)).untilAsserted(() -> assertThat(commands.failed("getMore")).as("getMore that failed").isEmpty());
        assertThat(commands.named("getMore")).as("getMore sent").isNotEmpty();
    }

    @Test
    void a_subscription_opens_its_change_stream_once() {
        // Given
        waitUntilStarted(subscriptionModel.subscribe("sharded", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));

        // When
        write(nameDefined());

        // Then
        await().during(OBSERVATION).atMost(OBSERVATION.plusSeconds(10)).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSize(1));
    }

    @Test
    void a_subscription_that_matches_nothing_reports_a_quiet_position() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> Mono.fromRunnable(() -> quietPositions.add(quietPosition))));
        waitUntilStarted(subscriptionModel.subscribe("sharded", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));

        // When
        write(nameWasChanged());

        // Then
        await().atMost(60, SECONDS).untilAsserted(() -> assertThat(quietPositions).as("quiet positions reported").isNotEmpty());
    }

    @Test
    void a_subscription_delivers_an_event_it_matches() {
        // Given
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("sharded", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));

        // When
        NameDefined matched = nameDefined();
        write(matched);

        // Then
        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(matched.eventId()));
    }

    private static void waitUntilStarted(Subscription subscription) {
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(30)).block()).as("subscription %s started", subscription.id()).isTrue();
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
    }

    private NameWasChanged nameWasChanged() {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
    }

    private void write(DomainEvent event) {
        mongoEventStore.write(UUID.randomUUID().toString(), 0, serialize(event)).block();
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
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
