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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;

/**
 * A durable subscription on reactive MongoDB that matches few of the events written. The events it does not match move
 * the change stream on without an event to deliver, and the position that is then reported is what the durable model
 * saves as the checkpoint, so a restart does not read them again.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableMongoSubscriptionModelQuietPositionTest {

    private static final String DATABASE = "reactordurablemongoquietposition";
    private static final String SUBSCRIPTION_ID = "sub";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration QUIET_POSITION_IS_REPORTED_WITHIN = Duration.ofSeconds(30);
    private static final SubscriptionFilter MATCHING_ONLY = AgnosticSubscriptionFilter.filter(type("Matching"));

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static MongoClient mongoClient;

    // A collection of its own for every test, since a subscription started at the present can also receive what was
    // written up to 16 seconds before it
    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final String checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
    private final CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
    private final ReactiveMongoTemplate template;
    private final ReactorMongoEventStore eventStore;
    private final ReactorMongoSubscriptionModel mongoModel;
    private final ReactorCheckpointStorage checkpointStorage;
    private @Nullable ReactorDurableSubscriptionModel model;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    ReactorDurableMongoSubscriptionModelQuietPositionTest() {
        template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(timeRepresentation)
                .build();
        eventStore = new ReactorMongoEventStore(template, eventStoreConfig);
        mongoModel = new ReactorMongoSubscriptionModel(template, eventCollectionName, timeRepresentation);
        checkpointStorage = new ReactorCheckpointStorage(template, checkpointCollectionName);
    }

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
        template.remove(new Query(), eventCollectionName).block(TIMEOUT);
        template.remove(new Query(), checkpointCollectionName).block(TIMEOUT);
    }

    @Test
    void the_checkpoint_moves_past_the_last_delivered_event_when_only_events_that_do_not_match_are_written() {
        // Given
        model = new ReactorDurableSubscriptionModel(mongoModel, checkpointStorage, new ReactorDurableSubscriptionModelConfig(1).saveQuietPositionEvery(Duration.ofMillis(200)));
        Checkpoint positionOfTheMatchedEvent = subscribeAndDeliverAMatchingEvent(model);

        // When
        write("Other");

        // Then
        await().atMost(QUIET_POSITION_IS_REPORTED_WITHIN).untilAsserted(() -> assertThat(storedPosition()).as("checkpoint stored after only events that do not match were written").isNotEqualTo(positionOfTheMatchedEvent.asString()));
        assertThat(operationTimeOf(storedCheckpoint())).as("operation time of the checkpoint stored after the events that do not match").isGreaterThanOrEqualTo(operationTimeOf(positionOfTheMatchedEvent));
        assertThat(delivered).as("events delivered to the action").hasSize(1);
    }

    @Test
    void the_checkpoint_stays_at_the_last_delivered_event_when_the_quiet_position_is_never_saved() {
        // Given
        model = new ReactorDurableSubscriptionModel(mongoModel, checkpointStorage, new ReactorDurableSubscriptionModelConfig(1).neverSaveQuietPosition());
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        mongoModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> Mono.fromRunnable(() -> quietPositions.add(quietPosition))));
        Checkpoint positionOfTheMatchedEvent = subscribeAndDeliverAMatchingEvent(model);

        // When
        write("Other");

        // Then
        await().atMost(QUIET_POSITION_IS_REPORTED_WITHIN).untilAsserted(() -> assertThat(quietPositions).as("quiet positions the Mongo model reported").anyMatch(position -> !position.asString().equals(positionOfTheMatchedEvent.asString())));
        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> assertThat(storedPosition()).as("checkpoint stored after a quiet position was reported").isEqualTo(positionOfTheMatchedEvent.asString()));
    }

    private Checkpoint subscribeAndDeliverAMatchingEvent(ReactorDurableSubscriptionModel durable) {
        durable.subscribe(SUBSCRIPTION_ID, MATCHING_ONLY, StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> delivered.add(event))).waitUntilStarted(TIMEOUT).block();
        write("Matching");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).hasSize(1));
        Checkpoint position = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(delivered.getFirst());
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storedPosition()).as("checkpoint stored for the delivered event").isEqualTo(position.asString()));
        return position;
    }

    private void write(String type) {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write(UUID.randomUUID().toString(), Flux.just(event)).block(TIMEOUT);
    }

    private @Nullable String storedPosition() {
        return checkpointStorage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT);
    }

    private @Nullable Checkpoint storedCheckpoint() {
        return checkpointStorage.read(SUBSCRIPTION_ID).block(TIMEOUT);
    }

    // The first nine bytes of a resume token are its operation time, which orders positions in the stream
    private static String operationTimeOf(@Nullable Checkpoint checkpoint) {
        assertThat(checkpoint).isInstanceOf(MongoResumeTokenCheckpoint.class);
        return ((MongoResumeTokenCheckpoint) checkpoint).resumeToken.getString("_data").getValue().substring(0, 18);
    }
}
