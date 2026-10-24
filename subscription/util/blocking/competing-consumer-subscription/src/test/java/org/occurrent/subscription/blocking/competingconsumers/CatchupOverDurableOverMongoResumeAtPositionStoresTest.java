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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
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
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
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
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * #1253 over a real MongoDB. A resume at a position through the catch-up model of the stack the Spring Boot starter
 * builds below the competing consumer, a catch-up model over a durable model over the MongoDB model with one checkpoint
 * storage shared by the two, stores the position before the MongoDB model resumes there. The position is stored before
 * any event arrives after the resume, and a restart that follows resumes from it.
 */
@Testcontainers
@DisplayName("CatchupOverDurableOverMongoResumeAtPositionStores")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupOverDurableOverMongoResumeAtPositionStoresTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private String streamId;
    private String subscriptionId;
    private SpringMongoCheckpointStorage checkpointStorage;
    private SpringMongoSubscriptionModel mongoModel;
    private CatchupSubscriptionModel catchup;
    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_the_stack() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        streamId = UUID.randomUUID().toString();
        subscriptionId = UUID.randomUUID().toString();
        checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints-" + UUID.randomUUID());
        mongoModel = springModel();
        catchup = catchupOver(mongoModel);
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
    }

    @Test
    @Timeout(60)
    void a_resume_at_a_position_stores_it_before_any_event_arrives_and_delivery_continues_from_it() {
        // Given a running subscription that has delivered e1, paused, with two events written while it was paused
        catchup.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), delivered::add).waitUntilStarted();
        eventStore.write(streamId, serialize(new NameDefined("e1", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed")));
        await("e1 is delivered and its checkpoint stored").atMost(5, SECONDS).untilAsserted(() -> assertThat(ids()).containsExactly("e1"));
        await("e1's checkpoint is stored").atMost(5, SECONDS).until(() -> checkpointStorage.exists(subscriptionId));
        Checkpoint storedBeforeTheResume = requireNonNull(checkpointStorage.read(subscriptionId));
        catchup.pauseSubscription(subscriptionId);
        eventStore.write(streamId, serialize(new NameWasChanged("while-paused-1", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "one")));
        eventStore.write(streamId, serialize(new NameWasChanged("while-paused-2", LocalDateTime.of(2026, 1, 1, 0, 0, 2), "name", "two")));
        Checkpoint position = requireNonNull(mongoModel.globalCheckpoint());

        // When the subscription is resumed at a position past both, through the lookup on the catch-up model
        RepositionableSubscriptions.findIn(catchup).orElseThrow().resumeSubscription(subscriptionId, StartAt.checkpoint(position));

        // Then the position is stored before any new event, and a new event is delivered without the two written while paused
        assertAll(
                () -> assertThat(checkpointStorage.read(subscriptionId)).as("stored at the resume").isEqualTo(position),
                () -> assertThat(position).as("a position other than the one stored before").isNotEqualTo(storedBeforeTheResume)
        );
        eventStore.write(streamId, serialize(new NameWasChanged("e4", LocalDateTime.of(2026, 1, 1, 0, 0, 3), "name", "after")));
        await("e4 is delivered").atMost(5, SECONDS).untilAsserted(() -> assertThat(ids()).contains("e4"));
        assertThat(ids()).as("delivered").containsExactly("e1", "e4");
    }

    @Test
    @Timeout(60)
    void a_restart_after_a_resume_at_a_position_continues_from_that_position() {
        // Given a subscription resumed at a position past two events written while it was paused
        catchup.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), delivered::add).waitUntilStarted();
        eventStore.write(streamId, serialize(new NameDefined("e1", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed")));
        await("e1 is delivered").atMost(5, SECONDS).untilAsserted(() -> assertThat(ids()).containsExactly("e1"));
        await("e1's checkpoint is stored").atMost(5, SECONDS).until(() -> checkpointStorage.exists(subscriptionId));
        catchup.pauseSubscription(subscriptionId);
        eventStore.write(streamId, serialize(new NameWasChanged("while-paused-1", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "one")));
        eventStore.write(streamId, serialize(new NameWasChanged("while-paused-2", LocalDateTime.of(2026, 1, 1, 0, 0, 2), "name", "two")));
        Checkpoint position = requireNonNull(mongoModel.globalCheckpoint());
        RepositionableSubscriptions.findIn(catchup).orElseThrow().resumeSubscription(subscriptionId, StartAt.checkpoint(position));

        // When the application restarts before another event, and an event is written while it is down
        catchup.shutdown();
        eventStore.write(streamId, serialize(new NameWasChanged("while-down", LocalDateTime.of(2026, 1, 1, 0, 0, 3), "name", "down")));
        List<CloudEvent> deliveredAfterTheRestart = new CopyOnWriteArrayList<>();
        catchup = catchupOver(springModel());
        catchup.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), deliveredAfterTheRestart::add).waitUntilStarted();

        // Then it resumes from the position, so the event written while it was down is delivered and the two written while paused are not
        await("the event written while down is delivered").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(deliveredAfterTheRestart).extracting(CloudEvent::getId).contains("while-down"));
        assertThat(deliveredAfterTheRestart).extracting(CloudEvent::getId).as("delivered after the restart").containsExactly("while-down");
    }

    private List<String> ids() {
        return delivered.stream().map(CloudEvent::getId).toList();
    }

    // The catch-up model over the durable model over the MongoDB model, the three sharing one checkpoint storage, as the Spring Boot starter builds them
    private CatchupSubscriptionModel catchupOver(SpringMongoSubscriptionModel mongo) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(mongo, checkpointStorage);
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(useCheckpointStorage(checkpointStorage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1000));
        return new CatchupSubscriptionModel(durable, eventStore, config);
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events").getCollection()), TimeRepresentation.RFC_3339_STRING);
    }

    private static List<CloudEvent> serialize(DomainEvent e) {
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
}
