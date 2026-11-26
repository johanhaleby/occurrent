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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A node whose competing consumer model is stopped subscribes at an explicit checkpoint that the node holding the
 * lease before it stored, so the subscription stays paused and the stored checkpoint stays as it is. When
 * {@code start()} grants this node a newer lease, the plain resume stores this node's fencing token with that stored
 * checkpoint, so a delayed save by the old holder is refused before this node has saved a checkpoint of its own. Runs
 * against a MongoDB replica set in testcontainers, with {@link SpringMongoLeaseCompetingConsumerStrategy} and
 * {@link SpringMongoCheckpointStorage}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerResumeStoresThisNodesVersionOverSpringMongoTest {

    // Large enough that no scheduled refresh of a lease fires within the test
    private static final Duration LEASE_TIME = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private EventStore eventStore;
    private MongoTemplate mongoTemplate;
    private ObjectMapper objectMapper;
    private String streamId;
    private String locksCollection;
    private String checkpointCollection;

    private CompetingConsumerSubscriptionModel oldHolder;
    private CompetingConsumerSubscriptionModel thisNode;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
        streamId = UUID.randomUUID().toString();
        locksCollection = "competing-consumer-locks-" + UUID.randomUUID();
        checkpointCollection = "checkpoints-" + UUID.randomUUID();
    }

    @AfterEach
    void shutdown() {
        if (oldHolder != null) {
            oldHolder.shutdown();
        }
        if (thisNode != null) {
            thisNode.shutdown();
        }
    }

    @Test
    void a_save_by_the_old_holder_with_its_older_fencing_token_is_refused_after_the_start_resumes_a_subscription_made_while_stopped_at_the_checkpoint_the_old_holder_stored() {
        // Given an old holder that delivered an event and stored the checkpoint for it with its fencing token, and then gave the lease up
        String subscriptionId = UUID.randomUUID().toString();
        SpringMongoCheckpointStorage storageOfTheOldHolder = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        SpringMongoLeaseCompetingConsumerStrategy strategyOfTheOldHolder = strategy();
        oldHolder = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(springModel(), storageOfTheOldHolder, strategyOfTheOldHolder::fencingToken), strategyOfTheOldHolder);
        CopyOnWriteArrayList<CloudEvent> deliveredToTheOldHolder = new CopyOnWriteArrayList<>();
        oldHolder.subscribe(subscriptionId, deliveredToTheOldHolder::add).waitUntilStarted();
        eventStore.write(streamId, serialize(new NameDefined("seed", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed value")));
        await("the old holder delivers the event").atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToTheOldHolder).hasSize(1));
        OptionalLong tokenOfTheOldHolder = strategyOfTheOldHolder.fencingToken(subscriptionId);
        assertThat(tokenOfTheOldHolder).as("fencing token of the old holder").isPresent();
        // The action runs before the checkpoint of its event is saved, and the position stored before the event is an
        // operation time, so the checkpoint of the event is the stored one once it is a resume token with this version
        await("the old holder stores the checkpoint of the event with its fencing token").atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(storageOfTheOldHolder.read(subscriptionId)).as("stored checkpoint").isInstanceOf(MongoResumeTokenCheckpoint.class);
            assertThat(storageOfTheOldHolder.writeVersion(subscriptionId)).as("stored version").isEqualTo(tokenOfTheOldHolder);
        });
        oldHolder.pauseSubscription(subscriptionId);
        Checkpoint checkpointOfTheOldHolder = requireNonNull(storageOfTheOldHolder.read(subscriptionId));
        await("nothing of the old holder is saved after the pause").during(1, SECONDS).atMost(3, SECONDS).untilAsserted(() ->
                assertThat(storageOfTheOldHolder.read(subscriptionId)).as("stored checkpoint after the pause").isEqualTo(checkpointOfTheOldHolder));

        // and this node, stopped, subscribing at that checkpoint, so it stays paused with the checkpoint as it was
        SpringMongoCheckpointStorage storageOfThisNode = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        SpringMongoLeaseCompetingConsumerStrategy strategyOfThisNode = strategy();
        SpringMongoSubscriptionModel springModelOfThisNode = springModel();
        thisNode = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(springModelOfThisNode, storageOfThisNode, strategyOfThisNode::fencingToken), strategyOfThisNode);
        thisNode.stop();
        CopyOnWriteArrayList<CloudEvent> deliveredToThisNode = new CopyOnWriteArrayList<>();
        thisNode.subscribe("this-node", subscriptionId, null, StartAt.checkpoint(checkpointOfTheOldHolder), deliveredToThisNode::add);
        assertThat(springModelOfThisNode.isPaused(subscriptionId)).as("paused while this node is stopped").isTrue();
        assertThat(storageOfThisNode.read(subscriptionId)).as("stored checkpoint after the subscribe").isEqualTo(checkpointOfTheOldHolder);
        assertThat(storageOfThisNode.writeVersion(subscriptionId)).as("stored version after the subscribe").isEqualTo(tokenOfTheOldHolder);

        // When the start grants this node a newer lease and resumes the subscription, and the old holder's delayed save comes
        // before this node has delivered an event
        thisNode.start();
        await("this node resumes the subscription").atMost(10, SECONDS).untilAsserted(() -> assertThat(springModelOfThisNode.isRunning(subscriptionId)).isTrue());
        OptionalLong tokenOfThisNode = strategyOfThisNode.fencingToken(subscriptionId);
        Throwable saveOfTheOldHolder = catchThrowable(() -> storageOfTheOldHolder.save(subscriptionId, new StringBasedCheckpoint("old-holders-delayed-position"), CheckpointWriteCondition.notOlderThan(tokenOfTheOldHolder.getAsLong())));

        // Then
        assertAll(
                () -> assertThat(tokenOfThisNode).as("fencing token of this node").isPresent(),
                () -> assertThat(tokenOfThisNode.getAsLong()).as("fencing token of this node").isGreaterThan(tokenOfTheOldHolder.getAsLong()),
                () -> assertThat(saveOfTheOldHolder).as("save with the old holder's fencing token").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(requireNonNull(storageOfThisNode.read(subscriptionId)).asString()).as("stored checkpoint").isEqualTo(checkpointOfTheOldHolder.asString()),
                () -> assertThat(storageOfThisNode.writeVersion(subscriptionId)).as("stored version").isEqualTo(tokenOfThisNode),
                () -> assertThat(deliveredToThisNode).as("events delivered to this node").isEmpty()
        );
    }

    private SpringMongoLeaseCompetingConsumerStrategy strategy() {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events").getCollection()), TimeRepresentation.RFC_3339_STRING);
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
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
