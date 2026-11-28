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
import org.bson.Document;
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
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
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
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A node that delivers from the position it stored with its version, and another node whose start position is
 * earlier moves the stored position earlier. The node's next save of a delivered event is refused instead of storing a
 * position after the earlier one, and the competing consumer model replays from the stored position while the node
 * holds the lease, so the events between the two positions are delivered. Runs against a MongoDB replica set in
 * testcontainers, with {@link SpringMongoLeaseCompetingConsumerStrategy} and {@link SpringMongoCheckpointStorage}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerReplaysFromAStartPositionThatMovedTheStoredPositionEarlierOverSpringMongoTest {

    private static final String SUBSCRIBER_ID = "this-node";
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

    private SpringMongoSubscriptionModel springModel;
    private SpringMongoCheckpointStorage storage;
    private SpringMongoLeaseCompetingConsumerStrategy strategy;
    private DurableWithCountedPlainResumes durable;
    private CompetingConsumerSubscriptionModel model;

    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
    private final List<CheckpointWriteConditionNotFulfilledException> reported = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_the_node() {
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

        springModel = new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(connectionString.getCollection()), TimeRepresentation.RFC_3339_STRING);
        storage = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        strategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
        durable = new DurableWithCountedPlainResumes(springModel, storage, strategy);
        durable.addStartRefusalListener(new RecordingListener());
        model = new CompetingConsumerSubscriptionModel(durable, strategy);
    }

    @AfterEach
    void shutdown() {
        model.shutdown();
    }

    @Test
    void every_event_between_the_start_position_that_moved_the_stored_position_earlier_and_the_position_this_node_started_from_is_delivered() {
        // Given a position taken before three events are written, which another node's start uses
        String subscriptionId = UUID.randomUUID().toString();
        Checkpoint earlierStartPosition = requireNonNull(springModel.globalCheckpoint());
        eventStore.write(streamId, serialize(event("between-1")));
        eventStore.write(streamId, serialize(event("between-2")));
        eventStore.write(streamId, serialize(event("between-3")));
        // and a write that this subscription doesn't see, so the position this node starts from is after all three
        mongoTemplate.getCollection("another-collection-" + UUID.randomUUID()).insertOne(new Document("anything", "at all"));
        // and this node, which holds the lease and delivers from the position it stored with its fencing token
        Subscription subscription = model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), delivered::add);
        subscription.waitUntilStarted();
        OptionalLong tokenOfThisNode = strategy.fencingToken(subscriptionId);
        await("this node stores the position it starts from with its fencing token").atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(storage.read(subscriptionId)).as("stored checkpoint").isInstanceOf(MongoOperationTimeCheckpoint.class);
            assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEqualTo(tokenOfThisNode);
        });
        assertThat(tokenOfThisNode).as("fencing token of this node").isPresent();
        assertThat(delivered).as("events delivered before the start position of the other node is stored").isEmpty();

        // When the other node's start position, which is earlier, replaces the stored one, and an event is written
        Checkpoint storedByTheOtherNode = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection).resolveFirstCheckpointRace(subscriptionId, earlierStartPosition).orElseThrow();
        eventStore.write(streamId, serialize(event("after-the-move")));

        // Then
        await("every event from the earlier start position on is delivered").atMost(20, SECONDS).untilAsserted(() ->
                assertThat(idsOf(delivered)).as("ids of events delivered").contains("between-1", "between-2", "between-3", "after-the-move"));
        assertThat(storedByTheOtherNode.asString()).as("position the other node stored").isEqualTo(earlierStartPosition.asString());
    }

    @Test
    void a_save_refused_for_the_moved_stored_position_is_reported_and_the_subscription_is_resumed_through_the_plain_resume_while_this_node_stays_a_candidate_for_the_lease() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        Checkpoint earlierStartPosition = requireNonNull(springModel.globalCheckpoint());
        eventStore.write(streamId, serialize(event("between-1")));
        mongoTemplate.getCollection("another-collection-" + UUID.randomUUID()).insertOne(new Document("anything", "at all"));
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), delivered::add).waitUntilStarted();
        OptionalLong tokenOfThisNode = strategy.fencingToken(subscriptionId);
        await("this node stores the position it starts from with its fencing token").atMost(10, SECONDS).untilAsserted(() ->
                assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEqualTo(tokenOfThisNode));
        new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection).resolveFirstCheckpointRace(subscriptionId, earlierStartPosition).orElseThrow();

        // When
        eventStore.write(streamId, serialize(event("after-the-move")));
        await("the replay delivers the event between the positions").atMost(20, SECONDS).untilAsserted(() ->
                assertThat(idsOf(delivered)).as("ids of events delivered").contains("between-1", "after-the-move"));
        await("the replay stores a position of its own").atMost(10, SECONDS).untilAsserted(() ->
                assertThat(requireNonNull(storage.read(subscriptionId)).asString()).as("stored checkpoint").isNotEqualTo(earlierStartPosition.asString()));

        // Then
        assertAll(
                () -> assertThat(reported).as("refusals reported").isNotEmpty().allSatisfy(refusal -> {
                    assertThat(refusal.subscriptionId).as("subscription id of the refusal").isEqualTo(subscriptionId);
                    assertThat(refusal.condition).as("condition of the refused save").isEqualTo(CheckpointWriteCondition.notOlderThan(tokenOfThisNode.getAsLong()));
                    assertThat(refusal.storedVersion).as("stored version of the refusal").hasValue(tokenOfThisNode.getAsLong());
                }),
                () -> assertThat(durable.plainResumes).as("plain resumes of the durable model").hasValueGreaterThanOrEqualTo(1),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEqualTo(tokenOfThisNode),
                () -> assertThat(springModel.isRunning(subscriptionId)).as("running in the wrapped model").isTrue(),
                () -> assertThat(model.isRunning(subscriptionId)).as("running in the competing consumer model").isTrue(),
                () -> assertThat(strategy.hasLock(subscriptionId, SUBSCRIBER_ID)).as("this node holds the lease").isTrue()
        );
    }

    private static List<String> idsOf(List<CloudEvent> events) {
        return events.stream().map(CloudEvent::getId).toList();
    }

    private static NameDefined event(String id) {
        return new NameDefined(id, LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value of " + id);
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

    private final class RecordingListener implements StartRefusalListener {
        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
        }

        @Override
        public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
        }

        @Override
        public void handleStoredPositionMovedEarlier(String subscriptionId, CheckpointWriteConditionNotFulfilledException refusal) {
            reported.add(refusal);
        }
    }

    private static final class DurableWithCountedPlainResumes extends DurableSubscriptionModel {
        final AtomicInteger plainResumes = new AtomicInteger();

        DurableWithCountedPlainResumes(SpringMongoSubscriptionModel wrapped, SpringMongoCheckpointStorage storage, SpringMongoLeaseCompetingConsumerStrategy strategy) {
            super(wrapped, storage, strategy::fencingToken);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            plainResumes.incrementAndGet();
            return super.resumeSubscription(subscriptionId);
        }
    }
}
