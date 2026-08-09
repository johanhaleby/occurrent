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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.HoldableSpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * The checkpoint fence (ADR 116, #665) proved end to end over a real MongoDB replica set, as one system. A real
 * {@link SpringMongoLeaseCompetingConsumerStrategy} lease, a real fencing token, a real
 * {@link CompetingConsumerSubscriptionModel} wiring {@code strategy::fencingToken} into a real
 * {@link DurableSubscriptionModel}, and a real conditional write against a real
 * {@link org.occurrent.subscription.api.blocking.CheckpointStorage}. Two "nodes" are two model-and-strategy stacks
 * against one Mongo container in this one JVM, the pattern {@code CompetingConsumerSubscriptionModelTest} and
 * {@code MongoLeaseRaceTest} both use.
 * <p>
 * Covers the first two scenarios of the epic's end-to-end proof. The first is an expired-lease takeover, where the
 * old holder is still delivering and has to be refused rather than trusted to notice. The second is a graceful
 * handover, where the old holder gives its lease up on purpose.
 * <p>
 * Scenario 3 lives in {@link CheckpointFenceIsolatesOtherSubscriptionsTest}, in its own class because it needs the
 * native stack this module normally has no reason to depend on, and its mutation proof reverts
 * {@code NativeMongoSubscriptionModel}'s retry exclusion. Scenario 4, the Spring Boot starter's registrar-driven
 * wiring, lives in {@code framework/spring-boot-starter-mongodb} because it needs a Spring application context.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointFenceLeaseTransferTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointFenceLeaseTransferTest.class);
    // Large enough that neither strategy's own scheduled refresh fires within a test's lifetime, so every state
    // change here is caused by the direct Mongo writes and explicit calls below, not a background race with the
    // refresh thread. That background behavior is scenario 3's subject, not this one's.
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

    private CompetingConsumerSubscriptionModel nodeA;
    private CompetingConsumerSubscriptionModel nodeB;
    private SpringMongoLeaseCompetingConsumerStrategy strategyA;
    private SpringMongoLeaseCompetingConsumerStrategy strategyB;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
        streamId = UUID.randomUUID().toString();
        // Collections of their own per test, the same isolation CompetingConsumerSubscriptionModelFixture uses. A
        // leftover lease or checkpoint from one test must never answer for another's subscription id.
        locksCollection = "competing-consumer-locks-" + UUID.randomUUID();
        checkpointCollection = "checkpoints-" + UUID.randomUUID();
    }

    @AfterEach
    void shutdown() {
        if (nodeA != null) {
            nodeA.shutdown();
        }
        if (nodeB != null) {
            nodeB.shutdown();
        }
    }

    @Test
    void expired_lease_takeover_refuses_the_stale_holders_write_and_the_new_holder_redelivers() {
        // Given
        // Node A wins the (uncontested) lease and delivers a seed event, so its tracked change-stream position
        // concretizes on a real delivery before the failure scenario runs (a resume or restart before that point
        // would resolve to "now" and skip straight past the event this test is about).
        CopyOnWriteArrayList<CloudEvent> eventsA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> eventsB = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        HoldableSpringMongoCheckpointStorage checkpointStorageA = new HoldableSpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        SpringMongoCheckpointStorage checkpointStorageB = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
        strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
        DurableSubscriptionModel durableA = new DurableSubscriptionModel(springModel(), checkpointStorageA, strategyA::fencingToken);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(springModel(), checkpointStorageB, strategyB::fencingToken);
        nodeA = new CompetingConsumerSubscriptionModel(durableA, strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(durableB, strategyB);

        nodeA.subscribe(subscriptionId, eventsA::add).waitUntilStarted();

        NameDefined seed = new NameDefined("seed", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed value");
        eventStore.write(streamId, serialize(seed));
        await("node A delivers the seed event").atMost(5, SECONDS).untilAsserted(() -> assertThat(eventsA).hasSize(1));

        OptionalLong tokenA = strategyA.fencingToken(subscriptionId);
        assertThat(tokenA).as("the fence must actually be active for this proof to mean anything, since a missed wiring site answers empty and stamps any()").isPresent();

        // When
        // A's lease expires on the database clock (ADR 114), out of band, so A never notices from inside its own
        // process. Node B then takes the lease over. Registering while it looks expired to Mongo wins it
        // synchronously, without waiting on anyone's scheduled refresh.
        expireLeaseFor(subscriptionId);
        nodeB.subscribe(subscriptionId, eventsB::add).waitUntilStarted();

        OptionalLong tokenB = strategyB.fencingToken(subscriptionId);
        assertThat(tokenB).isPresent();
        assertThat(tokenB.getAsLong()).as("a genuine takeover raises the fencing token").isGreaterThan(tokenA.getAsLong());

        // A new event now reaches both A's change stream, open all along and still unaware, and B's, freshly opened
        // from the seed checkpoint. A's checkpoint write for it is held so B's write is guaranteed to land first,
        // modeling the race ADR 116 names explicitly rather than hoping for one interleaving over the other.
        checkpointStorageA.armHold();
        NameWasChanged staleEvent = new NameWasChanged("stale-write", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "changed while A was stale");
        eventStore.write(streamId, serialize(staleEvent));

        checkpointStorageA.awaitHeldWriteArrived();
        await("node B redelivers/delivers the event under its own, higher token").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(eventsB).extracting(CloudEvent::getId).contains(staleEvent.eventId()));
        await("B's checkpoint write lands and is stamped with B's token").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(checkpointStorageB.writeVersion(subscriptionId)).isEqualTo(tokenB));

        // Then
        // A's held write is released. It is now guaranteed to be refused, since the stored version already exceeds
        // the stale token A is still offering.
        checkpointStorageA.release();

        // The stored checkpoint never moves backward. It stays at B's version well past the moment A's refused
        // write lands. during(), not a single poll, since a redelivered-back regression would show up between polls.
        await("the stored checkpoint never regresses to A's stale token").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(checkpointStorageB.writeVersion(subscriptionId)).isEqualTo(tokenB));

        // At-least-once holds. Every event that was published was processed by somebody, none lost. A's own
        // delivery of the stale event (its handler runs before the checkpoint write that later gets refused, ADR
        // 116's "the event is not acknowledged" is about the position, not the handler) plus B's redelivery both
        // count.
        assertThat(Stream.concat(eventsA.stream(), eventsB.stream()).map(CloudEvent::getId).distinct())
                .as("at-least-once, nothing published is missing from what somebody delivered")
                .contains(seed.eventId(), staleEvent.eventId());
    }

    @Test
    void graceful_handover_increments_the_version_and_refuses_a_late_write_from_the_old_token() {
        // Given
        // Node A wins the (uncontested) lease and delivers a seed event.
        CopyOnWriteArrayList<CloudEvent> eventsA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> eventsB = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        SpringMongoCheckpointStorage checkpointStorageA = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        SpringMongoCheckpointStorage checkpointStorageB = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
        strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).leaseTime(LEASE_TIME).collectionName(locksCollection).build();
        DurableSubscriptionModel durableA = new DurableSubscriptionModel(springModel(), checkpointStorageA, strategyA::fencingToken);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(springModel(), checkpointStorageB, strategyB::fencingToken);
        nodeA = new CompetingConsumerSubscriptionModel(durableA, strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(durableB, strategyB);

        nodeA.subscribe(subscriptionId, eventsA::add).waitUntilStarted();

        NameDefined seed = new NameDefined("seed", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed value");
        eventStore.write(streamId, serialize(seed));
        await("node A delivers the seed event").atMost(5, SECONDS).untilAsserted(() -> assertThat(eventsA).hasSize(1));

        OptionalLong tokenA = strategyA.fencingToken(subscriptionId);
        assertThat(tokenA).isPresent();
        // The write A's own model made for the seed event, kept so this test can play it back late, standing in for
        // a write A's process had already issued before pausing that only reaches MongoDB afterwards.
        Checkpoint staleCheckpoint = requireNonNull(checkpointStorageA.read(subscriptionId));

        // When
        // Node A pauses (a user pause unregisters, see CompetingConsumerSubscriptionModel.pauseSubscription), which
        // releases the lease through the unset-not-delete path PR 672 put in place rather than deleting the lock
        // document.
        nodeA.pauseSubscription(subscriptionId);

        Document lockDocumentAfterPause = requireNonNull(mongoTemplate.getCollection(locksCollection).find(eq("_id", subscriptionId)).first());
        assertThat(lockDocumentAfterPause.containsKey("subscriberId"))
                .as("released, not deleted, so subscriberId is unset rather than the document being gone")
                .isFalse();
        assertThat(lockDocumentAfterPause.get("version"))
                .as("the version survives the release, which is what keeps it a fencing token rather than a counter that resets")
                .isNotNull();

        // Node B acquires the now-free lease and its write is accepted.
        nodeB.subscribe(subscriptionId, eventsB::add).waitUntilStarted();
        OptionalLong tokenB = strategyB.fencingToken(subscriptionId);
        assertThat(tokenB).isPresent();
        assertThat(tokenB.getAsLong()).as("a genuine handover raises the fencing token").isGreaterThan(tokenA.getAsLong());

        NameWasChanged afterHandover = new NameWasChanged("after-handover", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "changed after handover");
        eventStore.write(streamId, serialize(afterHandover));
        await("node B delivers the event under its own, accepted token").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(eventsB).extracting(CloudEvent::getId).contains(afterHandover.eventId()));
        await("B's write is accepted and stamped with B's token").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(checkpointStorageB.writeVersion(subscriptionId)).isEqualTo(tokenB));

        // Then
        // A late write still carrying A's old token, arriving after B has already moved the stored version forward,
        // is refused.
        Throwable lateWrite = catchThrowable(() -> checkpointStorageA.save(subscriptionId, staleCheckpoint, CheckpointWriteCondition.notOlderThan(tokenA.getAsLong())));
        assertThat(lateWrite).isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(checkpointStorageB.writeVersion(subscriptionId))
                .as("the refused late write must not have moved the stored checkpoint backward")
                .isEqualTo(tokenB);
    }

    /**
     * Writes {@code expiresAt} on the subscription's lock document directly, so it looks expired to the database's
     * own clock, which ADR 114 makes production code judge it against, without moving anything or waiting on
     * anyone's scheduled refresh in this process. Same technique {@code MongoLeaseRaceTest} uses.
     */
    private void expireLeaseFor(String subscriptionId) {
        mongoTemplate.getCollection(locksCollection).updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minusSeconds(2)));
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
