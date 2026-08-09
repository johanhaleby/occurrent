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
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Occurrent #668: a competing consumer that regains its lease used to resume its own delegate's tracked position,
 * not the checkpoint, so it redelivered everything the interim holder had already handled and could write the
 * stored checkpoint backward. Two "nodes" are two model-and-strategy stacks against one Mongo container in this
 * one JVM, the pattern {@link CheckpointFenceLeaseTransferTest} uses, but the lease handover itself is driven
 * directly through {@link CompetingConsumerStrategy.CompetingConsumerListener#onConsumeProhibited(String, String)}
 * and {@code onConsumeGranted} rather than a real lease's expiry and a scheduled refresh, so which node holds the
 * lease at every point in the test is exact rather than raced.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseRegainResumesFromCheckpointTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private String streamId;
    private String checkpointCollection;
    private DeterministicCompetingConsumerStrategy strategy;

    private CompetingConsumerSubscriptionModel nodeA;
    private CompetingConsumerSubscriptionModel nodeB;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        streamId = UUID.randomUUID().toString();
        checkpointCollection = "checkpoints-" + UUID.randomUUID();
        strategy = new DeterministicCompetingConsumerStrategy();
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
    void a_lease_regain_resumes_from_the_interim_holders_checkpoint() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> eventsA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> eventsB = new CopyOnWriteArrayList<>();
        SpringMongoCheckpointStorage checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        DurableSubscriptionModel durableA = new DurableSubscriptionModel(springModel(), checkpointStorage);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(springModel(), checkpointStorage);
        nodeA = new CompetingConsumerSubscriptionModel(durableA, strategy);
        nodeB = new CompetingConsumerSubscriptionModel(durableB, strategy);

        nodeA.subscribe("A", subscriptionId, null, StartAt.subscriptionModelDefault(), eventsA::add).waitUntilStarted();

        NameDefined seed = new NameDefined("e1", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed");
        eventStore.write(streamId, serialize(seed));
        await("A delivers the seed event").atMost(5, SECONDS).untilAsserted(() -> assertThat(eventsA).hasSize(1));

        // When
        // A's lease is handed to B, deterministically. No lease expiry, no scheduled refresh, just the two
        // listener calls the strategy would otherwise make on its own schedule.
        strategy.transferLease(subscriptionId, "A", "B");
        nodeB.subscribe("B", subscriptionId, null, StartAt.subscriptionModelDefault(), eventsB::add).waitUntilStarted();

        NameWasChanged e2 = new NameWasChanged("e2", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "e2");
        NameWasChanged e3 = new NameWasChanged("e3", LocalDateTime.of(2026, 1, 1, 0, 0, 2), "name", "e3");
        eventStore.write(streamId, serialize(e2));
        eventStore.write(streamId, serialize(e3));
        await("B delivers e2 and e3").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(eventsB).extracting(CloudEvent::getId).contains("e2", "e3"));
        // B's own final checkpoint, the position A must resume from once it regains the lease, and the value the
        // stored checkpoint must never move away from afterward.
        Checkpoint checkpointAfterB = requireNonNull(checkpointStorage.read(subscriptionId));

        // B's lease is handed back to A, the regain #668 is about.
        strategy.transferLease(subscriptionId, "B", "A");

        // Then
        // A never redelivers e2 or e3, and the stored checkpoint never regresses from what B left it at. during(),
        // not a single poll, since a regression landing between polls would otherwise go unseen. This fails today
        // on both halves. Without the fix A resumes its own delegate's stale position (right after e1) and
        // redelivers e2 and e3, moving the stored checkpoint backward as it does.
        await("A never redelivers e2 or e3, and the checkpoint never regresses from B's final value").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> {
                    assertThat(eventsA).extracting(CloudEvent::getId).doesNotContain("e2", "e3");
                    assertThat(checkpointStorage.read(subscriptionId)).isEqualTo(checkpointAfterB);
                });
    }

    @Test
    void a_consumer_with_no_checkpoint_yet_still_receives_what_was_published_while_stood_down() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> eventsA = new CopyOnWriteArrayList<>();
        SpringMongoCheckpointStorage checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        SpringMongoSubscriptionModel springModelA = springModel();
        DurableSubscriptionModel durableA = new DurableSubscriptionModel(springModelA, checkpointStorage);
        nodeA = new CompetingConsumerSubscriptionModel(durableA, strategy);

        // An explicit, concrete starting position rather than the model default. A default would make
        // DurableSubscriptionModel seed checkpointStorage immediately from the global checkpoint, at subscribe
        // time, before anything is delivered, which is not the case this test is about. Nothing has been
        // checkpointed, by anybody, when the regain below happens. The delegate's own tracked position starts
        // here instead, and never advances further, since nothing is read before A stands down.
        Checkpoint startingPosition = requireNonNull(springModelA.globalCheckpoint());
        nodeA.subscribe("A", subscriptionId, null, StartAt.checkpoint(startingPosition), eventsA::add).waitUntilStarted();
        assertThat(checkpointStorage.exists(subscriptionId)).as("nothing has been delivered yet, so nothing has been checkpointed").isFalse();

        // When
        // A stands down before it ever delivers anything, an event is published while it is down, and A is the
        // only one that ever picks the subscription back up.
        strategy.loseLease(subscriptionId, "A");
        assertThat(checkpointStorage.exists(subscriptionId)).as("standing down before any delivery does not write a checkpoint either").isFalse();

        NameDefined whileStoodDown = new NameDefined("e1", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "published while stood down");
        eventStore.write(streamId, serialize(whileStoodDown));

        strategy.grantLease(subscriptionId, "A");

        // Then
        // DurableSubscriptionModel.resumeSubscription finds nothing in checkpointStorage and falls back to the
        // delegate's own resumeSubscription, which still holds the concrete starting position above, never
        // StartAt.subscriptionModelDefault(), which would resolve to the present at resume time and skip the
        // event entirely (the guard this fallback exists for).
        await("A still receives what was published while it was stood down, despite never having checkpointed anything")
                .atMost(5, SECONDS).untilAsserted(() -> assertThat(eventsA).extracting(CloudEvent::getId).contains("e1"));
    }

    @Test
    void a_lease_regain_does_not_replay_history_through_the_catch_up_model() {
        // Given the starter's own composition, CompetingConsumer(Catchup(Durable(SpringMongo))).
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> eventsA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> eventsB = new CopyOnWriteArrayList<>();
        SpringMongoCheckpointStorage checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, checkpointCollection);
        DurableSubscriptionModel durableA = new DurableSubscriptionModel(springModel(), checkpointStorage);
        DurableSubscriptionModel durableB = new DurableSubscriptionModel(springModel(), checkpointStorage);
        CatchupSubscriptionModel catchupA = new CatchupSubscriptionModel(durableA, eventStore);
        CatchupSubscriptionModel catchupB = new CatchupSubscriptionModel(durableB, eventStore);
        nodeA = new CompetingConsumerSubscriptionModel(catchupA, strategy);
        nodeB = new CompetingConsumerSubscriptionModel(catchupB, strategy);

        nodeA.subscribe("A", subscriptionId, null, StartAt.subscriptionModelDefault(), eventsA::add).waitUntilStarted();

        NameDefined seed = new NameDefined("e1", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed");
        eventStore.write(streamId, serialize(seed));
        await("A delivers the seed event").atMost(5, SECONDS).untilAsserted(() -> assertThat(eventsA).hasSize(1));

        // When
        strategy.transferLease(subscriptionId, "A", "B");
        nodeB.subscribe("B", subscriptionId, null, StartAt.subscriptionModelDefault(), eventsB::add).waitUntilStarted();

        NameWasChanged e2 = new NameWasChanged("e2", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "e2");
        NameWasChanged e3 = new NameWasChanged("e3", LocalDateTime.of(2026, 1, 1, 0, 0, 2), "name", "e3");
        eventStore.write(streamId, serialize(e2));
        eventStore.write(streamId, serialize(e3));
        await("B delivers e2 and e3").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(eventsB).extracting(CloudEvent::getId).contains("e2", "e3"));

        strategy.transferLease(subscriptionId, "B", "A");

        NameWasChanged e4 = new NameWasChanged("e4", LocalDateTime.of(2026, 1, 1, 0, 0, 3), "name", "e4");
        eventStore.write(streamId, serialize(e4));

        // Then, the regain reaches DurableSubscriptionModel directly, since CatchupSubscriptionModel's
        // resumeSubscription is a plain forward that never routes through a catch-up child, so isCatchingUp stays
        // false throughout and neither e1 (already delivered before the handover) nor e2/e3 (delivered by B) is
        // redelivered through a replay. e4, published after the regain, still arrives normally.
        await("A delivers e4 without redelivering e2 or e3").atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(eventsA).extracting(CloudEvent::getId).contains("e4"));
        assertThat(eventsA).extracting(CloudEvent::getId).doesNotContain("e2", "e3");
        await("catch-up is never re-triggered by the regain").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(catchupA.isCatchingUp(subscriptionId)).isFalse());
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
                .withData(unchecked(OBJECT_MAPPER::writeValueAsBytes).apply(e))
                .build());
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Drives lease handover with two direct listener calls instead of a real lease's expiry and scheduled refresh,
     * so a test using it controls exactly when each side finds out, with no race to await. The lock itself is
     * tracked only so {@link CompetingConsumerStrategy#hasLock(String, String)} answers consistently with the
     * transitions this strategy has already told its listeners about.
     */
    private static final class DeterministicCompetingConsumerStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> lockHolder = new HashMap<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String currentHolder = lockHolder.get(subscriptionId);
            boolean acquired = currentHolder == null || currentHolder.equals(subscriberId);
            if (acquired) {
                lockHolder.put(subscriptionId, subscriberId);
            }
            return acquired;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            lockHolder.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            lockHolder.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return subscriberId.equals(lockHolder.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
            listeners.remove(listener);
        }

        /**
         * Tells {@code subscriberId} its consumption is prohibited, with no register call and no lock change of
         * its own; the lock stays whatever it already was, exactly like an expired lease that nobody has taken
         * over yet.
         */
        void loseLease(String subscriptionId, String subscriberId) {
            listeners.forEach(l -> l.onConsumeProhibited(subscriptionId, subscriberId));
        }

        /**
         * Grants the lock to {@code subscriberId} and tells every listener, exactly like a lease a strategy's own
         * refresh thread just handed out with no register call from the model.
         */
        void grantLease(String subscriptionId, String subscriberId) {
            lockHolder.put(subscriptionId, subscriberId);
            listeners.forEach(l -> l.onConsumeGranted(subscriptionId, subscriberId));
        }

        /**
         * The handover this test suite is about: {@code oldHolder} loses the lease and {@code newHolder} is
         * granted it, in that order, with nothing in between.
         */
        void transferLease(String subscriptionId, String oldHolder, String newHolder) {
            loseLease(subscriptionId, oldHolder);
            grantLease(subscriptionId, newHolder);
        }
    }
}
