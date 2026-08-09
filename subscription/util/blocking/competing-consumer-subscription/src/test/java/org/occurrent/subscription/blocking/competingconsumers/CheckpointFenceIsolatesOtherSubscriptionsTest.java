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
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.nativedriver.blocking.HoldableNativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Scenario 3 of the checkpoint fence's end-to-end proof (ADR 116, #665): "the hard rule" from the ADR's
 * Consequences section, checked against a live node rather than trusted. Two subscriptions share one
 * {@link CompetingConsumerSubscriptionModel} and one {@link NativeMongoLeaseCompetingConsumerStrategy}, standing in
 * for one node. One of them has its lease stolen and its checkpoint write refused. The other must keep delivering
 * and its lease must keep refreshing, undisturbed, because a refusal thrown on a delivery thread must never reach
 * the lease refresh thread that serves both.
 * <p>
 * This is the native stack, not the Spring one the sibling {@link CheckpointFenceLeaseTransferTest} uses, because
 * this scenario's mutation proof reverts {@code NativeMongoSubscriptionModel}'s retry exclusion specifically.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class CheckpointFenceIsolatesOtherSubscriptionsTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointFenceIsolatesOtherSubscriptionsTest.class);
    // Shorter than the sibling test's 30 seconds, unlike it, because this scenario's assertion needs healthySub's
    // lease to actually cycle a few times inside the await budgets below. Not shorter than this, because node's own
    // scheduled refresh (every LEASE_TIME/2, unsynchronized with anything this test does) has to stay comfortably
    // behind the test's own steal-and-arm sequence, which runs in low tens of milliseconds. A shorter lease made
    // that a real race, not a hoped-for one. Refresh sometimes noticed refusedSub's stolen lease and paused it
    // before this test ever got to write the event the hold is armed for, which is a different, uninteresting
    // failure to what this test is about. Generous await budgets on top (not tight ones, which is what flaked a
    // low-lease-time Awaitility test elsewhere this week) absorb CI's own slowness rather than a tight timeout
    // doing that job.
    private static final Duration LEASE_TIME = Duration.ofSeconds(6);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoEventStore eventStore;
    private ObjectMapper objectMapper;
    private ExecutorService dispatcherNode;
    private ExecutorService dispatcherRival;
    private CompetingConsumerSubscriptionModel node;
    private CompetingConsumerSubscriptionModel rival;
    private String locksCollection;
    private String checkpointCollectionName;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), requireNonNull(connectionString.getCollection()), new EventStoreConfig(timeRepresentation));
        objectMapper = new ObjectMapper();
        // Tolerant of exactly the refusal this test provokes on purpose: ADR 116 has it reach "an executor's
        // uncaught handler" once logged, on refusedSub's dispatcher thread, and Awaitility otherwise catches any
        // thread's uncaught exception and rethrows it into whichever await() is polling at the time, turning this
        // test's own expected, provoked escape into a spurious failure (same reasoning and same fix as
        // NativeMongoSubscriptionModelResilienceTest's CheckpointWriteRefusalTest).
        dispatcherNode = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((t, throwable) -> {
                if (!(throwable instanceof org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException)) {
                    throw new AssertionError("Unexpected uncaught exception on subscription dispatcher thread " + t, throwable);
                }
            });
            return thread;
        });
        dispatcherRival = Executors.newCachedThreadPool();
        locksCollection = "competing-consumer-locks-" + UUID.randomUUID();
        checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
    }

    @AfterEach
    void shutdown() {
        if (node != null) {
            node.shutdown();
        }
        if (rival != null) {
            rival.shutdown();
        }
        ExecutorShutdown.shutdownSafely(dispatcherNode, 5, TimeUnit.SECONDS);
        ExecutorShutdown.shutdownSafely(dispatcherRival, 5, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_refused_write_on_one_subscription_never_stops_lease_refresh_or_delivery_for_another_on_the_same_node() {
        // Given
        // One node running two subscriptions, refusedSub and healthySub, sharing one strategy and one checkpoint
        // storage, the way two subscriptions on one process genuinely do.
        String refusedSub = "refused-" + UUID.randomUUID();
        String healthySub = "healthy-" + UUID.randomUUID();
        String refusedStream = UUID.randomUUID().toString();
        String healthyStream = UUID.randomUUID().toString();
        String nodeSubscriberId = "node-subscriber";
        CopyOnWriteArrayList<CloudEvent> refusedEvents = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> healthyEvents = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> rivalEvents = new CopyOnWriteArrayList<>();
        // Counts invocations for the one event whose checkpoint write is refused, so this test also proves the
        // mutation ADR 116 names outright. Excluding the refusal from retry is what keeps the node from re-running
        // this action's side effects forever, which is scenario 3's mutation target
        // (NativeMongoSubscriptionModel.RETRYABLE).
        java.util.concurrent.atomic.AtomicInteger targetRefusedInvocations = new java.util.concurrent.atomic.AtomicInteger();

        HoldableNativeMongoCheckpointStorage checkpointStorageNode = new HoldableNativeMongoCheckpointStorage(database.getCollection(checkpointCollectionName));
        NativeMongoCheckpointStorage checkpointStorageRival = new NativeMongoCheckpointStorage(database, checkpointCollectionName);
        NativeMongoLeaseCompetingConsumerStrategy strategyNode = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locksCollection).leaseTime(LEASE_TIME).build();
        NativeMongoLeaseCompetingConsumerStrategy strategyRival = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locksCollection).leaseTime(LEASE_TIME).build();

        DurableSubscriptionModel durableNode = new DurableSubscriptionModel(nativeModel(dispatcherNode), checkpointStorageNode, strategyNode::fencingToken);
        DurableSubscriptionModel durableRival = new DurableSubscriptionModel(nativeModel(dispatcherRival), checkpointStorageRival, strategyRival::fencingToken);
        node = new CompetingConsumerSubscriptionModel(durableNode, strategyNode);
        rival = new CompetingConsumerSubscriptionModel(durableRival, strategyRival);

        // Each filtered to its own stream. The native model applies no filter at all when none is given, so without
        // this both subscriptions would see every event either one publishes.
        node.subscribe(nodeSubscriberId, refusedSub, StreamSubscriptionFilter.filter(Filter.streamId(refusedStream)), org.occurrent.subscription.StartAt.subscriptionModelDefault(), event -> {
            if (event.getId().equals("target-refused")) {
                targetRefusedInvocations.incrementAndGet();
            }
            refusedEvents.add(event);
        }).waitUntilStarted();
        node.subscribe(nodeSubscriberId, healthySub, StreamSubscriptionFilter.filter(Filter.streamId(healthyStream)), org.occurrent.subscription.StartAt.subscriptionModelDefault(), healthyEvents::add).waitUntilStarted();

        NameDefined seedRefused = new NameDefined("seed-refused", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed");
        NameDefined seedHealthy = new NameDefined("seed-healthy", LocalDateTime.of(2026, 1, 1, 0, 0), "name", "seed");
        eventStore.write(refusedStream, serialize(seedRefused));
        eventStore.write(healthyStream, serialize(seedHealthy));
        await("both subscriptions deliver their seed event, so their tracked change-stream position concretizes before the failure scenario runs").atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(refusedEvents).hasSize(1);
                    assertThat(healthyEvents).hasSize(1);
                });
        // The action running is not the same as the checkpoint save completing: DurableSubscriptionModel calls the
        // action first and only then saves, on the same thread but not atomically with the assertion above. Waiting
        // for the save too, not only the action, is what keeps the hold armed below from catching the seed event's
        // own (still in-flight) write instead of the target event's.
        await("the seed checkpoint save itself has completed, not only the action that precedes it").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(checkpointStorageNode.writeVersion(refusedSub)).isPresent());

        // When
        // RefusedSub's lease is stolen by a rival, the same way CheckpointFenceLeaseTransferTest steals one, while
        // healthySub is never contested by anybody.
        expireLeaseFor(refusedSub);
        rival.subscribe("rival-subscriber", refusedSub, StreamSubscriptionFilter.filter(Filter.streamId(refusedStream)), org.occurrent.subscription.StartAt.subscriptionModelDefault(), rivalEvents::add).waitUntilStarted();

        // A new event on refusedSub reaches both node (still unaware) and rival. Node's write is held so rival's
        // write, offering the higher token, is guaranteed to land first.
        checkpointStorageNode.armHold(refusedSub);
        NameWasChanged targetRefused = new NameWasChanged("target-refused", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "changed while node was stale");
        eventStore.write(refusedStream, serialize(targetRefused));
        checkpointStorageNode.awaitHeldWriteArrived();
        await("rival redelivers/delivers the event under its own, higher token").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(rivalEvents).extracting(CloudEvent::getId).contains(targetRefused.eventId()));
        OptionalLong rivalToken = strategyRival.fencingToken(refusedSub);
        assertThat(rivalToken).isPresent();
        await("rival's write lands and is stamped with rival's token").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(checkpointStorageRival.writeVersion(refusedSub)).isEqualTo(rivalToken));
        // Node's held write is released. It is now guaranteed to be refused, since the stored version already
        // exceeds the stale token node is still offering.
        checkpointStorageNode.release();

        // The mutation target. The refusal must never be retried, so the action that threw it runs exactly once.
        // A reverted exclusion retries it forever instead, re-running this action's side effects every backoff
        // interval, which during() catches even though a single poll could land between retries and miss it.
        await("the refused action is invoked exactly once, never retried").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(targetRefusedInvocations.get()).isEqualTo(1));
        await("and stays exactly once well past what a retry backoff would allow").during(3, SECONDS).atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(targetRefusedInvocations.get()).isEqualTo(1));

        // Then
        // The hard rule. HealthySub is never affected. It keeps delivering, and a sniper that spends the same
        // window trying to steal it must never succeed, which is the strongest available proof that its lease kept
        // refreshing rather than merely that nobody happened to look at the wrong moment.
        NameWasChanged healthyFollowUp = new NameWasChanged("healthy-follow-up", LocalDateTime.of(2026, 1, 1, 0, 0, 1), "name", "still healthy");
        eventStore.write(healthyStream, serialize(healthyFollowUp));
        await("healthySub keeps delivering, unaffected by refusedSub's refusal").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(healthyEvents).extracting(CloudEvent::getId).contains(healthyFollowUp.eventId()));

        NativeMongoLeaseCompetingConsumerStrategy sniperStrategy = new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, locksCollection).leaseTime(LEASE_TIME).build();
        try {
            // Spans several lease periods (LEASE_TIME=6s, refreshed every 3s, window=15s is 4-5 cycles), generous
            // atMost so CI slowness fails loud rather than flaking, during() so it re-checks continuously rather
            // than trusting one truthy poll (pollDelay().atMost() would pass on the first sniper attempt regardless
            // of what happens for the rest of the window).
            await("healthySub's lease never expires long enough for a sniper to take it, across several lease periods")
                    .atMost(30, SECONDS).during(15, SECONDS)
                    .untilAsserted(() -> assertThat(sniperStrategy.registerCompetingConsumer(healthySub, "sniper")).isFalse());
        } finally {
            sniperStrategy.shutdown();
        }

        assertThat(node.isRunning(healthySub)).as("healthySub must still be the node's own, undisturbed").isTrue();

        // The ADR's other requirement on the refused side is that the subscription stays known and pausable rather
        // than forgotten, so the strategy's own refresh (which detects the loss independently of any of the above)
        // can pause it within a couple of lease periods once it notices.
        await("refusedSub becomes known-paused once the strategy's refresh notices the lease is gone").atMost(20, SECONDS)
                .untilAsserted(() -> assertThat(node.isPaused(refusedSub)).isTrue());
    }

    /**
     * Writes {@code expiresAt} on the subscription's lock document directly, so it looks expired to the database's
     * own clock (ADR 114), without moving anything or waiting on anyone's scheduled refresh in this process. Same
     * technique {@code MongoLeaseRaceTest} uses.
     */
    private void expireLeaseFor(String subscriptionId) {
        database.getCollection(locksCollection).updateOne(eq("_id", subscriptionId), set("expiresAt", Instant.now().minusSeconds(2)));
    }

    private NativeMongoSubscriptionModel nativeModel(ExecutorService dispatcher) {
        return new NativeMongoSubscriptionModel(database, requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events").getCollection()), TimeRepresentation.RFC_3339_STRING, dispatcher);
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
