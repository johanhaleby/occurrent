/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.reactor.durable;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A subscribe that takes over the delete a cancel of the same id started writes its positions on the condition
 * {@code notOlderThan(v + 2)}, where {@code v} is the version the delete read. The MongoDB checkpoint storage refuses
 * such a save while another node's start position has moved the stored position earlier, and
 * {@code ReactorMongoSubscriptionModel} runs the action again for as long as the save is refused. The model saves the
 * position again with no condition when the refusal is for that mark, which removes the mark and keeps the stored
 * version, so the delete is still fenced off. A refusal for a newer stored version is not for the mark, and is left
 * alone.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactorDurableMarkedCheckpointAfterDeleteTakeoverMongoTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String DATABASE = "reactormarkedcheckpointdeletetakeover";
    private static final String SUBSCRIPTION_ID = "sub";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static MongoClient mongoClient;
    private static String databaseName;

    private ReactiveMongoTemplate reactiveMongoTemplate;
    private String eventCollectionName;
    private String checkpointCollectionName;
    private ReactorMongoEventStore eventStore;
    private ReactorCheckpointStorage storage;
    private HoldingDeleteStorage holdingDeleteStorage;
    private ReactorDurableSubscriptionModel model;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void create_the_node() {
        eventCollectionName = "events-" + UUID.randomUUID();
        checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
        reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        eventStore = eventStore();
        storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        holdingDeleteStorage = new HoldingDeleteStorage(storage);
        model = new ReactorDurableSubscriptionModel(new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING), holdingDeleteStorage);
    }

    @AfterEach
    void release_the_delete_and_shut_down() {
        holdingDeleteStorage.release();
        model.shutdown();
    }

    @Test
    void a_save_refused_for_a_start_position_that_moved_the_stored_position_earlier_after_a_delete_takeover_is_saved_without_a_condition_and_still_fenced_off_from_the_delete() {
        // Given
        long v = subscribedAgainWhileTheDeleteIsHeld();
        List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), recording(delivered)).waitUntilStarted().block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.writeVersion(SUBSCRIPTION_ID).block(TIMEOUT)).as("the write version once the subscribe that took the delete over has written its start").isEqualTo(v + 2));
        BsonTimestamp earlier = new BsonTimestamp(1, 1);

        // When
        Checkpoint governing = storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, new MongoOperationTimeCheckpoint(earlier)).block(TIMEOUT);
        Document marked = storedDocument();
        assertThat(governing).as("the checkpoint that governs after another node's start at an operation time earlier than the first position").isEqualTo(new MongoOperationTimeCheckpoint(earlier));
        assertThat(marked.getBoolean("positionMovedEarlier")).as("the mark in %s", marked).isTrue();
        assertThat(((Number) marked.get("version")).longValue()).as("the version stored with the mark in %s", marked).isEqualTo(v + 2);
        CloudEvent next = event("next");
        eventStore.write("stream-next", Flux.just(next)).block(TIMEOUT);

        // Then
        await().atMost(TIMEOUT).until(() -> delivered.stream().anyMatch(event -> event.getId().equals("next")));
        Checkpoint nextCheckpoint = checkpointOf(delivered, "next");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storedCheckpoint()).as("the checkpoint stored after the event, with the mark refusing its save, delivered %s", ids(delivered)).isEqualTo(nextCheckpoint.asString()));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(ids(delivered).stream().filter("next"::equals).count()).as("how many times the event was delivered, %s", ids(delivered)).isEqualTo(1));
        Document stored = storedDocument();
        assertThat(stored.containsKey("positionMovedEarlier")).as("the mark in %s once the save landed", stored).isFalse();
        assertThat(storage.writeVersion(SUBSCRIPTION_ID).block(TIMEOUT)).as("the version stored, which the save without a condition keeps").isEqualTo(v + 2);
        assertThatThrownBy(() -> storage.delete(SUBSCRIPTION_ID, CheckpointWriteCondition.notOlderThan(v)).block(TIMEOUT))
                .as("the delete the cancel tried, after the save")
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
        assertThatThrownBy(() -> storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("write-back"), CheckpointWriteCondition.notOlderThan(v + 1)).block(TIMEOUT))
                .as("the write back the takeover stopped, after the save")
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
    }

    @Test
    void a_save_refused_for_a_newer_stored_version_after_a_delete_takeover_is_not_saved_over_and_the_action_runs_again() {
        // Given
        long v = subscribedAgainWhileTheDeleteIsHeld();
        List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), recording(delivered)).waitUntilStarted().block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.writeVersion(SUBSCRIPTION_ID).block(TIMEOUT)).as("the write version once the subscribe that took the delete over has written its start").isEqualTo(v + 2));
        Checkpoint another = new StringBasedCheckpoint("another-node");
        storage.save(SUBSCRIPTION_ID, another, CheckpointWriteCondition.notOlderThan(v + 3)).block(TIMEOUT);

        // When
        eventStore.write("stream-next", Flux.just(event("next"))).block(TIMEOUT);

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(ids(delivered).stream().filter("next"::equals).count()).as("how many times the event was delivered, %s", ids(delivered)).isGreaterThan(1));
        assertThat(storedCheckpoint()).as("the checkpoint stored, which the refused save is not saved over").isEqualTo(another.asString());
        assertThat(storage.writeVersion(SUBSCRIPTION_ID).block(TIMEOUT)).as("the version stored").isEqualTo(v + 3);
    }

    // Subscribes at the model default with nothing stored, saves the position of an event, and then cancels the id and
    // subscribes it again while the cancel's delete of the checkpoint is held. Answers the version the delete read.
    private long subscribedAgainWhileTheDeleteIsHeld() {
        List<CloudEvent> first = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), recording(first)).waitUntilStarted().block(TIMEOUT);
        eventStore.write("stream-first", Flux.just(event("first"))).block(TIMEOUT);
        await().atMost(TIMEOUT).until(() -> first.stream().anyMatch(event -> event.getId().equals("first")));
        Checkpoint firstCheckpoint = checkpointOf(first, "first");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storedCheckpoint()).as("the checkpoint stored after the first event").isEqualTo(firstCheckpoint.asString()));

        holdingDeleteStorage.holdTheNextDelete();
        model.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(holdingDeleteStorage.deleteHeld()).as("the delete of the cancelled subscription held after it read the stored version").isTrue();
        return holdingDeleteStorage.versionTheDeleteRead();
    }

    private static Function<CloudEvent, Mono<Void>> recording(List<CloudEvent> delivered) {
        return event -> Mono.fromRunnable(() -> delivered.add(event));
    }

    private static Checkpoint checkpointOf(List<CloudEvent> delivered, String id) {
        return delivered.stream().filter(event -> event.getId().equals(id)).findFirst().map(CheckpointAwareCloudEvent::getCheckpointOrThrowIAE).orElseThrow();
    }

    private static List<String> ids(List<CloudEvent> delivered) {
        return delivered.stream().map(CloudEvent::getId).toList();
    }

    private @Nullable String storedCheckpoint() {
        Checkpoint checkpoint = storage.read(SUBSCRIPTION_ID).block(TIMEOUT);
        return checkpoint == null ? null : checkpoint.asString();
    }

    private Document storedDocument() {
        return requireNonNull(Mono.from(mongoClient.getDatabase(databaseName).getCollection(checkpointCollectionName).find(new Document("_id", SUBSCRIPTION_ID)).first()).block(TIMEOUT));
    }

    private ReactorMongoEventStore eventStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new org.springframework.data.mongodb.ReactiveMongoTransactionManager(new org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        return new ReactorMongoEventStore(reactiveMongoTemplate, config);
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test-event-" + id).withTime(OffsetDateTime.now()).build();
    }

    // The real storage, except that the next conditional delete is held after it has been asked, which is once the model
    // has read the version in its condition and before the delete reaches the store
    private static final class HoldingDeleteStorage implements CheckpointStorage {
        private final CheckpointStorage storage;
        private final CountDownLatch deleteEntered = new CountDownLatch(1);
        private final AtomicLong versionTheDeleteRead = new AtomicLong(-1);
        private volatile Sinks.@Nullable Empty<Void> gate;
        private final Sinks.Empty<Void> released = Sinks.empty();

        private HoldingDeleteStorage(CheckpointStorage storage) {
            this.storage = storage;
        }

        private void holdTheNextDelete() {
            gate = released;
        }

        private boolean deleteHeld() {
            try {
                return deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private long versionTheDeleteRead() {
            return versionTheDeleteRead.get();
        }

        private void release() {
            released.tryEmitEmpty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId, CheckpointWriteCondition condition) {
            Sinks.@Nullable Empty<Void> held = gate;
            if (held == null || !(condition instanceof CheckpointWriteCondition.NotOlderThan notOlderThan)) {
                return storage.delete(subscriptionId, condition);
            }
            gate = null;
            versionTheDeleteRead.set(notOlderThan.writeVersion());
            deleteEntered.countDown();
            return held.asMono().then(Mono.defer(() -> storage.delete(subscriptionId, condition)));
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return storage.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return storage.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return storage.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return storage.evaluatesWriteConditionsFor(subscriptionId);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return storage.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return storage.delete(subscriptionId);
        }

        @Override
        public boolean evaluatesDeleteConditions() {
            return storage.evaluatesDeleteConditions();
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return storage.resolveFirstCheckpointRace(subscriptionId, candidate);
        }
    }
}
