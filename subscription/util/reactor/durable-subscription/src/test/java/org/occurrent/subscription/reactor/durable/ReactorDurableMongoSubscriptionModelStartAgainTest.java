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
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
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
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription handed to {@link ReactorMongoSubscriptionModel} that {@link ReactorDurableSubscriptionModel} starts
 * again there from an earlier first position is cancelled while the Mongo model takes the cancel the start again sends
 * it before its second subscribe. That cancel goes by id, so a subscribe of the id that came before it would lose its
 * subscription to it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableMongoSubscriptionModelStartAgainTest {

    private static final String DATABASE = "reactordurablemongostartagain";
    private static final String SUBSCRIPTION_ID = "sub";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;

    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final AtomicLong eventsWritten = new AtomicLong();
    private final ExecutorService caller = Executors.newSingleThreadExecutor();
    private final ReactiveMongoTemplate template;
    private final ReactorMongoEventStore eventStore;
    private final HeldCancelMongoModel mongoModel;
    private final RaceStorage storage;
    private final ReactorDurableSubscriptionModel model;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    ReactorDurableMongoSubscriptionModelStartAgainTest() {
        template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(timeRepresentation)
                .build();
        eventStore = new ReactorMongoEventStore(template, eventStoreConfig);
        mongoModel = new HeldCancelMongoModel(template, eventCollectionName, timeRepresentation);
        storage = new RaceStorage();
        model = new ReactorDurableSubscriptionModel(mongoModel, storage);
    }

    @AfterEach
    void shutdown() {
        mongoModel.letGo.countDown();
        storage.deleteLetGo.countDown();
        caller.shutdownNow();
        model.shutdown();
        template.remove(new Query(), eventCollectionName).block(TIMEOUT);
    }

    @Test
    void a_subscribe_of_the_id_before_its_cancel_completes_is_refused_while_the_mongo_model_takes_the_cancel_of_the_start_again() throws Exception {
        // Given
        heldInTheCancelOfTheStartAgain();

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        CompletableFuture<Void> cancelCompleted = cancelled.toFuture();
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        boolean completedWhileHeld = cancelCompleted.isDone();
        mongoModel.letGo.countDown();
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the subscribe before the cancel completed ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(completedWhileHeld).as("the cancel completed while the Mongo model took the cancel of the start again").isFalse();
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    @Test
    void a_subscribe_of_the_id_once_its_cancel_completes_delivers_every_event_written_after_it() throws Exception {
        // Given
        heldInTheCancelOfTheStartAgain();

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        mongoModel.letGo.countDown();
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    // Subscribes the id and cancels it while the delete of its checkpoint is held. Subscribes it again from the model
    // default, and another node stores an earlier checkpoint right before that subscribe records its first, so the model
    // starts it again in the Mongo model from that one. Returns once the cancel the start again sends there is held.
    private void heldInTheCancelOfTheStartAgain() throws Exception {
        List<Checkpoint> checkpoints = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> checkpoints.add(CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(cloudEvent))))
                .waitUntilStarted(TIMEOUT).block();
        write();
        write();
        await().atMost(TIMEOUT).until(() -> checkpoints.size() == 2);
        // The subscribe below then records the position it reads from the Mongo model as its first
        storage.storage.delete(SUBSCRIPTION_ID).block(TIMEOUT);
        storage.holdsDeletes = true;
        model.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
        storage.storedElsewhereBeforeIfAbsent = checkpoints.getFirst();
        mongoModel.heldCancel = 2;
        subscribe(new CopyOnWriteArrayList<>());
        storage.deleteLetGo.countDown();
        assertThat(mongoModel.cancelHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("cancel of the start again held").isTrue();
    }

    // What a subscribe of the id from the model default delivered, of an event written before it and two written after
    private record Later(List<Long> delivered, long writtenBefore, List<Long> writtenAfter) {
    }

    private Later subscribeAgain() throws Exception {
        long writtenBefore = write();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        subscribe(delivered).waitUntilStarted(TIMEOUT).block();
        List<Long> writtenAfter = List.of(write(), write());
        try {
            await().atMost(TIMEOUT).until(() -> delivered.contains(writtenAfter.getLast()));
        } catch (ConditionTimeoutException notDelivered) {
            // What was delivered instead is asserted next
        }
        return new Later(delivered, writtenBefore, writtenAfter);
    }

    // On a thread of its own, since a subscribe that takes over a delete can wait for a read
    private Subscription subscribe(List<Long> delivered) throws Exception {
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private long write() {
        long position = eventsWritten.incrementAndGet();
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(String.valueOf(position))
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Written")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write("stream", Flux.just(cloudEvent)).block(TIMEOUT);
        return position;
    }

    private static Function<CloudEvent, Mono<Void>> action(List<Long> delivered) {
        return cloudEvent -> Mono.fromRunnable(() -> delivered.add(Long.parseLong(cloudEvent.getId())));
    }

    // Holds the cancel of that number, counted from the first, before it reaches the Mongo model, until letGo opens
    private static final class HeldCancelMongoModel extends ReactorMongoSubscriptionModel {
        private final AtomicInteger cancels = new AtomicInteger();
        private final CountDownLatch cancelHeld = new CountDownLatch(1);
        private final CountDownLatch letGo = new CountDownLatch(1);
        private volatile int heldCancel = -1;

        private HeldCancelMongoModel(ReactiveMongoTemplate template, String eventCollectionName, TimeRepresentation timeRepresentation) {
            super(template, eventCollectionName, timeRepresentation);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            if (cancels.incrementAndGet() == heldCancel) {
                cancelHeld.countDown();
                try {
                    letGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.cancelSubscription(subscriptionId);
        }
    }

    // Holds every delete while holdsDeletes is set, until deleteLetGo opens. Stores a checkpoint of another node right
    // before the next write on the condition that nothing is stored, and settles a first-position race in favour of
    // what is stored.
    private static final class RaceStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final CountDownLatch deleteEntered = new CountDownLatch(1);
        private final CountDownLatch deleteLetGo = new CountDownLatch(1);
        private volatile boolean holdsDeletes;
        private volatile @Nullable Checkpoint storedElsewhereBeforeIfAbsent;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return storage.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return storage.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                @Nullable Checkpoint elsewhere = storedElsewhereBeforeIfAbsent;
                if (elsewhere != null && condition instanceof CheckpointWriteCondition.IfAbsent) {
                    storedElsewhereBeforeIfAbsent = null;
                    return storage.save(subscriptionId, elsewhere, CheckpointWriteCondition.any())
                            .then(Mono.defer(() -> storage.save(subscriptionId, checkpoint, condition)));
                }
                return storage.save(subscriptionId, checkpoint, condition);
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return storage.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return storage.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                if (!holdsDeletes) {
                    return storage.delete(subscriptionId);
                }
                holdsDeletes = false;
                deleteEntered.countDown();
                return Mono.fromCallable(() -> deleteLetGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .subscribeOn(Schedulers.boundedElastic())
                        .then(Mono.defer(() -> storage.delete(subscriptionId)));
            });
        }
    }
}
