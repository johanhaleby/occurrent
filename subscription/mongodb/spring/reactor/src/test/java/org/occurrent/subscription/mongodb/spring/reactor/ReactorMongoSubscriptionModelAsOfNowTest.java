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

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Timeout(30)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelAsOfNowTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String database;
    private ReactorMongoEventStore eventStore;
    private final List<Disposable> disposables = new CopyOnWriteArrayList<>();

    // Less than the 15 seconds the model lets the server clock be ahead of the cluster time the client knows, and more
    // than a write right after subscribe(..) takes, so the write gets a cluster time before the start the server clock
    // alone gives
    private static final Duration SERVER_CLOCK_STEP = Duration.ofSeconds(5);

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        database = Objects.requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        eventStore = eventStore(mongoClient);
    }

    private ReactorMongoEventStore eventStore(MongoClient client) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        return new ReactorMongoEventStore(new ReactiveMongoTemplate(client, database), eventStoreConfig);
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startsAtThePresent")
    void a_named_subscription_receives_an_event_written_right_after_subscribe_returns_while_the_server_clock_is_held_back(StartAt startAt) throws InterruptedException {
        // Given
        Sinks.Empty<Void> released = Sinks.empty();
        ReactorMongoSubscriptionModel model = model(command -> released.asMono().then(Mono.empty()));
        disposables.add(model::shutdown);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        model.subscribe("subscription", null, startAt, cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));

        // When
        write("written-after-subscribe-returned");
        // Held past the next second, so a start taken from the server clock without subtracting the time since the call is after the event
        Thread.sleep(1100);
        released.tryEmitEmpty();
        writeUntilDelivered("written-once-the-clock-arrived", delivered);

        // Then
        assertThat(delivered).contains("written-after-subscribe-returned");
    }

    private static Stream<Named<StartAt>> startsAtThePresent() {
        return Stream.of(Named.of("StartAt.now()", StartAt.now()), Named.of("the model default", StartAt.subscriptionModelDefault()));
    }

    @Test
    void a_subscription_started_from_the_global_checkpoint_as_of_now_receives_an_event_written_after_the_call() {
        // Given
        ReactorMongoSubscriptionModel model = model(command -> Mono.empty());
        Mono<Checkpoint> asOfNow = model.globalCheckpointAsOfNow();
        write("written-after-the-call");

        // When
        Checkpoint checkpoint = asOfNow.block(Duration.ofSeconds(10));
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        disposables.add(model.subscribe(null, StartAt.checkpoint(Objects.requireNonNull(checkpoint))).subscribe(cloudEvent -> delivered.add(cloudEvent.getId())));

        writeUntilDelivered("written-once-subscribed", delivered);

        // Then
        assertThat(delivered).contains("written-after-the-call");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startsAtThePresent")
    void a_named_subscription_receives_an_event_another_client_writes_right_after_subscribe_returns_while_the_server_clock_is_stepped_forward(StartAt startAt) {
        // Given
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP);
        disposables.add(model::shutdown);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        write("written-before-subscribe");
        try (MongoClient otherClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            ReactorMongoEventStore otherEventStore = eventStore(otherClient);
            model.subscribe("subscription", null, startAt, cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));

            // When
            write(otherEventStore, "written-by-another-client-after-subscribe-returned");
            writeUntilDelivered("written-once-subscribed", delivered);
        }

        // Then
        assertThat(delivered).contains("written-by-another-client-after-subscribe-returned").doesNotContain("written-before-subscribe");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startsAtThePresent")
    void a_named_subscription_receives_an_event_the_same_client_writes_right_after_subscribe_returns_while_the_server_clock_is_stepped_forward(StartAt startAt) {
        // Given
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP);
        disposables.add(model::shutdown);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        model.subscribe("subscription", null, startAt, cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));

        // When
        write("written-right-after-subscribe-returned");
        writeUntilDelivered("written-once-subscribed", delivered);

        // Then
        assertThat(delivered).contains("written-right-after-subscribe-returned");
    }

    @Test
    void a_subscription_from_the_flux_receives_an_event_written_right_after_it_is_subscribed_to_while_the_server_clock_is_stepped_forward() {
        // Given
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        disposables.add(model.subscribe(null, StartAt.now()).subscribe(cloudEvent -> delivered.add(cloudEvent.getId())));

        // When
        write("written-right-after-the-flux-was-subscribed-to");
        writeUntilDelivered("written-once-subscribed", delivered);

        // Then
        assertThat(delivered).contains("written-right-after-the-flux-was-subscribed-to");
    }

    @Test
    void a_named_subscription_receives_an_event_the_same_client_writes_while_subscribe_notes_the_present() {
        // Given
        write("written-before-subscribe");
        WriteWhenTheModelTakesTheTime writeWhenTheModelTakesTheTime = new WriteWhenTheModelTakesTheTime("written-while-subscribe-noted-the-present");
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP, writeWhenTheModelTakesTheTime);
        disposables.add(model::shutdown);
        Set<String> delivered = ConcurrentHashMap.newKeySet();

        // When
        writeWhenTheModelTakesTheTime.arm();
        model.subscribe("subscription", null, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));
        writeUntilDelivered("written-once-subscribed", delivered);

        // Then
        assertThat(writeWhenTheModelTakesTheTime.wrote()).isTrue();
        assertThat(delivered).contains("written-while-subscribe-noted-the-present");
    }

    @Test
    void a_subscription_started_from_the_global_checkpoint_as_of_now_receives_an_event_the_same_client_writes_while_the_call_notes_the_present() {
        // Given
        write("written-before-the-call");
        WriteWhenTheModelTakesTheTime writeWhenTheModelTakesTheTime = new WriteWhenTheModelTakesTheTime("written-while-the-call-noted-the-present");
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP, writeWhenTheModelTakesTheTime);

        // When
        writeWhenTheModelTakesTheTime.arm();
        Checkpoint checkpoint = model.globalCheckpointAsOfNow().block(Duration.ofSeconds(10));
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        disposables.add(model.subscribe(null, StartAt.checkpoint(Objects.requireNonNull(checkpoint))).subscribe(cloudEvent -> delivered.add(cloudEvent.getId())));
        writeUntilDelivered("written-once-subscribed", delivered);

        // Then
        assertThat(writeWhenTheModelTakesTheTime.wrote()).isTrue();
        assertThat(delivered).contains("written-while-the-call-noted-the-present");
    }

    @Test
    void a_subscription_restarted_after_its_history_was_lost_receives_an_event_another_client_writes_right_after_the_restart_while_the_server_clock_is_stepped_forward() throws InterruptedException {
        // Given
        CountDownLatch historyLost = new CountDownLatch(1);
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardByAndHistoryLostOnce(SERVER_CLOCK_STEP, historyLost);
        disposables.add(model::shutdown);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        write("written-before-subscribe");
        try (MongoClient otherClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            ReactorMongoEventStore otherEventStore = eventStore(otherClient);
            model.subscribe("subscription", null, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));
            assertThat(historyLost.await(10, TimeUnit.SECONDS)).isTrue();

            // When
            Thread.sleep(50);
            write(otherEventStore, "written-by-another-client-right-after-the-restart");
            writeUntilDelivered("written-once-restarted", delivered);
        }

        // Then
        assertThat(delivered).contains("written-by-another-client-right-after-the-restart");
    }

    @Test
    void the_cluster_time_can_be_read_when_it_was_looked_up_on_an_interrupted_thread_and_the_interrupt_is_kept() {
        // Given
        Thread.currentThread().interrupt();

        // When
        KnownClusterTime knownClusterTime;
        boolean stillInterrupted;
        try {
            knownClusterTime = KnownClusterTime.of(new ReactiveMongoTemplate(mongoClient, database));
        } finally {
            stillInterrupted = Thread.interrupted();
        }
        write("advances-the-cluster-time-the-client-knows");

        // Then
        assertThat(stillInterrupted).isTrue();
        assertThat(knownClusterTime.isReadable()).isTrue();
        assertThat(knownClusterTime.read()).isNotNull();
    }

    @Test
    void global_checkpoint_as_of_now_is_just_after_the_cluster_time_the_client_knew_at_the_call_when_that_is_before_the_server_clock() {
        // Given
        ReactorMongoSubscriptionModel model = modelWithServerClockSteppedForwardBy(SERVER_CLOCK_STEP);
        write("advances-the-cluster-time-the-client-knows");
        BsonTimestamp known = Objects.requireNonNull(KnownClusterTime.of(new ReactiveMongoTemplate(mongoClient, database)).read());

        // When
        Checkpoint checkpoint = model.globalCheckpointAsOfNow().block(Duration.ofSeconds(10));

        // Then
        assertThat(checkpoint).isEqualTo(new MongoOperationTimeCheckpoint(new BsonTimestamp(known.getTime(), known.getInc() + 1)));
    }

    @Test
    void the_cluster_time_the_driver_knows_can_be_read_on_this_driver_version() {
        // Fails when a driver upgrade moves the driver's internal clock, rather than every subscription quietly
        // starting from the server's clock alone
        KnownClusterTime knownClusterTime = KnownClusterTime.of(new ReactiveMongoTemplate(mongoClient, database));
        write("advances-the-cluster-time-the-client-knows");

        assertThat(knownClusterTime.isReadable()).isTrue();
        assertThat(knownClusterTime.read()).isNotNull();
    }

    @Test
    void the_cluster_time_is_not_read_from_operations_that_are_not_a_reactive_mongo_template() {
        KnownClusterTime knownClusterTime = KnownClusterTime.of(Mockito.mock(ReactiveMongoOperations.class));

        assertThat(knownClusterTime.isReadable()).isFalse();
        assertThat(knownClusterTime.read()).isNull();
    }

    @Test
    void global_checkpoint_as_of_now_asks_with_is_master_when_the_server_does_not_know_hello() {
        ReactorMongoSubscriptionModel model = model(command -> command.containsKey("hello") ? Mono.error(commandNotFound()) : Mono.empty());

        StepVerifier.create(model.globalCheckpointAsOfNow()).expectNextCount(1).verifyComplete();
    }

    @Test
    void global_checkpoint_as_of_now_fails_when_the_reply_has_no_clock() {
        ReactorMongoSubscriptionModel model = model(command -> Mono.just(new Document("ok", 1.0)));

        StepVerifier.create(model.globalCheckpointAsOfNow())
                .expectErrorSatisfies(throwable -> assertThat(throwable).hasMessageContaining("has no localTime"))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void a_subscription_started_now_stops_with_an_error_when_the_reply_has_no_clock() {
        // Given
        ReactorMongoSubscriptionModel model = model(command -> Mono.just(new Document("ok", 1.0)));

        // When
        Subscription subscription = model.subscribe("subscription", null, StartAt.now(), cloudEvent -> Mono.empty());

        // Then
        StepVerifier.create(subscription.waitUntilStarted())
                .expectErrorSatisfies(throwable -> assertThat(throwable).hasMessageContaining("has no localTime"))
                .verify(Duration.ofSeconds(10));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(model.isRunning("subscription")).isFalse());
        model.shutdown();
    }

    // A model whose replies to hello show a server clock that is ahead of the real one by step, as if it had been
    // stepped forward between subscribe(..) and the reply
    private ReactorMongoSubscriptionModel modelWithServerClockSteppedForwardBy(Duration step) {
        return modelWithServerClockSteppedForwardBy(step, System::nanoTime);
    }

    private ReactorMongoSubscriptionModel modelWithServerClockSteppedForwardBy(Duration step, LongSupplier nanoTime) {
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, database) {
            @Override
            public Mono<Document> executeCommand(Document command) {
                return super.executeCommand(command).map(reply -> stepped(command, reply, step));
            }
        };
        return new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING, ReactorMongoSubscriptionModelConfig.withConfig(), nanoTime);
    }

    // Like modelWithServerClockSteppedForwardBy, and the first change stream fails because its history is gone. The
    // model restarts it, and every hello after the first is answered a second late, so a write right after the
    // restart is made before the restarted change stream opens.
    private ReactorMongoSubscriptionModel modelWithServerClockSteppedForwardByAndHistoryLostOnce(Duration step, CountDownLatch historyLost) {
        AtomicInteger hellos = new AtomicInteger();
        AtomicInteger changeStreams = new AtomicInteger();
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, database) {
            @Override
            public Mono<Document> executeCommand(Document command) {
                Mono<Document> reply = super.executeCommand(command).map(it -> stepped(command, it, step));
                return command.containsKey("hello") && hellos.incrementAndGet() > 1 ? reply.delayElement(Duration.ofSeconds(1)) : reply;
            }

            @Override
            public <T> Flux<ChangeStreamEvent<T>> changeStream(String database, String collectionName, ChangeStreamOptions options, Class<T> targetType) {
                if (changeStreams.incrementAndGet() == 1) {
                    return Flux.defer(() -> {
                        historyLost.countDown();
                        return Flux.error(changeStreamHistoryLost());
                    });
                }
                return super.changeStream(database, collectionName, options, targetType);
            }
        };
        ReactorMongoSubscriptionModelConfig config = ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true);
        return new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING, config);
    }

    private static Document stepped(Document command, Document reply, Duration step) {
        return command.containsKey("hello") && reply.get("localTime") instanceof Date localTime
                ? new Document(reply).append("localTime", new Date(localTime.getTime() + step.toMillis()))
                : reply;
    }

    // Writes an event through the test's client the first time the model takes the time on the thread that armed it,
    // which is while that thread's call notes the present
    private final class WriteWhenTheModelTakesTheTime implements LongSupplier {
        private final String id;
        private final AtomicReference<Thread> armedBy = new AtomicReference<>();
        private final AtomicBoolean wrote = new AtomicBoolean();

        private WriteWhenTheModelTakesTheTime(String id) {
            this.id = id;
        }

        void arm() {
            armedBy.set(Thread.currentThread());
        }

        boolean wrote() {
            return wrote.get();
        }

        @Override
        public long getAsLong() {
            long now = System.nanoTime();
            if (armedBy.compareAndSet(Thread.currentThread(), null)) {
                write(id);
                wrote.set(true);
            }
            return now;
        }
    }

    private static UncategorizedMongoDbException changeStreamHistoryLost() {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("the resume point may no longer be in the oplog"))
                .append("code", new BsonInt32(286))
                .append("codeName", new BsonString("ChangeStreamHistoryLost"));
        return new UncategorizedMongoDbException("the resume point may no longer be in the oplog", new MongoCommandException(response, new ServerAddress()));
    }

    // replies answers a command in place of the server, or completes empty to let the server answer
    private ReactorMongoSubscriptionModel model(Function<Document, Mono<Document>> replies) {
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, database) {
            @Override
            public Mono<Document> executeCommand(Document command) {
                return replies.apply(command).switchIfEmpty(Mono.defer(() -> super.executeCommand(command)));
            }
        };
        return new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
    }

    private static UncategorizedMongoDbException commandNotFound() {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("no such command: 'hello'"))
                .append("code", new BsonInt32(59))
                .append("codeName", new BsonString("CommandNotFound"));
        return new UncategorizedMongoDbException("no such command: 'hello'", new MongoCommandException(response, new ServerAddress()));
    }

    // Writes until the event arrives, since the change stream may open after the first write
    private void writeUntilDelivered(String id, Set<String> delivered) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until(() -> {
            write(id + "-" + System.nanoTime());
            return delivered.stream().anyMatch(deliveredId -> deliveredId.startsWith(id));
        });
    }

    private void write(String id) {
        write(eventStore, id);
    }

    private static void write(ReactorMongoEventStore eventStore, String id) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Written")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write(id, Flux.just(cloudEvent)).block(Duration.ofSeconds(10));
    }
}
