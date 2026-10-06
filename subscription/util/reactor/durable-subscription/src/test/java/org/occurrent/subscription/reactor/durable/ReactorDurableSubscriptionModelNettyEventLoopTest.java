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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A subscribe made inside a callback of the reactive MongoDB driver runs on the driver's own event loop thread, which
 * Reactor does not count as a thread that may not block. The driver here has one such thread, so a subscribe that
 * waited there for MongoDB to answer would wait for an answer only that thread can deliver.
 */
@Timeout(60)
@Testcontainers
class ReactorDurableSubscriptionModelNettyEventLoopTest {
    private static final String DATABASE = "netty-event-loop";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "subscription";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final String checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
    private final AtomicLong eventsWritten = new AtomicLong();
    private final List<ReactorDurableSubscriptionModel> handingOver = new CopyOnWriteArrayList<>();
    private EventLoopGroup eventLoop;
    private MongoClient mongoClient;
    private ReactiveMongoTemplate template;
    private ReactorMongoEventStore eventStore;
    private ReactorDurableSubscriptionModel model;

    @BeforeEach
    void create_a_mongodb_client_with_one_event_loop_thread() {
        eventLoop = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE)))
                .transportSettings(TransportSettings.nettyBuilder().eventLoopGroup(eventLoop).build())
                .build();
        mongoClient = MongoClients.create(settings);
        template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        eventStore = new ReactorMongoEventStore(template, new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build());
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(template, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        model = new ReactorDurableSubscriptionModel(feedOnly(mongoModel), new ReactorCheckpointStorage(template, checkpointCollectionName));
    }

    @AfterEach
    void shutdown() {
        // Also ends a subscribe still waiting on the event loop thread, so the client can close
        model.shutdown();
        handingOver.forEach(ReactorDurableSubscriptionModel::shutdown);
        mongoClient.close();
        eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
    }

    static Stream<Arguments> startPositionsReadFromMongoDB() {
        return Stream.of(
                Arguments.of(Named.of("the model default", StartAt.subscriptionModelDefault())),
                Arguments.of(Named.of("now", StartAt.now())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startPositionsReadFromMongoDB")
    void a_subscribe_made_on_the_only_event_loop_thread_of_the_mongodb_driver_returns_and_the_subscription_starts(StartAt startAt) {
        // Given
        AtomicBoolean onTheEventLoop = new AtomicBoolean();

        // When
        CompletableFuture<Subscription> subscribed = Mono.from(mongoClient.getDatabase(DATABASE).runCommand(new Document("ping", 1)))
                .map(__ -> {
                    onTheEventLoop.set(eventLoop.next().inEventLoop());
                    return model.subscribe("subscription", null, startAt, cloudEvent -> Mono.empty());
                })
                .toFuture();

        // Then
        assertThat(subscribed).as("the subscribe").succeedsWithin(Duration.ofSeconds(10));
        assertThat(onTheEventLoop).as("subscribed on the event loop thread").isTrue();
        assertThat(subscribed.join().waitUntilStarted().toFuture()).as("the start").succeedsWithin(Duration.ofSeconds(10));
    }

    @Test
    void a_subscribe_from_the_model_default_made_on_the_only_event_loop_thread_returns_and_the_mongodb_model_it_is_handed_to_delivers_what_is_written_after_it() {
        // Given
        ReactorDurableSubscriptionModel durableMongo = handingOverToTheMongoDBModel();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean onTheEventLoop = new AtomicBoolean();

        // When
        CompletableFuture<Subscription> subscribed = subscribeOnTheEventLoop(durableMongo, onTheEventLoop, delivered);

        // Then
        assertThat(subscribed).as("the subscribe").succeedsWithin(TIMEOUT);
        assertThat(onTheEventLoop).as("subscribed on the event loop thread").isTrue();
        assertThat(subscribed.join().waitUntilStarted().toFuture()).as("the start").succeedsWithin(TIMEOUT);
        long writtenAfter = write();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered").contains(writtenAfter));
    }

    @Test
    void a_subscribe_from_the_model_default_made_on_the_only_event_loop_thread_returns_and_the_mongodb_model_it_is_handed_to_resumes_from_the_stored_checkpoint() {
        // Given
        ReactorDurableSubscriptionModel before = handingOverToTheMongoDBModel();
        List<Long> deliveredBefore = new CopyOnWriteArrayList<>();
        before.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(deliveredBefore)).waitUntilStarted().block(TIMEOUT);
        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(template, checkpointCollectionName);
        String pinned = await().atMost(TIMEOUT).until(() -> asString(storage.read(SUBSCRIPTION_ID).block(TIMEOUT)), stored -> stored != null);
        long handledBefore = write();
        await().atMost(TIMEOUT).until(() -> deliveredBefore.contains(handledBefore) && !pinned.equals(asString(storage.read(SUBSCRIPTION_ID).block(TIMEOUT))));
        before.shutdown();
        long writtenWhileShutDown = write();
        ReactorDurableSubscriptionModel durableMongo = handingOverToTheMongoDBModel();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean onTheEventLoop = new AtomicBoolean();

        // When
        CompletableFuture<Subscription> subscribed = subscribeOnTheEventLoop(durableMongo, onTheEventLoop, delivered);

        // Then
        assertThat(subscribed).as("the subscribe").succeedsWithin(TIMEOUT);
        assertThat(onTheEventLoop).as("subscribed on the event loop thread").isTrue();
        assertThat(subscribed.join().waitUntilStarted().toFuture()).as("the start").succeedsWithin(TIMEOUT);
        long writtenAfter = write();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered").contains(writtenAfter));
        assertThat(delivered).as("events delivered").containsExactly(writtenWhileShutDown, writtenAfter);
    }

    // The durable model hands the subscription to the MongoDB model, which manages named subscriptions itself
    private ReactorDurableSubscriptionModel handingOverToTheMongoDBModel() {
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(template, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        ReactorDurableSubscriptionModel durableMongo = new ReactorDurableSubscriptionModel(mongoModel, new ReactorCheckpointStorage(template, checkpointCollectionName));
        handingOver.add(durableMongo);
        return durableMongo;
    }

    private CompletableFuture<Subscription> subscribeOnTheEventLoop(ReactorDurableSubscriptionModel durableMongo, AtomicBoolean onTheEventLoop, List<Long> delivered) {
        return Mono.from(mongoClient.getDatabase(DATABASE).runCommand(new Document("ping", 1)))
                .map(__ -> {
                    onTheEventLoop.set(eventLoop.next().inEventLoop());
                    return durableMongo.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered));
                })
                .toFuture();
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

    private static @Nullable String asString(@Nullable Checkpoint checkpoint) {
        return checkpoint == null ? null : checkpoint.asString();
    }

    // The durable model reads the feed itself when the wrapped model has no named subscriptions of its own
    private static CheckpointAwareSubscriptionModel feedOnly(ReactorMongoSubscriptionModel wrapped) {
        return new CheckpointAwareSubscriptionModel() {
            @Override
            public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
                return wrapped.subscribe(filter, startAt);
            }

            @Override
            public Mono<Checkpoint> globalCheckpoint() {
                return wrapped.globalCheckpoint();
            }

            @Override
            public Mono<Checkpoint> globalCheckpointAsOfNow() {
                return wrapped.globalCheckpointAsOfNow();
            }
        };
    }
}
