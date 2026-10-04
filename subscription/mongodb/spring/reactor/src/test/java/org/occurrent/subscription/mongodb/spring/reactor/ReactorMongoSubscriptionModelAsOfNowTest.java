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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

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

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        database = Objects.requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, database), eventStoreConfig);
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
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

    @Test
    void global_checkpoint_as_of_now_is_an_operation_time_at_the_start_of_a_second() {
        Checkpoint checkpoint = model(command -> Mono.empty()).globalCheckpointAsOfNow().block(Duration.ofSeconds(10));

        assertThat(checkpoint).isInstanceOfSatisfying(MongoOperationTimeCheckpoint.class, operationTime -> assertThat(operationTime.operationTime.getInc()).isZero());
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
