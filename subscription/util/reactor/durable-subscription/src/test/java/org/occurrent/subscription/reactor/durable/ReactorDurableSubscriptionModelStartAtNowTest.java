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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code StartAt.now()} starts where {@code subscribe(..)} was called, however late the wrapped MongoDB model learns
 * where that is. Every command the model sends is held until the test releases it, so an event written after
 * {@code subscribe(..)} returns always reaches MongoDB before the model has asked it anything.
 */
@Timeout(30)
@Testcontainers
class ReactorDurableSubscriptionModelStartAtNowTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private HeldCommandsTemplate modelTemplate;
    private ReactorMongoEventStore eventStore;
    private ReactorCheckpointStorage checkpointStorage;
    private @Nullable ReactorDurableSubscriptionModel model;

    @BeforeEach
    void create_mongo_event_store_and_models() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        String database = Objects.requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, database);
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        eventStore = new ReactorMongoEventStore(template, eventStoreConfig);
        checkpointStorage = new ReactorCheckpointStorage(template, "checkpoints");
        modelTemplate = new HeldCommandsTemplate(mongoClient, database);
    }

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
        mongoClient.close();
    }

    @ParameterizedTest(name = "handed over to the MongoDB model: {0}")
    @ValueSource(booleans = {true, false})
    void an_event_written_after_subscribe_returns_is_delivered_when_the_model_learns_the_position_later(boolean handedOver) {
        // Given
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(modelTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        model = new ReactorDurableSubscriptionModel(handedOver ? mongoModel : feedOnly(mongoModel), checkpointStorage);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        model.subscribe("subscription", null, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));

        // When
        write("after-subscribe-returned");
        modelTemplate.releaseCommands();
        writeUntilDelivered("after-the-model-was-answered", delivered);

        // Then
        assertThat(delivered).contains("after-subscribe-returned");
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

    // Writes until the event arrives, since the change stream may open after the first write and an event written
    // before it opens is not what this waits for
    private void writeUntilDelivered(String id, Set<String> delivered) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until(() -> {
            if (!delivered.contains(id)) {
                write(id + "-" + System.nanoTime());
            }
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

    // Holds every command the model sends until releaseCommands(), which is how the model learns where the present is
    private static final class HeldCommandsTemplate extends ReactiveMongoTemplate {
        private final Sinks.Empty<Void> released = Sinks.empty();

        HeldCommandsTemplate(MongoClient mongoClient, String databaseName) {
            super(mongoClient, databaseName);
        }

        @Override
        public Mono<Document> executeCommand(Document command) {
            return released.asMono().then(Mono.defer(() -> super.executeCommand(command)));
        }

        void releaseCommands() {
            released.tryEmitEmpty();
        }
    }
}
