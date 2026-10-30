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
import org.junit.jupiter.api.*;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.testsupport.previousrelease.PreviousRelease;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A reactor catch-up stamps a replayed event with the marked form when its replay started past position 0, and the
 * durable model stores that stamp. Occurrent 0.34 would resume the composite without its live start, so it must refuse
 * the marked form before it writes anything, and this release must resume from it. A reactor catch-up never writes the
 * marked form of a time replay, so a stored one is refused before anything is delivered or written.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReactorDurableMarkedCheckpointPreviousReleaseMongoTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String DATABASE = "reactormarkedcheckpoint";

    // Subscribes the way a 0.34 node resumes a subscription from storage
    private static final String READER = """
            import com.mongodb.reactivestreams.client.MongoClient;
            import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
            import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
            import org.occurrent.filter.Filter;
            import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
            import org.occurrent.subscription.StartAt;
            import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
            import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
            import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
            import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
            import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
            import reactor.core.publisher.Mono;

            import java.time.Duration;

            import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

            public class Reader {
                public static Object run(Object... args) {
                    MongoClient client = (MongoClient) args[0];
                    ReactiveMongoTemplate template = new ReactiveMongoTemplate(client, (String) args[1]);
                    String events = (String) args[2];
                    ReactorMongoEventStore eventStore = new ReactorMongoEventStore(template, new EventStoreConfig.Builder()
                            .eventStoreCollectionName(events).transactionConfig(new org.springframework.data.mongodb.ReactiveMongoTransactionManager(
                                    new org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory(client, (String) args[1])))
                            .timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build());
                    ReactorDurableSubscriptionModel durable = new ReactorDurableSubscriptionModel(
                            new ReactorCatchupSubscriptionModel(new ReactorMongoSubscriptionModel(template, events, TimeRepresentation.RFC_3339_STRING), eventStore, Filter.all()),
                            new ReactorCheckpointStorage(template, (String) args[3]));
                    try {
                        durable.subscribe("sub", null, StartAt.subscriptionModelDefault(), event -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(20));
                        return null;
                    } finally {
                        durable.shutdown();
                    }
                }
            }
            """;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static PreviousRelease.Driver reader;
    private static MongoClient mongoClient;
    private static String databaseName;

    private final CountDownLatch crashed = new CountDownLatch(1);
    private final List<ReactorDurableSubscriptionModel> models = new ArrayList<>();
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private String eventCollectionName;
    private String checkpointCollectionName;

    @BeforeAll
    static void connect_and_compile_the_0_34_reader() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        reader = PreviousRelease.fromJarsIn(Path.of("target", "occurrent-0.34.0")).compile("Reader", READER);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void create_collections() {
        eventCollectionName = "events-" + UUID.randomUUID();
        checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
        reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
    }

    @AfterEach
    void shutdown() {
        crashed.countDown();
        models.forEach(ReactorDurableSubscriptionModel::shutdown);
    }

    @Test
    void a_position_a_replay_from_past_the_beginning_stored_is_refused_by_0_34_and_resumed_by_this_release() {
        // Given
        ReactorMongoEventStore eventStore = eventStore();
        List.of("A", "B", "C", "D").forEach(id -> eventStore.write("stream-" + id, Flux.just(event(id))).block(TIMEOUT));
        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        CountDownLatch deliveringC = new CountDownLatch(1);
        ReactorDurableSubscriptionModel first = durable(eventStore, storage);
        first.subscribe("sub", null, StartAt.checkpoint(GlobalCheckpoint.of(1)), event -> Mono.fromRunnable(() -> {
            if (event.getId().equals("C")) {
                deliveringC.countDown();
                awaitOrFail(crashed);
            }
        }));
        awaitOrFail(deliveringC);
        first.shutdown();
        String stored = requireNonNull(storage.read("sub").block(TIMEOUT), "a checkpoint stored for B").asString();
        assertThat(CatchupCheckpoints.parse(new StringBasedCheckpoint(stored))).as("the stamp of B the durable model stored, %s", stored)
                .isInstanceOfSatisfying(CatchupCheckpoints.Position.class, position -> assertThat(position.position()).as("its position").isEqualTo(2));
        assertThat(CatchupCheckpoints.isMarked(new StringBasedCheckpoint(stored))).as("whether %s is the marked form", stored).isTrue();
        Document document = requireNonNull(Mono.from(mongoClient.getDatabase(databaseName).getCollection(checkpointCollectionName).find(new Document("_id", "sub")).first()).block(TIMEOUT));
        assertThat(document.getString("checkpoint")).as("the whole marked form in the checkpoint field").isEqualTo(stored);
        assertThat(document.keySet()).as("the fields of the stored document").noneMatch(field -> field.startsWith("catchup"));

        // When
        // Then
        assertThatThrownBy(() -> reader.run(mongoClient, databaseName, eventCollectionName, checkpointCollectionName))
                .as("what 0.34 throws when it resumes from the marked form")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a global checkpoint: " + stored);
        assertThat(checkpointDocuments()).as("the checkpoints after 0.34 refused the marked form").containsExactly(document);

        List<String> received = new CopyOnWriteArrayList<>();
        ReactorDurableSubscriptionModel restarted = durable(eventStore, storage);
        restarted.subscribe("sub", null, StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> received.add(event.getId()))).waitUntilStarted().block(TIMEOUT);
        eventStore.write("stream-E", Flux.just(event("E"))).block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).as("what the restarted subscription received").contains("E"));
        assertThat(received).as("the events after the stored stamp, replayed from it").contains("C", "D").doesNotContain("A", "B");
    }

    @Test
    void a_stored_marked_time_is_refused_before_anything_is_delivered_or_written() {
        // Given
        ReactorMongoEventStore eventStore = eventStore();
        eventStore.write("stream-A", Flux.just(event("A"))).block(TIMEOUT);
        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        Checkpoint markedTime = new StringBasedCheckpoint(CatchupCheckpoints.format("2026-10-01T12:00:00Z", "2026-10-01T11:00:00Z", requireNonNull(new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING).globalCheckpoint().block(TIMEOUT))));
        storage.save("sub", markedTime).block(TIMEOUT);
        List<Document> before = checkpointDocuments();
        List<String> received = new CopyOnWriteArrayList<>();
        ReactorDurableSubscriptionModel durable = durable(eventStore, storage);

        // When
        // Then
        assertThatThrownBy(() -> durable.subscribe("sub", null, StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> received.add(event.getId())))
                .waitUntilStarted().block(TIMEOUT))
                .as("what resuming from the stored marked time throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", markedTime.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        eventStore.write("stream-B", Flux.just(event("B"))).block(TIMEOUT);
        assertThat(checkpointDocuments()).as("the checkpoints after the refusal").isEqualTo(before);
        assertThat(received).as("what the refused subscription delivered").isEmpty();
    }

    private List<Document> checkpointDocuments() {
        return requireNonNull(Flux.from(mongoClient.getDatabase(databaseName).getCollection(checkpointCollectionName).find()).collectList().block(TIMEOUT));
    }

    private ReactorDurableSubscriptionModel durable(ReactorMongoEventStore eventStore, ReactorCheckpointStorage storage) {
        ReactorMongoSubscriptionModel live = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new ReactorCatchupSubscriptionModel(live, eventStore, Filter.all()), storage);
        models.add(model);
        return model;
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

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            assertThat(latch.await(60, SECONDS)).as("the latch was counted down in time").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
