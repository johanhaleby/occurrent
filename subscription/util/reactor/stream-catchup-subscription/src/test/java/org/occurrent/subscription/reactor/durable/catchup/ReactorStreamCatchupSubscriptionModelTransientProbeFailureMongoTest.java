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

package org.occurrent.subscription.reactor.durable.catchup;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An unnamed catch-up asks the MongoDB model whether it can resume from a stored checkpoint before it goes live from
 * it. A fail point makes that one {@code aggregate} fail with a primary that stepped down.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelTransientProbeFailureMongoTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "unnamed-catchup-probe-subscriber";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int NOT_WRITABLE_PRIMARY = 10107;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private MongoClient adminClient;
    private MongoClient subscriberClient;
    private Disposable subscription;
    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel liveModel;

    @BeforeEach
    void createModels() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".unnamedcatchupprobe");
        String database = requireNonNull(connectionString.getDatabase());
        String collection = "events-" + UUID.randomUUID();
        adminClient = MongoClients.create(connectionString);
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(SUBSCRIBER_APPLICATION_NAME).build());
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(adminClient, database), new EventStoreConfig.Builder().eventStoreCollectionName(collection)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(adminClient, database))).timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM).build());
        liveModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, database), collection, TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofMillis(10), Duration.ofMillis(50)));
    }

    @AfterEach
    void close() {
        if (subscription != null) {
            subscription.dispose();
        }
        Mono.from(adminClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"))).block(TIMEOUT);
        subscriberClient.close();
        adminClient.close();
    }

    @Test
    void a_transient_failure_of_the_check_of_a_stored_checkpoint_does_not_end_an_unnamed_catch_up_that_goes_live_from_it() {
        eventStore.write("stream", event("before")).block(TIMEOUT);
        CatchupReadScope scope = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        String storedCheckpoint = requireNonNull(scope).liveStart();
        scope.close().block(TIMEOUT);
        eventStore.write("stream", event("written-after-the-checkpoint")).block(TIMEOUT);
        failNextAggregate();

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(liveModel, eventStore);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        subscription = catchup.subscribe(Filter.all(), StartAt.checkpoint(new StringBasedCheckpoint(storedCheckpoint)))
                .subscribe(delivered -> received.add(delivered.getId()), failure::set);
        await().atMost(TIMEOUT).until(() -> !received.isEmpty() || failure.get() != null);

        assertAll(
                () -> assertThat(failure.get()).as("failure of the Flux").isNull(),
                () -> assertThat(received).as("received").containsExactly("written-after-the-checkpoint"));
        eventStore.write("stream", event("written-live")).block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).containsExactly("written-after-the-checkpoint", "written-live"));
    }

    private void failNextAggregate() {
        Document data = new Document("failCommands", List.of("aggregate")).append("appName", SUBSCRIBER_APPLICATION_NAME).append("errorCode", NOT_WRITABLE_PRIMARY);
        Document command = new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1)).append("data", data);
        Mono.from(adminClient.getDatabase("admin").runCommand(command)).block(TIMEOUT);
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Happened").withTime(OffsetDateTime.now()).build();
    }
}
