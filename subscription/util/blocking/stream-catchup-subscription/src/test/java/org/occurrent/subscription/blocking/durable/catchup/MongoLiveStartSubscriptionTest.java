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

package org.occurrent.subscription.blocking.durable.catchup;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * A change stream started at the live start of a scope delivers what was written after the scope opened and not what its reads can see.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoLiveStartSubscriptionTest {

    private static final Duration AT_MOST = Duration.ofSeconds(40);

    enum Driver {NATIVE, SPRING}

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "livestart"));

    private MongoClient mongoClient;
    private ExecutorService executor;
    private SubscriptionModel subscriptions;

    @AfterEach
    void shutdown() {
        if (subscriptions != null) {
            subscriptions.shutdown();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        mongoClient.close();
    }

    @ParameterizedTest
    @EnumSource(Driver.class)
    void a_subscription_started_at_the_live_start_of_a_scope_receives_what_was_written_after_the_scope_was_opened_and_not_what_the_scope_could_read(Driver driver) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("livestart"));
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        EventStore eventStore;
        if (driver == Driver.NATIVE) {
            org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig config = new org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig.Builder()
                    .timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build();
            eventStore = new org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore(mongoClient, database, "events", config);
            executor = Executors.newCachedThreadPool();
            subscriptions = new NativeMongoSubscriptionModel(mongoClient.getDatabase(database), "events", TimeRepresentation.RFC_3339_STRING, executor);
        } else {
            MongoTemplate template = new MongoTemplate(mongoClient, database);
            MongoTransactionManager tx = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, database));
            org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig config = new org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig.Builder()
                    .eventStoreCollectionName("events").transactionConfig(tx).timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build();
            eventStore = new org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore(template, config);
            subscriptions = new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
        }
        eventStore.write("stream", List.of(event("before-scope", "Happened")));
        String liveStart;
        try (CatchupReadScope scope = ((CatchupReads) eventStore).openCatchupReadScope(null)) {
            liveStart = scope.liveStart();
        }
        eventStore.write("stream", List.of(event("after-scope", "Happened")));

        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscriptions.subscribe("live-start", StartAt.checkpoint(new StringBasedCheckpoint(liveStart)), (CloudEvent e) -> received.add(e.getId())).waitUntilStarted();
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("after-scope"));
        eventStore.write("stream", List.of(event("sentinel", "Happened")));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).contains("sentinel"));

        assertThat(received).containsExactly("after-scope", "sentinel");
    }
}
