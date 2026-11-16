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
package org.occurrent.springboot.mongo.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catch-up marker of {@code CatchupThenPushSubscriptionModel} means done on the blocking stack (ADR 179), checked over the real MongoDB event store and checkpoint storage.
 * Every start is a fresh model over the same store and storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class CatchupThenPushMarkerMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient mongoClient;
    private SpringMongoEventStore eventStore;
    private CheckpointStorage marker;

    @BeforeEach
    void create_store_and_marker_storage() {
        String databaseName = "catchup-resume-" + UUID.randomUUID();
        mongoClient = MongoClients.create(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);
        MongoTemplate mongoTemplate = new MongoTemplate(databaseFactory);
        eventStore = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(databaseFactory))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build());
        marker = new SpringMongoCheckpointStorage(mongoTemplate, "catchup-markers");
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() throws Exception {
        write("1", "2");
        List<String> first = new CopyOnWriteArrayList<>();
        start(first);
        assertThat(first).as("the first start replays the history").containsExactly("1", "2");
        assertThat(marker.exists("proj")).as("the first start stored the marker").isTrue();

        List<String> restarted = new CopyOnWriteArrayList<>();
        start(restarted);

        assertThat(restarted).as("a restart over the same storage and id replays nothing").isEmpty();
    }

    @Test
    void a_cancel_deletes_the_marker_so_a_resubscribe_replays_the_whole_history() throws Exception {
        write("1", "2");
        CatchupThenPushSubscriptionModel first = start(new CopyOnWriteArrayList<>());
        assertThat(marker.exists("proj")).as("the first start stored the marker").isTrue();

        first.cancelSubscription("proj");
        assertThat(marker.exists("proj")).as("the cancel deleted the marker").isFalse();

        List<String> resubscribed = new CopyOnWriteArrayList<>();
        start(resubscribed);

        assertThat(resubscribed).as("a resubscribe of the cancelled id replays the whole history").containsExactly("1", "2");
    }

    private CatchupThenPushSubscriptionModel start(List<String> delivered) throws InterruptedException {
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(eventStore, new PushSubscriptionModel(), marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> delivered.add(ce.getId())).waitUntilStarted();
        // The marker is written before the replay entry is removed, so a finished catch-up means the marker is stored.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (model.isCatchingUp("proj") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(model.isCatchingUp("proj")).as("catch-up finished").isFalse();
        return model;
    }

    private void write(String... ids) {
        List<CloudEvent> events = java.util.Arrays.stream(ids)
                .map(id -> CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated").build())
                .toList();
        eventStore.write("stream", events);
    }
}
