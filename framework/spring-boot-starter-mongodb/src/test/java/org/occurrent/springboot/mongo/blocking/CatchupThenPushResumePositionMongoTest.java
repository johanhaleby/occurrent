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
import org.occurrent.subscription.GlobalCheckpoint;
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
 * The catch-up marker of {@code CatchupThenPushSubscriptionModel} is the position the next catch-up resumes after
 * (ADR 179), checked over the real MongoDB event store and checkpoint storage. Every start is a fresh model over the
 * same store and storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class CatchupThenPushResumePositionMongoTest {

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
    void a_restart_replays_only_the_events_after_the_stored_marker_and_stores_the_head_it_read_before_the_replay() throws Exception {
        write("1", "2");
        long head = eventStore.currentPosition();
        start(new CopyOnWriteArrayList<>());
        assertThat(GlobalCheckpoint.positionOf(marker.read("proj"))).isEqualTo(head);
        write("3");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(delivered);

        assertThat(delivered).containsExactly("3");
        assertThat(GlobalCheckpoint.positionOf(marker.read("proj"))).isEqualTo(eventStore.currentPosition());
    }

    @Test
    void a_marker_that_is_written_after_a_cancel_deleted_it_makes_the_next_start_resume_after_it() throws Exception {
        write("1", "2");
        long head = eventStore.currentPosition();
        CatchupThenPushSubscriptionModel first = start(new CopyOnWriteArrayList<>());
        first.cancelSubscription("proj");
        assertThat(marker.exists("proj")).as("the cancel deleted the marker").isFalse();
        marker.save("proj", GlobalCheckpoint.of(head));
        write("3");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(delivered);

        assertThat(delivered).as("resumed after the late marker, nothing lost and nothing before it").containsExactly("3");
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() throws Exception {
        write("1", "2");
        long olderHead = eventStore.currentPosition();
        start(new CopyOnWriteArrayList<>());
        write("3");
        start(new CopyOnWriteArrayList<>());
        marker.save("proj", GlobalCheckpoint.of(olderHead));
        write("4");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(delivered);

        assertThat(delivered).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() throws Exception {
        write("1", "2", "3");
        marker.save("proj", GlobalCheckpoint.of(eventStore.currentPosition() - 1));

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(delivered);

        assertThat(delivered).containsExactly("3");
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
