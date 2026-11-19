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

package org.occurrent.springboot.mongo.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
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
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The catch-up marker of the reactor {@code CatchupThenPushSubscriptionModel} is the position the next catch-up
 * resumes after (ADR 179), checked over the real reactive MongoDB event store and {@code ReactorCheckpointStorage}.
 * Every start is a fresh model over the same store and storage, which is what a restart is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveCatchupThenPushResumePositionMongoTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient mongoClient;
    private ReactorMongoEventStore eventStore;
    private CheckpointStorage marker;

    @BeforeEach
    void create_store_and_marker_storage() {
        String databaseName = "catchup-resume-" + UUID.randomUUID();
        mongoClient = MongoClients.create(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        eventStore = new ReactorMongoEventStore(mongoTemplate, new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build());
        marker = new ReactorCheckpointStorage(mongoTemplate, "catchup-markers");
    }

    @AfterEach
    void close_client() {
        mongoClient.close();
    }

    @Test
    void a_start_without_a_marker_replays_the_whole_history_and_stores_the_head() {
        write("1", "2");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);

        assertThat(delivered).containsExactly("1", "2");
        assertThat(storedPosition()).isEqualTo(head());
    }

    @Test
    void a_restart_replays_only_the_events_after_the_stored_marker_and_stores_the_new_head() {
        write("1", "2");
        start(eventStore, new CopyOnWriteArrayList<>());
        write("3");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);

        assertThat(delivered).containsExactly("3");
        assertThat(storedPosition()).isEqualTo(head());
    }

    @Test
    void the_stored_marker_is_the_head_read_before_the_replay_and_not_the_head_when_it_is_marked() {
        write("1", "2");
        long headBeforeTheReplay = head();
        // An event is written while the replay reads, so the head has moved by the time the marker is written.
        boolean[] written = {false};
        PositionOrderedReader writingWhileReading = decorated(eventStore, replay -> replay.doOnNext(e -> {
            if (!written[0]) {
                written[0] = true;
                write("during");
            }
        }));

        start(writingWhileReading, new CopyOnWriteArrayList<>());

        assertThat(head()).as("the head moved during the replay").isGreaterThan(headBeforeTheReplay);
        assertThat(storedPosition()).isEqualTo(headBeforeTheReplay);
        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);
        assertThat(delivered).as("the event written during the replay is replayed by the next start").containsExactly("during");
    }

    @Test
    void a_late_save_of_an_older_head_only_lengthens_the_next_replay() {
        write("1", "2");
        long olderHead = head();
        start(eventStore, new CopyOnWriteArrayList<>());
        write("3");
        start(eventStore, new CopyOnWriteArrayList<>());
        marker.save("proj", GlobalCheckpoint.of(olderHead)).block(TIMEOUT);
        write("4");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);

        assertThat(delivered).as("events after the older head, replayed again and none skipped").containsExactly("3", "4");
    }

    @Test
    void cancelling_deletes_the_marker_so_the_next_subscribe_replays_from_the_beginning() {
        write("1", "2");
        CatchupThenPushSubscriptionModel model = start(eventStore, new CopyOnWriteArrayList<>());

        model.cancelSubscription("proj").block(TIMEOUT);
        assertThat(marker.read("proj").hasElement().block(TIMEOUT)).as("marker after the cancel").isFalse();
        write("3");

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);

        assertThat(delivered).containsExactly("1", "2", "3");
    }

    @Test
    void a_marker_stored_by_0_34_0_is_read_as_a_resume_position() {
        write("1", "2", "3");
        marker.save("proj", GlobalCheckpoint.of(head() - 1)).block(TIMEOUT);

        List<String> delivered = new CopyOnWriteArrayList<>();
        start(eventStore, delivered);

        assertThat(delivered).containsExactly("3");
    }

    private CatchupThenPushSubscriptionModel start(PositionOrderedReader reader, List<String> delivered) {
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, new PushSubscriptionModel(), marker);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId())))
                .waitUntilStarted().block(TIMEOUT);
        // The marker is written after the catch-up is reported done, so wait for the model to finish before reading it.
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(model.isCatchingUp("proj")).as("catch-up finished").isFalse());
        return model;
    }

    private void write(String... ids) {
        List<CloudEvent> events = java.util.Arrays.stream(ids)
                .map(id -> CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated").build())
                .toList();
        eventStore.write("stream", Flux.fromIterable(events)).block(TIMEOUT);
    }

    private long head() {
        return eventStore.currentPosition().block(TIMEOUT);
    }

    private long storedPosition() {
        return GlobalCheckpoint.positionOf(marker.read("proj").block(TIMEOUT));
    }

    private static PositionOrderedReader decorated(PositionOrderedReader delegate, UnaryOperator<Flux<CloudEvent>> onRead) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(() -> onRead.apply(delegate.readInPositionOrder(filter, range)));
            }

            @Override
            public Mono<Long> currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return delegate.writesPosition();
            }
        };
    }
}
