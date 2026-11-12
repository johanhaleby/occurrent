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

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A {@link ReactorDurableSubscriptionModel} over a {@link ReactorMongoSubscriptionModel} whose storage holds a position
 * a catch-up subscription model stored, which the MongoDB model cannot open a change stream at. It would otherwise open
 * at the present and skip every event written before it.
 */
@Timeout(60)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelStoredCatchupPositionMongoTest {

    private static final String DATABASE = "reactordurablestoredcatchupposition";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static MongoClient client;

    @BeforeAll
    static void connect() {
        client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        client.close();
    }

    @Test
    void a_subscription_whose_stored_checkpoint_is_a_global_checkpoint_is_refused_instead_of_opened_at_the_present() {
        // Given a checkpoint a catch-up subscription model stored, and an event written before the subscribe
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(client, DATABASE);
        String collection = "events-" + UUID.randomUUID();
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(template, collection, TimeRepresentation.RFC_3339_STRING);
        ReactorCheckpointStorage storage = new ReactorCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        ReactorDurableSubscriptionModel durable = new ReactorDurableSubscriptionModel(mongoModel, storage);
        ReactorMongoEventStore eventStore = new ReactorMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName(collection)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(client, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        String subscriptionId = UUID.randomUUID().toString();
        GlobalCheckpoint stored = GlobalCheckpoint.of(7);
        storage.save(subscriptionId, stored).block(TIMEOUT);
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        try {
            write(eventStore, "written-before-the-subscribe");

            // When it subscribes with the default start
            Subscription subscription = durable.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.fromRunnable(() -> handled.add(cloudEvent)));

            // Then the handle fails with the refusal naming the stored checkpoint, and the subscription is gone
            Throwable refusal = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block(TIMEOUT));
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("\"" + stored.asString() + "\"");
            assertThat(durable.isRunning(subscriptionId)).isFalse();
            assertThat(durable.subscriptionIds()).doesNotContain(subscriptionId);

            // And the stored checkpoint is still there, and the event written before the subscribe is never delivered
            Checkpoint after = storage.read(subscriptionId).block(TIMEOUT);
            assertThat(after).isNotNull();
            assertThat(after.asString()).isEqualTo(stored.asString());
            CopyOnWriteArrayList<CloudEvent> control = new CopyOnWriteArrayList<>();
            Subscription controlSubscription = mongoModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> control.add(cloudEvent)));
            assertThat(controlSubscription.waitUntilStarted(TIMEOUT).block(TIMEOUT)).isTrue();
            write(eventStore, "written-after-the-subscribe");
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(control).extracting(CloudEvent::getId).contains("written-after-the-subscribe"));
            assertThat(handled).isEmpty();
        } finally {
            durable.shutdown();
            mongoModel.shutdown();
        }
    }

    private static void write(ReactorMongoEventStore eventStore, String eventId) {
        CloudEvent event = CloudEventBuilder.v1().withId(eventId).withSource(URI.create("urn:test")).withType("SomethingHappened")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC)).withSubject("s").withDataContentType("application/json").withData("{}".getBytes()).build();
        eventStore.write(UUID.randomUUID().toString(), 0, Flux.just(event)).block(TIMEOUT);
    }
}
