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

package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Not every wrapped model waits until it is started to open a subscription. {@link NativeMongoSubscriptionModel}
 * opens its change stream anyway and reports it as paused, so a later resume opens a second one, and a catch-up model
 * starts replaying it. A competing subscription is handed only to a wrapped model that runs, and never while the user
 * has stopped this model.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerOverAWrappedModelThatDoesNotParkTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient client;
    private String database;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private String locks;
    private CompetingConsumerSubscriptionModel node;
    private SpringMongoLeaseCompetingConsumerStrategy rival;

    @BeforeEach
    void connect() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        database = requireNonNull(cs.getDatabase());
        template = new MongoTemplate(client, database);
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        locks = "locks-" + UUID.randomUUID();
    }

    @AfterEach
    void shutdown() {
        if (node != null) node.shutdown();
        if (rival != null) rival.shutdown();
        client.close();
    }

    @Test
    void a_subscription_made_while_this_model_is_stopped_delivers_nothing_while_another_node_holds_its_lease() {
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).isTrue();
        node = new CompetingConsumerSubscriptionModel(nativeModel(), strategy());
        node.stop();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();

        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handled::add);
        waitForAChangeStreamToOpen();
        String eventId = writeEvent();

        await().during(3, SECONDS).atMost(5, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId)
                .as("a stopped node without the lease delivers nothing")
                .doesNotContain(eventId));
    }

    @Test
    void a_subscription_made_while_this_model_is_stopped_is_delivered_once_after_a_start() {
        node = new CompetingConsumerSubscriptionModel(nativeModel(), strategy());
        node.stop();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handled::add);
        waitForAChangeStreamToOpen();

        node.start(true);
        await().atMost(5, SECONDS).until(() -> node.isRunning("X"));
        String eventId = writeEvent();

        await().during(3, SECONDS).atMost(6, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId)
                .as("X is delivered through one change stream")
                .containsExactly(eventId));
    }

    @Test
    void a_subscription_that_wins_its_lease_while_the_wrapped_model_is_stopped_is_delivered_once_after_a_start() {
        NativeMongoSubscriptionModel nativeModel = nativeModel();
        node = new CompetingConsumerSubscriptionModel(nativeModel, strategy());
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        await().atMost(5, SECONDS).until(() -> node.isRunning("X"));
        node.stop();
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).isTrue();
        node.start(true);
        assertThat(nativeModel.isRunning()).as("a start that won no lease does not start the wrapped model").isFalse();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();

        node.subscribe("node", "Y", null, StartAt.subscriptionModelDefault(), handled::add);
        waitForAChangeStreamToOpen();
        node.start(true);
        await().atMost(5, SECONDS).until(() -> node.isRunning("Y"));
        String eventId = writeEvent();

        await().during(3, SECONDS).atMost(6, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId)
                .as("Y is delivered through one change stream")
                .containsExactly(eventId));
    }

    @Test
    void a_catch_up_subscription_made_while_this_model_is_stopped_replays_once_this_node_is_started_and_wins_its_lease() {
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).isTrue();
        String historic = writeEvent();
        SpringMongoSubscriptionModel spring = new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING));
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        node = new CompetingConsumerSubscriptionModel(new CatchupSubscriptionModel(new DurableSubscriptionModel(spring, storage), eventStore), strategy());
        node.stop();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();

        assertThatCode(() -> node.subscribe("node", "X", null, StartAtTime.beginningOfTime(), handled::add))
                .as("subscribing while this model is stopped does not reach the catch-up model")
                .doesNotThrowAnyException();
        assertThatCode(() -> node.start(true)).doesNotThrowAnyException();
        rival.unregisterCompetingConsumer("X", "rival");

        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId)
                .as("the history is replayed once this node wins the lease")
                .contains(historic));
    }

    // Long enough for a change stream that a stopped wrapped model opens anyway to be open before the next event
    private static void waitForAChangeStreamToOpen() {
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private NativeMongoSubscriptionModel nativeModel() {
        return new NativeMongoSubscriptionModel(client.getDatabase(database), "events", TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
    }

    private SpringMongoLeaseCompetingConsumerStrategy strategy() {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
    }

    private String writeEvent() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
