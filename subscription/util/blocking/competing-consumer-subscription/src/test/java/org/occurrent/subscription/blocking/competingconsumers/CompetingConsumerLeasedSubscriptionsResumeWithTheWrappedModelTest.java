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

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A subscription that won its lease while the wrapped model was not started yet sits paused in the wrapped model,
 * although this model records it as running. Starting this model resumes it, and so does a grant that starts the
 * wrapped model, since this node holds its lease. A stop, on the other hand, stops this model whether the wrapped model
 * was started or not, and a subscription made while this model is stopped takes no lease until it is started.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeasedSubscriptionsResumeWithTheWrappedModelTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private String locks;
    private CompetingConsumerSubscriptionModel node;
    private SpringMongoLeaseCompetingConsumerStrategy rival;

    @BeforeEach
    void connect() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
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
    void a_grant_that_starts_the_wrapped_model_resumes_the_subscriptions_whose_lease_this_node_already_held() {
        rival = strategy();
        assertThat(rival.registerCompetingConsumer("W", "rival")).isTrue();
        node = new CompetingConsumerSubscriptionModel(notStartedSpringModel(), strategy());
        CopyOnWriteArrayList<CloudEvent> handledByX = new CopyOnWriteArrayList<>();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handledByX::add);
        node.subscribe("node", "W", null, StartAt.subscriptionModelDefault(), __ -> {
        });

        rival.unregisterCompetingConsumer("W", "rival");
        await("the node is granted W, which starts the wrapped model").atMost(6, SECONDS).until(() -> node.isRunning("W"));
        String eventId = writeEvent();

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(handledByX).extracting(CloudEvent::getId)
                .as("X, whose lease the node holds, is resumed along with the wrapped model")
                .contains(eventId));
    }

    @Test
    void starting_this_model_resumes_the_subscriptions_whose_lease_this_node_already_held() {
        node = new CompetingConsumerSubscriptionModel(notStartedSpringModel(), strategy());
        CopyOnWriteArrayList<CloudEvent> handledByX = new CopyOnWriteArrayList<>();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handledByX::add);

        node.start();
        await("X, whose lease the node holds, is resumed when the model is started").atMost(5, SECONDS).until(() -> node.isRunning("X"));
        String eventId = writeEvent();

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(handledByX).extracting(CloudEvent::getId)
                .as("X, whose lease the node holds, is resumed when the model is started")
                .contains(eventId));
    }

    @Test
    void a_stop_after_a_start_that_found_the_lease_taken_keeps_the_subscription_stopped_once_the_lease_is_free() {
        rival = strategy();
        node = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING)), strategy());
        CopyOnWriteArrayList<CloudEvent> handledByX = new CopyOnWriteArrayList<>();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handledByX::add);
        await().atMost(5, SECONDS).until(() -> node.isRunning("X"));
        node.stop();
        assertThat(rival.registerCompetingConsumer("X", "rival")).as("another node takes the lease while this node is stopped").isTrue();
        node.start();
        assertThat(node.getWrappedSubscriptionModel().isRunning()).as("nothing started the wrapped model, since this node won no lease").isFalse();

        node.stop();
        rival.unregisterCompetingConsumer("X", "rival");
        String eventId = writeEvent();

        await().during(5, SECONDS).atMost(7, SECONDS).untilAsserted(() -> assertThat(handledByX).extracting(CloudEvent::getId)
                .as("X delivers nothing after the user stopped the model, although the lease is free")
                .doesNotContain(eventId));
    }

    @Test
    void a_subscription_made_while_this_model_is_stopped_takes_no_lease_until_the_model_is_started() {
        node = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING)), strategy());
        node.stop();
        CopyOnWriteArrayList<CloudEvent> handledByX = new CopyOnWriteArrayList<>();
        node.subscribe("node", "X", null, StartAt.subscriptionModelDefault(), handledByX::add);

        rival = strategy();
        assertThat(rival.registerCompetingConsumer("X", "rival")).as("another node can take the lease while this node is stopped").isTrue();
        rival.unregisterCompetingConsumer("X", "rival");
        node.start();

        await("X competes for its lease once the model is started").atMost(6, SECONDS).until(() -> node.isRunning("X"));
        String eventId = writeEvent();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(handledByX).extracting(CloudEvent::getId).contains(eventId));
    }

    private SpringMongoLeaseCompetingConsumerStrategy strategy() {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(2)).collectionName(locks).build();
    }

    private SpringMongoSubscriptionModel notStartedSpringModel() {
        return new SpringMongoSubscriptionModel(template, withConfig("events", TimeRepresentation.RFC_3339_STRING).autoStartup(false));
    }

    private String writeEvent() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write("stream", List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
