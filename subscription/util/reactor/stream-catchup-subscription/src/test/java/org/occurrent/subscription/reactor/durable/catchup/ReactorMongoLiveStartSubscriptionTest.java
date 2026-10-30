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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
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
import reactor.core.Disposable;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A change stream started at the live start of a scope delivers what was written after the scope opened and not what its reads can see.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoLiveStartSubscriptionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(40);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "livestartreactor"));

    private MongoClient mongoClient;
    private Disposable subscription;

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.dispose();
        }
        mongoClient.close();
    }

    @Test
    void a_subscription_started_at_the_live_start_of_a_scope_receives_what_was_written_after_the_scope_was_opened_and_not_what_the_scope_could_read() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("livestartreactor"));
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, database);
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        ReactorMongoEventStore eventStore = new ReactorMongoEventStore(template, new EventStoreConfig.Builder()
                .eventStoreCollectionName("events").transactionConfig(tx).timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM).build());
        ReactorMongoSubscriptionModel subscriptions = new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);

        eventStore.write("stream", event("before-scope", "Happened")).block(TIMEOUT);
        CatchupReadScope scope = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        String liveStart = scope.liveStart();
        scope.close().block(TIMEOUT);
        eventStore.write("stream", event("after-scope", "Happened")).block(TIMEOUT);

        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscription = subscriptions.subscribe(null, StartAt.checkpoint(new StringBasedCheckpoint(liveStart))).map(CloudEvent::getId).subscribe(received::add);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("after-scope"));
        eventStore.write("stream", event("sentinel", "Happened")).block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(received).contains("sentinel"));

        assertThat(received).containsExactly("after-scope", "sentinel");
    }

    private static CloudEvent event(String id, String type) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType(type).withTime(OffsetDateTime.now()).build();
    }
}
