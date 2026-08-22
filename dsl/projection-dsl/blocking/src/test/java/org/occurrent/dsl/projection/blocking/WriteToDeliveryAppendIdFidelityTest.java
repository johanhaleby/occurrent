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

package org.occurrent.dsl.projection.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.cloudevents.core.builder.CloudEventBuilder.v1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Write-to-delivery {@code appendid} fidelity, blocking stack
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>):
 * the {@link AppendId} a write's {@link WriteResult} returns must be the exact same identifier a live subscriber's
 * {@link EventMetadata#getAppendId()} sees for the same event, unchanged. The TCK's {@code WriteResults} suite
 * already proves the stamped extension survives the store's own {@code read()}, but nothing in the repository
 * proved it survives the live change-stream delivery path a real projection actually uses. This closes that gap.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class WriteToDeliveryAppendIdFidelityTest {

    private static final URI SOURCE = URI.create("urn:occurrent:write-to-delivery-fidelity");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @Test
    void the_append_id_a_write_returns_is_the_exact_append_id_a_live_subscriber_sees_in_event_metadata() throws InterruptedException {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("write_to_delivery_fidelity_blocking"));
        MongoClient mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        String collection = "events";

        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(collection)
                .transactionConfig(new TransactionTemplate(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        SpringMongoEventStore eventStore = new SpringMongoEventStore(mongoTemplate, config);
        SpringMongoSubscriptionModel subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, collection, TimeRepresentation.RFC_3339_STRING);

        try {
            BlockingQueue<EventMetadata> delivered = new ArrayBlockingQueue<>(1);
            SubscriptionHandle subscription = subscriptionModel.subscribe("fidelity", StartAt.now(),
                    cloudEvent -> delivered.add(EventMetadata.from(cloudEvent)));
            assertThat(subscription.waitUntilStarted(Duration.ofSeconds(20))).as("the subscription never started").isTrue();

            WriteResult result = eventStore.write("stream-1", List.of(
                    v1().withId(UUID.randomUUID().toString())
                            .withSource(SOURCE)
                            .withType("Ticked")
                            .withTime(OffsetDateTime.now())
                            .withData("{}".getBytes(UTF_8))
                            .build()));
            AppendId writtenAppendId = result.appendId().orElseThrow();

            EventMetadata delivery = delivered.poll(20, TimeUnit.SECONDS);
            assertThat(delivery).as("event was never delivered to the live subscriber").isNotNull();
            assertThat(delivery.getAppendId()).as("EventMetadata.getAppendId() must be the raw string form of the written AppendId").isEqualTo(writtenAppendId.toString());
            assertThat(AppendId.from(delivery)).as("and parses back to the exact same identity").contains(writtenAppendId);
        } finally {
            subscriptionModel.shutdown();
            mongoClient.close();
        }
    }
}
