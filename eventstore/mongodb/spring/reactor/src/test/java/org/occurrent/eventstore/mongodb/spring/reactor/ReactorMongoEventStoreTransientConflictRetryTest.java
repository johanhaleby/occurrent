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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.all;

/**
 * A DCB append that loses a transaction conflict again and again is retried for about two minutes, the budget the
 * native store gets from the driver's {@code withTransaction}, not for 15 attempts. The conflict is forced with a
 * {@code failCommand} fail point rather than by racing threads, so the number of failed attempts is exact.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreTransientConflictRetryTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final int WRITE_CONFLICT = 112;
    // One more than the 15 attempts the store used to make, so an append that gives up at 15 never gets to succeed.
    private static final int FAILED_ATTEMPTS = 16;

    // enableTestCommands is what lets the server accept configureFailPoint. withCommand replaces the whole command
    // line, so the replica set name ReplicaSetReadyMongoDBContainer configures is repeated.
    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactorMongoEventStore eventStore;

    @BeforeEach
    void create_reactive_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".transientconflictretryreactor");
        mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @AfterEach
    void switch_off_fail_point_and_close_client() {
        configureFailPoint(new Document("mode", "off"));
        mongoClient.close();
    }

    @Test
    void a_dcb_append_that_loses_more_than_fifteen_transaction_conflicts_in_a_row_still_succeeds() {
        // The insert of the event document fails with a write conflict labelled TransientTransactionError, which is
        // what MongoDB answers a transaction that lost a write-write conflict on a shared marker document.
        configureFailPoint(new Document("mode", new Document("times", FAILED_ATTEMPTS))
                .append("data", new Document("failCommands", List.of("insert"))
                        .append("errorCode", WRITE_CONFLICT)
                        .append("errorLabels", List.of("TransientTransactionError"))));

        DcbAppendResult result = eventStore.append(List.of(DcbCloudEvents.withTags(event("NameDefined"), List.of(Tag.parse("name:1"))))).block();

        assertThat(requireNonNull(result).eventCount()).isEqualTo(1);
        assertThat(eventStore.count(all()).block()).isEqualTo(1L);
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        Mono.from(mongoClient.getDatabase("admin").runCommand(command)).block();
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
