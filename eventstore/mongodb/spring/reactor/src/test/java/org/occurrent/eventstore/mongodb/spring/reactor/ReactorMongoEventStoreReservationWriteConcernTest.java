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
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreReservationWriteConcernTest {

    private static final String COLLECTION = "events";
    private static final String POSITIONS = DcbMarkerModel.positionCollectionName(COLLECTION);
    private static final long WTIMEOUT_MILLIS = 12_345;
    private static final long COMMIT_TIMEOUT_MILLIS = 6_789;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private MongoClient mongoClient;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore store;

    @BeforeEach
    void open_a_store_on_a_client_at_write_concern_1_with_a_timeout_other_than_the_stores() {
        openStore(settings -> {
        });
    }

    private void openStore(Consumer<MongoClientSettings.Builder> clientSettings) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reservation");
        MongoClientSettings.Builder settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .writeConcern(WriteConcern.W1.withWTimeout(WTIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                .addCommandListener(commands);
        clientSettings.accept(settings);
        mongoClient = MongoClients.create(settings.build());
        String database = connectionString.getDatabase();
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .markerVersionsCommitTimeout(Duration.ofMillis(COMMIT_TIMEOUT_MILLIS))
                .build();
        store = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, database), config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void an_append_reserves_its_position_with_write_concern_majority_bounded_by_the_marker_versions_commit_timeout() {
        // Given
        commands.clear();

        // When
        store.append(List.of(taggedEvent("Happened", "order:1"))).block();

        // Then
        BsonDocument writeConcern = onlyReservation().document().getDocument("writeConcern", new BsonDocument());
        assertAll(
                () -> assertThat(writeConcern.get("w")).as("%s", writeConcern).isEqualTo(new BsonString("majority")),
                () -> assertThat(writeConcern.get("wtimeout")).as("%s", writeConcern).isIn(new BsonInt32((int) COMMIT_TIMEOUT_MILLIS), new BsonInt64(COMMIT_TIMEOUT_MILLIS))
        );
    }

    @Test
    void an_append_in_a_callers_transaction_reserves_its_position_outside_that_transaction_with_write_concern_majority() {
        // Given
        CloudEvent event = taggedEvent("Happened", "order:1");
        commands.clear();

        // When
        TransactionalOperator.create(transactionManager).transactional(store.append(List.of(event))).block();

        // Then
        Command reservation = onlyReservation();
        assertAll(
                () -> assertThat(reservation.document().containsKey("autocommit")).as("%s", reservation).isFalse(),
                () -> assertThat(reservation.document().getDocument("writeConcern").get("w")).as("%s", reservation).isEqualTo(new BsonString("majority")),
                () -> assertThat(store.read(tags(tag("order:1"))).block().events()).extracting(CloudEvent::getId).containsExactly(event.getId())
        );
    }

    @Test
    void a_rollback_of_the_callers_transaction_keeps_the_reserved_position_and_writes_no_event() {
        // Given
        CloudEvent event = taggedEvent("Happened", "order:1");

        // When
        TransactionalOperator.create(transactionManager).execute(status -> {
            status.setRollbackOnly();
            return store.append(List.of(event));
        }).blockLast();

        // Then
        assertAll(
                () -> assertThat(store.currentPosition().block()).as("the reservation committed on its own").isEqualTo(1L),
                () -> assertThat(store.read(tags(tag("order:1"))).block().events()).isEmpty()
        );
    }

    @Test
    void an_append_on_a_client_with_an_operation_timeout_reserves_its_position_within_the_marker_versions_commit_timeout() {
        // Given
        mongoClient.close();
        openStore(settings -> settings.timeout(60, TimeUnit.SECONDS));
        commands.clear();

        // When
        store.append(List.of(taggedEvent("Happened", "order:1"))).block();

        // Then
        BsonDocument reservation = onlyReservation().document();
        assertAll(
                () -> assertThat(reservation.containsKey("maxTimeMS")).as("%s", reservation).isTrue(),
                () -> assertThat(reservation.getNumber("maxTimeMS").longValue()).as("%s", reservation).isBetween(1L, COMMIT_TIMEOUT_MILLIS)
        );
    }

    private Command onlyReservation() {
        List<Command> reservations = commands.commands("findAndModify", POSITIONS);
        assertThat(reservations).as("reservations in %s", commands.commands()).hasSize(1);
        return reservations.get(0);
    }
}
