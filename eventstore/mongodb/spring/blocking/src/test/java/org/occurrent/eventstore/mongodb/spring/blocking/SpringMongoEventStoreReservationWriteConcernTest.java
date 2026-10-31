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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
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
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreReservationWriteConcernTest {

    private static final String COLLECTION = "events";
    private static final String POSITIONS = DcbMarkerModel.positionCollectionName(COLLECTION);
    private static final long WTIMEOUT_MILLIS = 12_345;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private MongoClient mongoClient;
    private MongoTransactionManager transactionManager;
    private SpringMongoEventStore store;

    @BeforeEach
    void open_a_store_on_a_client_at_write_concern_1_with_a_timeout() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reservation");
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .writeConcern(WriteConcern.W1.withWTimeout(WTIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                .addCommandListener(commands)
                .build());
        String database = connectionString.getDatabase();
        transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, database));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        store = new SpringMongoEventStore(new MongoTemplate(mongoClient, database), config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void an_append_reserves_its_position_with_write_concern_majority_and_the_timeout_of_the_client() {
        // Given
        commands.clear();

        // When
        store.append(List.of(taggedEvent("Happened", "order:1")));

        // Then
        BsonDocument writeConcern = onlyReservation().document().getDocument("writeConcern", new BsonDocument());
        assertAll(
                () -> assertThat(writeConcern.get("w")).as("%s", writeConcern).isEqualTo(new BsonString("majority")),
                () -> assertThat(writeConcern.get("wtimeout")).as("%s", writeConcern).isIn(new BsonInt32((int) WTIMEOUT_MILLIS), new BsonInt64(WTIMEOUT_MILLIS))
        );
    }

    @Test
    void an_append_in_a_callers_transaction_reserves_its_position_in_that_transaction_without_a_write_concern() {
        // Given
        CloudEvent event = taggedEvent("Happened", "order:1");
        commands.clear();

        // When
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> store.append(List.of(event)));

        // Then
        Command reservation = onlyReservation();
        assertAll(
                () -> assertThat(reservation.document().getBoolean("autocommit").getValue()).as("%s", reservation).isFalse(),
                () -> assertThat(reservation.document().containsKey("writeConcern")).as("%s", reservation).isFalse(),
                () -> assertThat(store.read(tags(tag("order:1"))).events()).extracting(CloudEvent::getId).containsExactly(event.getId())
        );
    }

    private Command onlyReservation() {
        List<Command> reservations = commands.commands("findAndModify", POSITIONS);
        assertThat(reservations).as("reservations in %s", commands.commands()).hasSize(1);
        return reservations.get(0);
    }
}
