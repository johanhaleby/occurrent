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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.LaggingSecondaryReplicaSet;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;

/**
 * Runs against a client at write concern 1, below the majority a reservation waits for. The set is failed over by the
 * test, so each test gets its own.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreReservationFailoverTest {

    private static final String DATABASE = "failover";
    private static final String COLLECTION = "events";
    private static final String WRITER = "reservation-writer";

    private LaggingSecondaryReplicaSet replicaSet;
    private MongoClient client;
    private SpringMongoEventStore store;

    @BeforeEach
    void start_a_replica_set_and_a_store_at_write_concern_1() {
        replicaSet = LaggingSecondaryReplicaSet.startForFailover();
        client = MongoClients.create(replicaSet.clientSettings().writeConcern(WriteConcern.W1).applicationName(WRITER).build());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        store = new SpringMongoEventStore(new MongoTemplate(client, DATABASE), config);
    }

    @AfterEach
    void stop_the_replica_set() {
        client.close();
        replicaSet.close();
    }

    @Test
    @Timeout(240)
    void an_append_whose_reservation_is_in_flight_when_the_primary_steps_down_is_found_by_a_read_of_its_criteria() {
        // Given
        DcbCriteria criteria = tags(tag("order:1"));
        store.append(List.of(taggedEvent("Seed", "seed:1")));
        replicaSet.stopReplicatingToSecondaries();
        replicaSet.holdNextFindOnInitialPrimary(WRITER, Duration.ofSeconds(3));
        long counterBefore = counterOnTheInitialPrimary();
        CloudEvent event = taggedEvent("OrderPlaced", "order:1");

        // When
        CompletableFuture<DcbAppendResult> append = CompletableFuture.supplyAsync(() -> store.append(List.of(event)));
        awaitCounterAbove(counterBefore);
        replicaSet.stepDownInitialPrimary();
        replicaSet.resumeReplicatingToSecondaries();
        append.orTimeout(120, TimeUnit.SECONDS).join();

        // Then
        assertThat(store.read(criteria).events()).extracting(CloudEvent::getId).containsExactly(event.getId());
    }

    private long counterOnTheInitialPrimary() {
        try (MongoClient direct = replicaSet.directClient(LaggingSecondaryReplicaSet.INITIAL_PRIMARY)) {
            Document counter = direct.getDatabase(DATABASE).getCollection(COLLECTION + "_position").find(new Document("_id", "dcb")).first();
            return counter == null ? 0 : ((Number) counter.get("position")).longValue();
        }
    }

    private void awaitCounterAbove(long counterBefore) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (counterOnTheInitialPrimary() <= counterBefore) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("The append had not reserved its position on the initial primary after 60 seconds");
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the reservation", e);
            }
        }
    }
}
