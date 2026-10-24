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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.LaggingSecondaryReplicaSet;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreCatchupReadScopeLaggingSecondaryTest {

    private static final String DATABASE = "lagging";
    private static final String COLLECTION = "events";

    private static LaggingSecondaryReplicaSet replicaSet;
    private static MongoClient writerClient;
    private static MongoClient laggingClient;
    private static SpringMongoEventStore writer;
    private static SpringMongoEventStore laggingReader;

    @BeforeAll
    static void start_a_replica_set_with_a_secondary_that_applies_writes_late() {
        replicaSet = LaggingSecondaryReplicaSet.start();
        writerClient = MongoClients.create(replicaSet.clientSettings().build());
        laggingClient = MongoClients.create(replicaSet.laggingReaderSettings().build());
        writer = storeOver(writerClient);
        laggingReader = storeOver(laggingClient);
    }

    @AfterAll
    static void stop_the_replica_set() {
        laggingClient.close();
        writerClient.close();
        replicaSet.close();
    }

    @Test
    @Timeout(120)
    void a_scope_replays_everything_written_before_it_opened_when_it_reads_from_a_secondary_that_is_behind() {
        replicaSet.stopApplyingWrites();
        writer.write("stream", List.of(event("first", "Happened"), event("second", "Happened"), event("third", "Happened")));
        assertThat(idsOf(laggingReader.read("stream").events())).as("events the lagging secondary has applied when the scope opens").isEmpty();

        resumeApplyingWritesInThreeSeconds();

        try (CatchupReadScope scope = laggingReader.openCatchupReadScope(null)) {
            try (Stream<CloudEvent> events = scope.readInPositionOrder(Filter.all(), PositionRange.upToPosition(scope.currentPosition()))) {
                assertThat(idsOf(events)).containsExactly("first", "second", "third");
            }
        }
    }

    private static SpringMongoEventStore storeOver(MongoClient client) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        return new SpringMongoEventStore(new MongoTemplate(client, DATABASE), config);
    }

    private static void resumeApplyingWritesInThreeSeconds() {
        CompletableFuture.runAsync(replicaSet::resumeApplyingWrites, CompletableFuture.delayedExecutor(3, TimeUnit.SECONDS));
    }
}
