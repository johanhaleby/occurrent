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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.tck.eventstore.blocking.DcbMarkerPruneConformance;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

/**
 * Runs the blocking DCB marker prune suite against the reactive store through
 * {@link org.occurrent.tck.eventstore.reactor.BlockingEventStoreOverReactive}, and deletes markers and events with a
 * reactive client of its own.
 */
@Testcontainers
class ReactorMongoEventStoreDcbMarkerPruneConformanceTest extends DcbMarkerPruneConformance {

    private static final String EVENTS = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    /**
     * Empties the database before each test, which is how the fixture can promise the suite a store with no events in
     * it. An extension callback runs before the {@code @BeforeEach} that creates the fixture, so the order is right.
     */
    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @Override
    protected EventStoreFixture createFixture() {
        return new ReactorMongoEventStoreConformanceFixture(connectionString());
    }

    @Override
    protected void deleteMarker(String markerKey) {
        try (MongoClient client = MongoClients.create(connectionString())) {
            Mono.from(client.getDatabase(connectionString().getDatabase())
                    .getCollection(DcbMarkerModel.checkpointCollectionName(EVENTS))
                    .deleteOne(eq("_id", DcbMarkerModel.markerId(markerKey)))).block();
        }
    }

    @Override
    protected void deleteEvents(String... eventIds) {
        try (MongoClient client = MongoClients.create(connectionString())) {
            Mono.from(client.getDatabase(connectionString().getDatabase())
                    .getCollection(EVENTS)
                    .deleteMany(in("id", Arrays.asList(eventIds)))).block();
        }
    }

    private static ConnectionString connectionString() {
        return new ConnectionString(mongoDBContainer.getReplicaSetUrl() + "." + EVENTS);
    }
}
