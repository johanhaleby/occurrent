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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.eventstore.blocking.StreamEventStoreConformance;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@Testcontainers
class SpringMongoEventStoreStreamConformanceTest extends StreamEventStoreConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    /**
     * Empties the database before each test, which is how the fixture can promise the suite a store with no events in
     * it. An extension callback runs before the {@code @BeforeEach} that creates the fixture, so the order is right.
     */
    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    @Override
    protected EventStoreFixture createFixture() {
        return new SpringMongoEventStoreFixture();
    }

    private static class SpringMongoEventStoreFixture implements EventStoreFixture {

        private final MongoClient mongoClient;
        private final SpringMongoEventStore eventStore;

        SpringMongoEventStoreFixture() {
            // The database here is "test" and the collection "events". Appending ".events" to the replica-set URL does
            // not change the database, because MongoDB forbids a dot in a database name, so only getCollection() sees it.
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
            this.mongoClient = MongoClients.create(connectionString);
            String database = requireNonNull(connectionString.getDatabase());
            MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, database);
            MongoTransactionManager transactionManager =
                    new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, database));
            EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                    .eventStoreCollectionName(connectionString.getCollection())
                    .transactionConfig(transactionManager)
                    .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                    .build();
            this.eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        }

        @Override
        public Set<EventStoreCapability> capabilities() {
            return Set.of(EventStoreCapability.STREAM);
        }

        @Override
        public EventStore eventStore() {
            return eventStore;
        }

        @Override
        public EventStoreQueries queries() {
            return eventStore;
        }

        @Override
        public EventStoreOperations operations() {
            return eventStore;
        }

        @Override
        public ReadEventStreamWithFilter filteredReader() {
            return eventStore;
        }

        @Override
        public PositionOrderedReader positionOrderedReader() {
            return eventStore;
        }

        @Override
        public void close() {
            mongoClient.close();
        }
    }
}
