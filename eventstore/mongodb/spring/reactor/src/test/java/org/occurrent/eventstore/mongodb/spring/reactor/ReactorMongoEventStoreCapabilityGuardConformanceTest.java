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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.tck.eventstore.blocking.CapabilityGuardConformance;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.ArrayList;
import java.util.List;

@Testcontainers
class ReactorMongoEventStoreCapabilityGuardConformanceTest extends CapabilityGuardConformance {

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
        // The database here is "test" and the collection "events". Appending ".events" to the replica-set URL does
        // not change the database, because MongoDB forbids a dot in a database name, so only getCollection() sees it.
        return new ReactorMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    }
}
