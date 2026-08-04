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
import org.occurrent.tck.eventstore.reactor.ReactiveEventStoreConformance;
import org.occurrent.tck.eventstore.reactor.ReactiveEventStoreFixture;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

@Testcontainers
class ReactorMongoEventStoreReactiveConformanceTest extends ReactiveEventStoreConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    /**
     * Empties the database before each test, which is how the fixture can promise the suite a store with no events in
     * it. An extension callback runs before the {@code @BeforeEach} that creates the fixture, so the order is right.
     */
    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));

    @Override
    protected ReactiveEventStoreFixture createFixture() {
        // The database here is "test" and the collection "events". Appending ".events" to the replica-set URL does
        // not change the database, because MongoDB forbids a dot in a database name, so only getCollection() sees it.
        return new ReactorMongoReactiveConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    }
}
