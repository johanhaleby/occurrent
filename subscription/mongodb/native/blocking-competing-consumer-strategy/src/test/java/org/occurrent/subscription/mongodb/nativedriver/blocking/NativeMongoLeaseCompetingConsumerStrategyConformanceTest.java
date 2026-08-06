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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyConformance;
import org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;

/**
 * The same contract as the Spring strategy answers, asserted separately rather than assumed. The two share
 * {@code MongoLeaseCompetingConsumerStrategySupport} but reach the lock collection through different APIs, the native
 * driver against Spring Data's {@code execute}, and it is only the second of those that runs inside a Spring managed
 * transaction if the caller has one open.
 */
@Testcontainers
class NativeMongoLeaseCompetingConsumerStrategyConformanceTest extends CompetingConsumerStrategyConformance {

    private static final String DATABASE = "nativecompetingconsumerconformance";

    /**
     * Short enough that the suite's waits are over quickly, long enough that a slow round trip to MongoDB cannot cost a
     * holder the lease it is refreshing halfway through it.
     */
    private static final Duration LEASE_TIME = Duration.ofSeconds(1);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // One client for the class, since standing a client up means server discovery. What has to be fresh per test is
    // the collection the locks live in, which the fixture takes care of.
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected CompetingConsumerStrategyFixture createFixture() {
        return new NativeMongoLeaseFixture(database);
    }

    private static class NativeMongoLeaseFixture implements CompetingConsumerStrategyFixture {

        private final MongoDatabase database;
        private final String collection;
        private final CompetingConsumerStrategy strategy;

        NativeMongoLeaseFixture(MongoDatabase database) {
            this.database = database;
            // A collection of its own per test, rather than emptying a shared one, so the storage starts with no locks
            // without depending on cleanup order and without disturbing anything else running against this container.
            this.collection = "competing-consumer-locks-" + UUID.randomUUID();
            this.strategy = build();
        }

        @Override
        public CompetingConsumerStrategy competingConsumerStrategy() {
            return strategy;
        }

        /**
         * A second strategy over the same collection. Contention is only visible between instances, and this is a
         * strategy an application would build the same way on another host.
         */
        @Override
        public CompetingConsumerStrategy newCompetingConsumerStrategy() {
            return build();
        }

        /**
         * A rival takes over from a holder that stopped refreshing after one lease plus one refresh period at worst,
         * which is well under this. The room is for a loaded machine, and it costs nothing to leave, because the suite
         * stops waiting the moment the condition holds and only a test that was going to fail pays the rest.
         */
        @Override
        public Duration timeToConverge() {
            return Duration.ofSeconds(5);
        }

        /**
         * The suite shuts down the strategies it took from the factory, so only the one under test is left here.
         */
        @Override
        public void close() {
            strategy.shutdown();
            database.getCollection(collection).drop();
        }

        private CompetingConsumerStrategy build() {
            return new NativeMongoLeaseCompetingConsumerStrategy.Builder(database, collection)
                    .leaseTime(LEASE_TIME)
                    .build();
        }
    }
}
