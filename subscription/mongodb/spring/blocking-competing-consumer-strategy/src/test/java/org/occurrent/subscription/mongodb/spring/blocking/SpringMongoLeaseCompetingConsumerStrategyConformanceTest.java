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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyConformance;
import org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;

@Testcontainers
class SpringMongoLeaseCompetingConsumerStrategyConformanceTest extends CompetingConsumerStrategyConformance {

    private static final String DATABASE = "springcompetingconsumerconformance";

    /**
     * Short enough that the suite's waits are over quickly, long enough that a slow round trip to MongoDB cannot cost a
     * holder the lease it is refreshing every {@code leaseTime / 2}. Half a second of headroom on every refresh.
     */
    private static final Duration LEASE_TIME = Duration.ofSeconds(1);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // One client and one template for the class, since standing a client up means server discovery. What has to be
    // fresh per test is the collection the locks live in, which the fixture takes care of.
    private static MongoClient mongoClient;
    private static MongoOperations mongoOperations;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoOperations = new MongoTemplate(mongoClient, DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected CompetingConsumerStrategyFixture createFixture() {
        return new SpringMongoLeaseFixture(mongoOperations);
    }

    private static class SpringMongoLeaseFixture implements CompetingConsumerStrategyFixture {

        private final MongoOperations mongoOperations;
        private final String collection;
        private final CompetingConsumerStrategy strategy;

        SpringMongoLeaseFixture(MongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
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
         * One lease plus one refresh period is the worst case for a rival taking over from a holder that stopped
         * refreshing, so 1.5 seconds here. Declared at 5 to leave room for a loaded machine: the suite stops waiting
         * the moment the condition holds, so the extra is only ever paid by a test that was going to fail.
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
            mongoOperations.dropCollection(collection);
        }

        private CompetingConsumerStrategy build() {
            return new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoOperations)
                    .collectionName(collection)
                    .leaseTime(LEASE_TIME)
                    .build();
        }
    }
}
