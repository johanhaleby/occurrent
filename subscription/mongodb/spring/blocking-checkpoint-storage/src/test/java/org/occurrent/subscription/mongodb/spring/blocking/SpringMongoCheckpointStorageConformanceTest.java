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
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.tck.subscription.blocking.CheckpointStorageConformance;
import org.occurrent.tck.subscription.blocking.CheckpointStorageFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

@Testcontainers
class SpringMongoCheckpointStorageConformanceTest extends CheckpointStorageConformance {

    private static final String DATABASE = "springcheckpointconformance";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // One client and one template for the class, since standing a client up means server discovery. What has to be
    // fresh per test is the collection, which the fixture takes care of.
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
    protected CheckpointStorageFixture createFixture() {
        return new SpringMongoCheckpointStorageFixture(mongoOperations);
    }

    private static class SpringMongoCheckpointStorageFixture implements CheckpointStorageFixture {

        private final MongoOperations mongoOperations;
        private final String collection;
        private final CheckpointStorage storage;

        SpringMongoCheckpointStorageFixture(MongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
            // A collection of its own per test, rather than dropping a shared one, so the storage starts empty without
            // depending on cleanup order and without disturbing anything else running against this container.
            this.collection = "checkpoints-" + UUID.randomUUID();
            this.storage = new SpringMongoCheckpointStorage(mongoOperations, collection);
        }

        @Override
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        /**
         * Same encoding as the native driver's storage, and deliberately asserted separately: the two adapters write
         * the same three shapes of document through different APIs, {@code upsert} against {@code replaceOne}, so
         * "they agree" is a claim worth pinning rather than assuming.
         */
        @Override
        public boolean preservesCheckpointType(Checkpoint checkpoint) {
            return checkpoint instanceof MongoResumeTokenCheckpoint
                    || checkpoint instanceof MongoOperationTimeCheckpoint
                    || checkpoint instanceof StringBasedCheckpoint;
        }

        /**
         * The two checkpoints a MongoDB change stream actually hands this storage. A resume token is only ever read
         * back through its {@code _data} field, which is why the token declared here carries that and nothing else.
         */
        @Override
        public List<Checkpoint> additionalCheckpoints() {
            return List.of(
                    new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("82ABCDEF"))),
                    new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)));
        }

        /**
         * Interim: this storage does not evaluate a write condition yet, see {@link SpringMongoCheckpointStorage#save}.
         */
        @Override
        public boolean evaluatesWriteConditions() {
            return false;
        }

        @Override
        public void close() {
            mongoOperations.dropCollection(collection);
        }
    }
}
