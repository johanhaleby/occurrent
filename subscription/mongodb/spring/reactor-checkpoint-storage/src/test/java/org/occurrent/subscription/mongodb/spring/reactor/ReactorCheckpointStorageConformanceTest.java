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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.tck.subscription.reactor.CheckpointStorageConformance;
import org.occurrent.tck.subscription.reactor.CheckpointStorageFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

/**
 * The ADR 116 conditional-checkpoint-write glue, {@link ReactorCheckpointStorage#save(String, Checkpoint,
 * org.occurrent.subscription.CheckpointWriteCondition)} and {@link ReactorCheckpointStorage#writeVersion(String)},
 * had no assertion anywhere before this test. Everything else in this module exercises the unconditional path.
 */
@Testcontainers
class ReactorCheckpointStorageConformanceTest extends CheckpointStorageConformance {

    private static final String DATABASE = "reactorcheckpointconformance";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // One client and one template for the class, since standing a client up means server discovery. What has to be
    // fresh per test is the collection, which the fixture takes care of.
    private static MongoClient mongoClient;
    private static ReactiveMongoOperations mongoOperations;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoOperations = new ReactiveMongoTemplate(mongoClient, DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected CheckpointStorageFixture createFixture() {
        return new ReactorCheckpointStorageFixture(mongoOperations);
    }

    private static class ReactorCheckpointStorageFixture implements CheckpointStorageFixture {

        private final ReactiveMongoOperations mongoOperations;
        private final String collection;
        private final CheckpointStorage storage;

        ReactorCheckpointStorageFixture(ReactiveMongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
            // A collection of its own per test, rather than dropping a shared one, so the storage starts empty without
            // depending on cleanup order and without disturbing anything else running against this container.
            this.collection = "checkpoints-" + UUID.randomUUID();
            this.storage = new ReactorCheckpointStorage(mongoOperations, collection);
        }

        @Override
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        /**
         * Same encoding the blocking Spring and native adapters use, since all three build and read the document
         * through the shared {@code MongoCommons}.
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

        @Override
        public void close() {
            mongoOperations.dropCollection(collection).block();
        }
    }
}
