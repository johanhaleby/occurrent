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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

@Testcontainers
class NativeMongoCheckpointStorageConformanceTest extends CheckpointStorageConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // One client for the class, since a client is a reusable handle and standing one up means server discovery.
    // What has to be fresh per test is the collection, which the fixture takes care of.
    private static MongoClient mongoClient;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl("nativecheckpointconformance"));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected CheckpointStorageFixture createFixture() {
        return new NativeMongoCheckpointStorageFixture(mongoClient.getDatabase("nativecheckpointconformance"));
    }

    private static class NativeMongoCheckpointStorageFixture implements CheckpointStorageFixture {

        private final MongoCollection<Document> collection;
        private final CheckpointStorage storage;

        NativeMongoCheckpointStorageFixture(MongoDatabase database) {
            // A collection of its own per test, rather than dropping a shared one, so the storage starts empty without
            // depending on cleanup order and without disturbing anything else running against this container.
            this.collection = database.getCollection("checkpoints-" + UUID.randomUUID());
            this.storage = new NativeMongoCheckpointStorage(collection);
        }

        @Override
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        /**
         * This storage writes its own two checkpoint types into fields it recognises and rebuilds them on the way out.
         * Anything else is stored as the string it reports, so it comes back a {@link StringBasedCheckpoint}: a
         * {@code GlobalCheckpoint} saved here is read back as one of those, which is why
         * {@code GlobalCheckpoint.isGlobalCheckpoint} has to recognise both forms.
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
         * Interim: this storage does not evaluate a write condition yet, see {@link NativeMongoCheckpointStorage#save}.
         */
        @Override
        public boolean evaluatesWriteConditions() {
            return false;
        }

        @Override
        public void close() {
            collection.drop();
        }
    }
}
