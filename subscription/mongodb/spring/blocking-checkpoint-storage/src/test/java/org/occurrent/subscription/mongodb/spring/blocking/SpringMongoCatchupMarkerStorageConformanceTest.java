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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;
import org.occurrent.tck.subscription.blocking.CatchupMarkerStorageConformance;
import org.occurrent.tck.subscription.blocking.CatchupMarkerStorageFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Testcontainers
class SpringMongoCatchupMarkerStorageConformanceTest extends CatchupMarkerStorageConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoOperations mongoOperations;

    // Writes at majority, since the store reads the marker at majority and a w:1 save can still be invisible to it
    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("springcatchupmarkerconformance"));
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY).build());
        mongoOperations = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected CatchupMarkerStorageFixture createFixture() {
        String collection = "checkpoints-" + UUID.randomUUID();
        CatchupMarkerStorage storage = new SpringMongoCheckpointStorage(mongoOperations, collection);
        return new CatchupMarkerStorageFixture() {
            @Override
            public CatchupMarkerStorage catchupMarkerStorage() {
                return storage;
            }

            @Override
            public void close() {
                mongoOperations.dropCollection(collection);
            }
        };
    }
}
