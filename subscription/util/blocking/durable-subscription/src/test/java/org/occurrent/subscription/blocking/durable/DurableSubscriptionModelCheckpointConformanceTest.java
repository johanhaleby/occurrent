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

package org.occurrent.subscription.blocking.durable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.tck.subscription.blocking.CheckpointAwareSubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * {@link DurableSubscriptionModel} implements {@link org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel}
 * by delegating {@code globalCheckpoint()} straight to the wrapped {@code NativeMongoSubscriptionModel}, so this
 * suite is exercising the delegation as much as the position itself.
 */
@Testcontainers
class DurableSubscriptionModelCheckpointConformanceTest extends CheckpointAwareSubscriptionModelConformance {

    private static final String DATABASE = "durablecheckpointconformance";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

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
    protected SubscriptionModelFixture createFixture() {
        return new DurableSubscriptionModelFixture(mongoClient, database);
    }
}
