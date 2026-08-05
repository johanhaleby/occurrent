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
import org.occurrent.tck.subscription.blocking.RestartConformance;
import org.occurrent.tck.subscription.blocking.RestartableSubscriptionModelFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Wires the bare {@link NativeMongoSubscriptionModel}, with no durable wrapper around it, to
 * {@link RestartConformance}. ADR 94 names this exact pairing as the suite's "false" branch: a change-stream model
 * with no checkpoint of its own reads from wherever the server is now after a restart, which is the opposite of what
 * {@code DurableSubscriptionModel} (wired against this same suite in the durable-subscription module) promises.
 */
@Testcontainers
class NativeMongoSubscriptionModelRestartConformanceTest extends RestartConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl("nativesubscriptionrestartconformance"));
        database = mongoClient.getDatabase("nativesubscriptionrestartconformance");
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected RestartableSubscriptionModelFixture createFixture() {
        return new NativeMongoSubscriptionModelFixture(mongoClient, database);
    }
}
