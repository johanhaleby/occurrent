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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.tck.subscription.blocking.CheckpointAwareSubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Reaches {@link ReactorMongoSubscriptionModel#globalCheckpoint()} through the checkpoint-aware bridge factory. This
 * is the contract #395 calls easy to miss and expensive to get wrong, and the reactor model answers it with an empty
 * {@code Mono} when the position cannot be resolved, which the bridge maps to the blocking {@code null} the suite
 * asserts on both branches.
 */
@Testcontainers
class ReactorMongoSubscriptionModelCheckpointConformanceTest extends CheckpointAwareSubscriptionModelConformance {

    private static final String DATABASE = "reactormongocheckpointconformance";

    // Per class and extension-managed, rather than one container shared across this module's test classes, mirroring
    // SpringMongoSubscriptionModelConformanceTest on the blocking side.
    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected SubscriptionModelFixture createFixture() {
        return new ReactorMongoSubscriptionModelFixture(mongoClient, DATABASE);
    }
}
