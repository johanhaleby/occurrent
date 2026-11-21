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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Docker-based. {@code SpringMongoSagaStateStore} holds a map that BSON reads back as other values, such as one with a
 * {@code BigDecimal}, as the bytes of the data, and splits a record over 16 MiB across chunks.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSkippedDataReleaseTest extends SkippedDataReleaseContract {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Override
    SagaStateStore<String> newStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("skd-" + UUID.randomUUID().toString().substring(0, 8)));
        return new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase())), "saga-skipped-data", String.class);
    }

    @Override
    boolean copiesMapsBsonCannotHold() {
        return false;
    }
}
